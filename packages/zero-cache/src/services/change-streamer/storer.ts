import {getHeapStatistics} from 'node:v8';
import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {type PendingQuery, type Row} from 'postgres';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {
  BigIntJSON,
  type JSONObject,
} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as v from '../../../../shared/src/valita.ts';
import * as Mode from '../../db/mode-enum.ts';
import {runTx} from '../../db/run-transaction.ts';
import {sharedSnapshot, TransactionPool} from '../../db/transaction-pool.ts';
import {type PostgresDB, type PostgresTransaction} from '../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import {orTimeout} from '../../types/timeout.ts';
import {
  backfillRequestSchema,
  isDataChange,
  isSchemaChange,
  type BackfillID,
  type BackfillRequest,
  type Change,
  type DataChange,
  type Identifier,
  type SchemaChange,
  type TableMetadata,
} from '../change-source/protocol/current.ts';
import {
  type ChangeStreamData,
  type Commit,
} from '../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../change-source/protocol/current/status.ts';
import {
  cookieOps,
  type BackfillCookie,
  type CookieOp,
  type CookieSet,
  type TableMetadataCookie,
} from '../replicator/change-log-cookies.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {Service} from '../service.ts';
import {
  reconstructWatermarkedChange,
  serializeChangeStreamDataWithChange,
  type ChangeLogEntry,
  type SerializedChangeStreamData,
} from './change-log-codec.ts';
import * as ErrorType from './error-type-enum.ts';
import {
  AutoResetSignal,
  markResetRequired,
  reinitializeChangeLog,
  type BackfillingColumn,
  type ChangeLogReinitialization,
  type TableMetadataRow,
} from './schema/tables.ts';
import type {Subscriber} from './subscriber.ts';

/**
 * Factory for creating dynamic connection pools used by the Storer to
 * isolate concurrent transactions into separate pg clients.
 */
export type PostgresDBProvider = (
  applicationName: string,
  maxConns: number,
) => PostgresDB;

type SubscriberAndMode = {
  subscriber: Subscriber;
  mode: ReplicatorMode;
};

type QueueEntry =
  | [
      'change',
      watermark: string,
      json: string,
      storedChange: string,
      orig: Exclude<Change, DataChange> | null, // null for DataChanges
    ]
  | ['ready', callback: () => void]
  | ['subscriber', SubscriberAndMode]
  | ['status', watermark: string]
  | ['abort']
  | 'stop';

/**
 * A transaction that is not stored because it was already stored by the
 * previous owner of the change log (see {@link Storer.assumeOwnershipAtHead}).
 */
type SkippedTransaction = {
  skipped: true;
  ack: boolean;
};

type PendingTransaction = {
  skipped?: undefined;
  pool: TransactionPool;
  preCommitWatermark: string;
  pos: number;
  startingReplicationState: Promise<ReplicationOwner>;
  ack: boolean;
  // changeLog rows buffered for the next multi-row INSERT flush.
  batch: ChangeLogRow[];
  // The most recently issued flush (or metadata) process, awaited to bound
  // pipeline depth and to order the commit-time replicationState update.
  lastFlush: Promise<unknown> | undefined;
  // Set if ownership of the change log is assumed in this transaction.
  ownershipAssumed: AssumedOwnership | undefined;
};

/** The state of the change log at which ownership was assumed. */
type AssumedOwnership = {
  /** The last watermark stored by the previous owner. */
  head: string;
  /** Whether the change log has pending backfills. */
  backfilling: boolean;
};

export type OwnershipTakeover = {
  /**
   * The lock that prevents the previous owner from purging the change log
   * (from which this change-streamer's replica was restored), released once
   * ownership is assumed.
   */
  purgeLock: PurgeLock | null;
  /**
   * Requests a restart of the change stream (at a transaction boundary),
   * which is used to start pending backfills once ownership is assumed.
   */
  requestRestart: (reason: string) => void;
};

type ReplicationOwner = {
  owner: string | null;
};

const backfillRequestsSchema = v.array(backfillRequestSchema);

export type TuningOptions = {
  backPressureLimitHeapProportion: number;
  statementTimeoutMs: number;
  changeLogBatchSize: number;
  drainTimeoutMs?: number | undefined;
};

/**
 * A single `changeLog` row, accumulated in {@link PendingTransaction.batch}
 * and written via `json_to_recordset()` (see `#flushChangeLog()`).
 */
type ChangeLogRow = {
  watermark: string;
  precommit: string | null;
  pos: number;
  change: string;
};

/**
 * Handles the storage of changes and the catchup of subscribers
 * that are behind.
 *
 * In the context of catchup and cleanup, it is the responsibility of the
 * Storer to decide whether a client can be caught up, or whether the
 * changes needed to catch a client up have been purged.
 *
 * **Maintained invariant**: The Change DB is only empty for a
 * completely new replica (i.e. initial-sync with no changes from the
 * replication stream).
 * * In this case, all new subscribers are expected start from the
 *   `replicaVersion`, which is the version at which initial sync
 *   was performed, and any attempts to catchup from a different
 *   point fail.
 *
 * Conversely, if non-initial changes have flowed through the system
 * (i.e. via the replication stream), the ChangeDB must *not* be empty,
 * and the earliest change in the `changeLog` represents the earliest
 * "commit" from (after) which a subscriber can be caught up.
 * * Any attempts to catchup from an earlier point must fail with
 *   a `WatermarkTooOld` error.
 * * Failure to do so could result in streaming changes to the
 *   subscriber such that there is a gap in its replication history.
 *
 * Note: Subscribers (i.e. `incremental-syncer`) consider an "error" signal
 * an unrecoverable error and shut down in response. This allows the
 * production system to replace it with a new task and fresh copy of the
 * replica backup.
 */
export class Storer implements Service {
  readonly id = 'storer';
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #taskID: string;
  readonly #discoveryAddress: string;
  readonly #discoveryProtocol: string;
  readonly #makeConnectionPool: PostgresDBProvider;

  // Isolated connection pools for steady-state, potentially concurrent
  // queries, in order to avoid a postgres.js connection-swapping bug:
  // https://github.com/porsager/postgres/issues/1204
  readonly #db: PostgresDB;
  readonly #inserter: PostgresDB;
  readonly #purger: PostgresDB;

  readonly #replicaVersion: string;
  readonly #onCommitted: (c: Commit) => void;
  readonly #onFatal: (err: Error) => void;

  /**
   * Set while waiting to assume ownership of the change log at its head
   * (see {@link assumeOwnershipAtHead}).
   */
  #pendingOwnership:
    | (OwnershipTakeover & {
        /** The last known head of the change log, which only moves forward. */
        head: string;
        /** Whether a backfill transaction was skipped. */
        skippedBackfill: boolean;
        start: number;
      })
    | null = null;
  readonly #queue = new Queue<QueueEntry>();
  readonly #backPressureThresholdBytes: number;
  readonly #statementTimeoutMs: number;
  readonly #changeLogBatchSize: number;
  readonly #drainTimeoutMs: number;
  readonly #progressMonitor: ProgressMonitor;

  #approximateQueuedBytes = 0;
  #running = false;

  constructor(
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    discoveryAddress: string,
    discoveryProtocol: string,
    dbProvider: PostgresDBProvider,
    replicaVersion: string,
    onCommitted: (c: Commit | UpstreamStatusMessage) => void,
    onFatal: (err: Error) => void,
    {
      backPressureLimitHeapProportion,
      statementTimeoutMs,
      changeLogBatchSize,
      drainTimeoutMs = 30_000,
    }: TuningOptions,
  ) {
    this.#lc = lc.withContext('component', 'change-log');
    this.#shard = shard;
    this.#taskID = taskID;
    this.#discoveryAddress = discoveryAddress;
    this.#discoveryProtocol = discoveryProtocol;
    this.#makeConnectionPool = dbProvider;
    this.#db = dbProvider('change-stream-init', 3);
    this.#inserter = dbProvider('change-log-inserter', 1);
    this.#purger = dbProvider('change-log-purger', 1);
    this.#replicaVersion = replicaVersion;
    this.#onCommitted = onCommitted;
    this.#onFatal = onFatal;
    this.#statementTimeoutMs = statementTimeoutMs;
    this.#changeLogBatchSize = Math.max(1, changeLogBatchSize);
    this.#drainTimeoutMs = drainTimeoutMs;
    this.#progressMonitor = new ProgressMonitor(
      lc,
      statementTimeoutMs,
      onFatal,
    );

    const heapStats = getHeapStatistics();
    this.#backPressureThresholdBytes =
      (heapStats.heap_size_limit - heapStats.used_heap_size) *
      backPressureLimitHeapProportion;

    this.#lc.info?.(
      `Using up to ${(this.#backPressureThresholdBytes / 1024 ** 2).toFixed(2)} MB of ` +
        `--max-old-space-size (~${(heapStats.heap_size_limit / 1024 ** 2).toFixed(2)} MB) ` +
        `to absorb upstream spikes`,
      {heapStats},
    );
  }

  // For readability in SQL statements.
  #cdc(table: string) {
    return this.#db(`${cdcSchema(this.#shard)}.${table}`);
  }

  /**
   * Bounds a one-off db call (i.e. not part of the main storer loop or the
   * background catchup read, which are tracked by the ProgressMonitor
   * instead) with a plain timeout. This covers calls like
   * {@link assumeOwnership} and {@link getStartStreamInitializationParameters}
   * that are made by the caller *before* {@link run()} -- and thus before the
   * ProgressMonitor's polling starts -- as well as ones made well after,
   * where a continuously-polling watchdog would be overkill for a single
   * bounded db round trip.
   *
   * Does not cancel `promise` on timeout (there is no cancellation mechanism
   * for a postgres.js query); it just stops waiting for it, and swallows a
   * later rejection so it doesn't surface as an unhandled rejection.
   */
  async #withTimeout<T>(name: string, promise: Promise<T>): Promise<T> {
    const result = await orTimeout(promise, this.#statementTimeoutMs);
    if (result === 'timed-out') {
      void promise.catch(() => {});
      throw new AbortError(
        `${name} did not complete within ${this.#statementTimeoutMs}ms`,
      );
    }
    return result;
  }

  #ownerAddress() {
    const ownerAddress = this.#discoveryAddress;
    const ownerProtocol = this.#discoveryProtocol;
    // we omit `ws://` so that old view syncer versions that are not expecting the protocol continue to not get it
    return ownerProtocol === 'ws'
      ? ownerAddress
      : `${ownerProtocol}://${ownerAddress}`;
  }

  /**
   * Re-initializes a change log that is no longer maintained from the
   * replica's state, taking ownership of it in the process (which is then
   * assumed again at its new head, see {@link assumeOwnershipAtHead}).
   * See {@link reinitializeChangeLog}.
   */
  reinitializeFromReplica(
    params: Omit<ChangeLogReinitialization, 'owner' | 'ownerAddress'>,
  ): Promise<'reinitialized' | 'maintained'> {
    return reinitializeChangeLog(
      this.#lc,
      this.#db,
      this.#shard,
      {...params, owner: this.#taskID, ownerAddress: this.#ownerAddress()},
      // The purge lock (if any) is released before the change log is
      // truncated, as it locks a row of it.
      this.#pendingOwnership?.purgeLock ?? undefined,
    );
  }

  /**
   * Prepares to assume ownership of the change log at its head, while it may
   * still be owned and maintained by another change-streamer. This allows the
   * previous owner to continue to serve subscribers while this change-source's
   * upstream decodes the WAL to catch up to the head of the change log, which
   * may be many minutes worth of transactions for large DBs.
   *
   * Once the change stream is started from the head of the change log:
   *
   * * Transactions that the change log already contains (i.e. stored by the
   *   previous owner) are not stored again, but are otherwise processed
   *   normally (e.g. for upstream acks, since they have been stored).
   * * Ownership is assumed in the transaction that stores the first
   *   transaction past the head of the change log. The previous owner's
   *   (subsequent) transactions fail its ownership check, which makes this
   *   transaction the one immediately after the previous owner's last one.
   * * Ownership is also assumed upon a {@link status} at or past the head of
   *   the change log (outside of a transaction), which covers an idle
   *   upstream: all transactions up to the status position have been
   *   received, and any past the head would have been stored.
   *
   * Subscribers are caught up from the change log as usual, as the stream
   * (which is forwarded to them) starts from its head.
   *
   * Backfill transactions are synthesized by each change-streamer's change
   * source, and thus never correspond to those stored by the previous owner.
   * Until ownership is assumed, they are not stored, and no backfills are
   * requested from the change log (see
   * {@link getStartStreamInitializationParameters}). Once ownership is
   * assumed, a restart of the change stream is requested if the change log has
   * pending backfills or if a backfill transaction was skipped.
   */
  assumeOwnershipAtHead(takeover: OwnershipTakeover) {
    this.#pendingOwnership = {
      ...takeover,
      head: '',
      skippedBackfill: false,
      start: performance.now(),
    };
  }

  /**
   * Whether ownership of the change log is pending, i.e. after
   * {@link assumeOwnershipAtHead} and before ownership is assumed.
   */
  get ownershipPending(): boolean {
    return this.#pendingOwnership !== null;
  }

  /**
   * Relays a status message received outside of a transaction, which is
   * used to assume ownership of the change log on an idle upstream. A no-op
   * unless ownership is pending.
   */
  status(watermark: string) {
    if (this.#pendingOwnership) {
      this.#queue.enqueue(['status', watermark]);
    }
  }

  /**
   * Assumes ownership if the change log contains nothing past `watermark`.
   * Otherwise, updates the known head of the change log.
   *
   * @param ifBefore When set, ownership is only assumed if the head of the
   *     change log is before (rather than at or before) the `watermark`,
   *     i.e. when the watermark is that of a transaction to be stored.
   * @returns The state of the change log at which ownership is assumed (in
   *     `tx`), or `null` if ownership was not assumed.
   */
  async #tryAssumeOwnership(
    tx: TransactionPool,
    watermark: string,
    ifBefore: boolean,
  ): Promise<AssumedOwnership | null> {
    const pending = must(this.#pendingOwnership);
    const [state] = await tx.processReadTask(
      sql => sql<{lastWatermark: string}[]>`
        SELECT "lastWatermark" FROM ${this.#cdc('replicationState')} FOR UPDATE`,
    );
    const head = state.lastWatermark;
    pending.head = head > pending.head ? head : pending.head;
    if (ifBefore ? head >= watermark : head > watermark) {
      return null;
    }
    const owner = this.#taskID;
    const ownerAddress = this.#ownerAddress();
    void tx.process(sql => [
      sql`UPDATE ${this.#cdc('replicationState')} SET ${sql({owner, ownerAddress})}`,
    ]);
    // Read with the replicationState row locked, i.e. consistent with the head.
    const [{backfilling}] = await tx.processReadTask(
      sql => sql<{backfilling: boolean}[]>`
        SELECT EXISTS (SELECT 1 FROM ${this.#cdc('backfilling')}) AS "backfilling"`,
    );
    this.#lc.info?.(
      `assuming ownership at ${ownerAddress} after change log head ${head}`,
    );
    return {head, backfilling};
  }

  #ownershipAssumed({head, backfilling}: AssumedOwnership) {
    const {purgeLock, requestRestart, skippedBackfill, start} = must(
      this.#pendingOwnership,
    );
    this.#pendingOwnership = null;
    this.#lc.info?.(
      `assumed ownership of the change log at ${head} after ` +
        `${(performance.now() - start).toFixed(2)} ms`,
    );
    // Once ownership has been assumed, the initial purge lock can be released,
    // as a change-streamer that was attempting to purge records will
    // correspondingly abort on the ownership check.
    void purgeLock?.release();
    if (backfilling || skippedBackfill) {
      requestRestart(`resuming backfills after assuming ownership`);
    }
  }

  /**
   * Assumes ownership of the change log immediately, regardless of its head.
   * This is used for in tests.
   *
   * A production change-streamer instead takes over the change log at its head
   * (see {@link assumeOwnershipAtHead}).
   */
  async assumeOwnership() {
    const db = this.#db;
    const owner = this.#taskID;
    const addressWithProtocol = this.#ownerAddress();
    this.#lc.info?.(`assuming ownership at ${addressWithProtocol}`);
    const start = performance.now();
    await this.#withTimeout(
      'assume-ownership',
      db`UPDATE ${this.#cdc('replicationState')} SET ${db({owner, ownerAddress: addressWithProtocol})}`,
    );
    const elapsed = (performance.now() - start).toFixed(2);
    this.#lc.info?.(
      `assumed ownership at ${addressWithProtocol} (${elapsed} ms)`,
    );
  }

  /**
   * The whole of what a stream connection starts from: where to resume, what to
   * ask the change source to backfill, and the raw cookie set those requests
   * were derived from.
   *
   * All four statements run in one REPEATABLE READ transaction, so they see one
   * snapshot. That is invariant 15 — the resume watermark and the cookies must
   * come from the same store, read at the same position — and it is why the
   * cookies are read here rather than by a second call: a cookie set paired
   * with a watermark it was not folded to loses every backfill that completed
   * in the interval, and those rows are not replayable from the slot.
   *
   * `cookies` is not `backfillRequests` in another shape. The request list is
   * driven off `backfilling`, so it drops the metadata of every table with no
   * in-flight backfill — correct for starting a stream, lossy as the cookie
   * snapshot that the SQLite change log continues folding onto.
   */
  async getStartStreamInitializationParameters(): Promise<{
    lastWatermark: string;
    backfillRequests: BackfillRequest[];
    cookies: CookieSet;
  }> {
    const [[{lastWatermark}], result, tableMetadata, backfilling] =
      await this.#withTimeout(
        'get-stream-params',
        runTx(
          this.#db,
          sql => [
            sql<{lastWatermark: string}[]>`
        SELECT "lastWatermark" FROM ${this.#cdc('replicationState')}`,

            // Formats a BackfillRequest using json_object_agg() to construct the
            // `columns` object. It is LEFT JOIN'ed with the `tableMetadata` table
            // to make it optional and possibly `null`. The change log does not
            // track backfill progress, so the columns have no `progress`
            // (i.e. they are backfilled from scratch).
            sql`
        SELECT
            json_build_object(
              'schema', b."schema",
              'name', b."table",
              'metadata', t."metadata"
            ) as "table",
            json_object_agg(b."column", json_build_object('id', b."backfill"))
              as "columns"
          FROM ${this.#cdc('backfilling')} as b
          LEFT JOIN ${this.#cdc('tableMetadata')} as t
          ON (b."schema" = t."schema" AND b."table" = t."table")
          GROUP BY b."schema", b."table", t."metadata"
        `,

            // Ordered by primary key, so that this set and the SQLite change log's
            // can be compared row for row. `COLLATE "C"` because that comparison is
            // against a store whose TEXT columns sort by byte: a linguistic
            // collation orders `foo-bar` and `foobar` differently than SQLite's
            // BINARY does, which would read as a divergence rather than as the
            // identical set it is.
            sql<TableMetadataCookie[]>`
        SELECT "schema", "table", "metadata" FROM ${this.#cdc('tableMetadata')}
          ORDER BY "schema" COLLATE "C", "table" COLLATE "C"`,

            sql<BackfillCookie[]>`
        SELECT "schema", "table", "column", "backfill" FROM ${this.#cdc('backfilling')}
          ORDER BY "schema" COLLATE "C", "table" COLLATE "C", "column" COLLATE "C"`,
          ],
          {mode: Mode.READONLY},
        ),
      );

    return {
      lastWatermark,
      // Backfills are not requested before ownership is assumed.
      // See assumeOwnershipAtHead().
      backfillRequests: this.#pendingOwnership
        ? []
        : v.parse(result, backfillRequestsSchema),
      cookies: {
        tableMetadata: [...tableMetadata],
        backfilling: [...backfilling],
      },
    };
  }

  async getMinWatermarkForCatchup(): Promise<string | null> {
    const [{minWatermark}] = await this.#withTimeout(
      'get-min-watermark',
      this.#db<{minWatermark: string | null}[]> /*sql*/ `
      SELECT min(watermark) as "minWatermark" FROM ${this.#cdc('changeLog')}`,
    );
    return minWatermark;
  }

  /**
   * Returns the retained Postgres catchup bounds.
   * `minWatermark` is the oldest transaction, or `null` for a new replica.
   * `lastWatermark` is the durable head.
   */
  async getCatchupBounds(): Promise<{
    minWatermark: string | null;
    lastWatermark: string;
  }> {
    const [bounds] = await this.#withTimeout(
      'get-catchup-bounds',
      this.#db<{minWatermark: string | null; lastWatermark: string}[]> /*sql*/ `
      SELECT
        (SELECT min(watermark) FROM ${this.#cdc('changeLog')}) as "minWatermark",
        (SELECT "lastWatermark" FROM ${this.#cdc('replicationState')}) as "lastWatermark"`,
    );
    return bounds;
  }

  /**
   * Lists at most `limit` committed transaction watermarks in
   * `(afterWatermark, throughWatermark]`, in ascending order.
   */
  async listCommitWatermarks(
    afterWatermark: string,
    throughWatermark: string,
    limit: number,
  ): Promise<string[]> {
    const rows = await this.#db<{watermark: string}[]> /*sql*/ `
      SELECT watermark FROM ${this.#cdc('changeLog')}
       WHERE precommit IS NOT NULL
         AND watermark > ${afterWatermark}
         AND watermark <= ${throughWatermark}
       ORDER BY watermark
       LIMIT ${limit}`;
    return rows.map(({watermark}) => watermark);
  }

  /**
   * Reads `(afterWatermark, throughWatermark]` in stream order and bounded batches.
   * This query matches the subscriber catchup query.
   *
   * Both queries fail when an escaped NUL reaches `change->'tag'`.
   * Postgres cannot convert that value to text.
   */
  readCatchupRange(
    afterWatermark: string,
    throughWatermark: string,
    batchRows = 2000,
  ): AsyncIterable<ChangeLogEntry[]> {
    assert(
      Number.isSafeInteger(batchRows) && batchRows > 0,
      'Postgres change log batch size must be a positive safe integer',
    );
    return this.#db<ChangeLogEntry[]> /*sql*/ `
      SELECT watermark, change->'tag' as tag, change::text FROM ${this.#cdc('changeLog')}
       WHERE watermark > ${afterWatermark}
         AND watermark <= ${throughWatermark}
       ORDER BY watermark, pos`.cursor(batchRows) as AsyncIterable<
      ChangeLogEntry[]
    >;
  }

  // Note: These are the only db `await`s that are not tracked by the
  //       ProgressMonitor, as the delete can legitimately block and/or take
  //       an arbitrary amount of time. Luckily, this is a background call and
  //       thus cannot cause the storer loop to hang.
  purgeRecordsBefore(watermark: string): Promise<number> {
    return runTx(this.#purger, async sql => {
      // This NOWAIT pre-check is an optimization to abort the transaction
      // (and release associated resources) early.
      await sql<{watermark: string}[]>`
          SELECT watermark FROM ${this.#cdc('changeLog')}
            ORDER BY watermark, pos LIMIT 1
            FOR UPDATE NOWAIT
        `;
      // If the row is purge-locked by an incoming replication-manager, it
      // will assume ownership of the change-log before releasing the lock.
      // This DELETE blocks until the lock is released, allowing the change
      // in ownership to be reliably detected (and the transaction aborted)
      // in the subsequent check.
      const [{deleted}] = await sql<{deleted: bigint}[]>`
        -- The backup watermark can be ahead of the durable changeLog if the
        -- storer is behind but the backup replica has consumed forwarded
        -- changes. Preserve the latest durable changeLog transaction as the
        -- catchup boundary instead of assuming the backup watermark exists.
        -- The storer inserts each changeLog transaction atomically, so any
        -- durable row for a watermark implies the full transaction is durable.
        WITH keep AS (
          SELECT max(watermark) AS watermark
          FROM ${this.#cdc('changeLog')}
        ), purged AS (
          DELETE FROM ${this.#cdc('changeLog')} WHERE watermark < ${watermark} 
            AND watermark < (SELECT watermark FROM keep)
            RETURNING watermark, pos
        ) SELECT COUNT(*) as deleted FROM purged;`;

      const [{owner}] = await sql<ReplicationOwner[]>`
        SELECT "owner" FROM ${this.#cdc('replicationState')} FOR SHARE`;
      if (owner !== this.#taskID) {
        throw new AbortError(
          `aborting changeLog purge to ${watermark} because ownership has been taken by ${owner}`,
        );
      }
      return Number(deleted);
    });
  }

  /**
   * @returns The JSON stringified stream message to be sent downstream.
   */
  store(
    watermark: string,
    data: ChangeStreamData,
    serialized: SerializedChangeStreamData = serializeChangeStreamDataWithChange(
      data,
    ),
  ) {
    // Eagerly stringify the JSON payload to:
    // - avoid redundant stringification when fanning out to subscribers
    // - efficiently estimate the amount of memory the payload consumes
    const {json, change: storedChange} = serialized;
    this.#approximateQueuedBytes += json.length;

    const change = data[1];
    this.#queue.enqueue([
      'change',
      watermark,
      json,
      storedChange,
      isDataChange(change) ? null : change, // drop DataChanges to save memory
    ]);

    return json;
  }

  abort() {
    this.#queue.enqueue(['abort']);
  }

  catchup(subscriber: Subscriber, mode: ReplicatorMode) {
    this.#queue.enqueue(['subscriber', {subscriber, mode}]);
  }

  #readyForMore: Resolver<void> | null = null;

  readyForMore(): Promise<void> | undefined {
    if (!this.#running) {
      return undefined;
    }
    if (
      this.#readyForMore === null &&
      this.#approximateQueuedBytes > this.#backPressureThresholdBytes
    ) {
      this.#lc.warn?.(
        `applying back pressure with ${this.#queue.size()} queued changes (~${(this.#approximateQueuedBytes / 1024 ** 2).toFixed(2)} MB)\n` +
          `\n` +
          `To inspect changeLog backlog in your change DB:\n` +
          `  SELECT\n` +
          `    (change->'relation'->>'schema') || '.' || (change->'relation'->>'name') AS table_name,\n` +
          `    change->>'tag' AS operation,\n` +
          `    COUNT(*) AS count\n` +
          `  FROM "<app_id>/cdc"."changeLog"\n` +
          `  GROUP BY 1, 2\n` +
          `  ORDER BY 3 DESC\n` +
          `  LIMIT 20;`,
      );
      this.#readyForMore = resolver();
    }
    return this.#readyForMore?.promise;
  }

  #maybeReleaseBackPressure() {
    if (this.#readyForMore !== null) {
      // Wait for at least 20% of the threshold to free up.
      if (
        this.#approximateQueuedBytes <
        this.#backPressureThresholdBytes * 0.8
      ) {
        this.#lc.info?.(
          `releasing back pressure with ${this.#queue.size()} queued changes (~${(this.#approximateQueuedBytes / 1024 ** 2).toFixed(2)} MB)`,
        );
        this.#readyForMore.resolve();
        this.#readyForMore = null;
      }
    }
  }

  /**
   * Flushes any buffered {@link PendingTransaction.batch} rows to the changeLog.
   *
   * Uses `json_to_recordset()` so the batch is a single JSON parameter: the
   * statement text stays constant regardless of batch size, avoiding the
   * unbounded prepared-statement variants (and Postgres memory growth) that a
   * multi-row INSERT would produce. See rocicorp/mono#3511.
   *
   * Returns (and updates) {@link PendingTransaction.lastFlush}; a no-op when the
   * batch is empty.
   */
  #flushChangeLog(tx: PendingTransaction): Promise<unknown> | undefined {
    const {batch} = tx;
    if (batch.length === 0) {
      return tx.lastFlush;
    }
    tx.batch = [];
    tx.lastFlush = tx.pool.process(sql => [
      sql`
        INSERT INTO ${this.#cdc('changeLog')} ("watermark", "pos", "change", "precommit")
        SELECT "watermark", "pos", "change"::json, "precommit"
          FROM json_to_recordset(${batch}) AS x(
            "watermark" TEXT,
            "pos" INT8,
            "change" TEXT,
            "precommit" TEXT
          )`,
    ]);
    return tx.lastFlush;
  }

  #stopped = promiseVoid;

  /**
   * Runs the storer loop until {@link stop()} is called, or an error is thrown.
   * Once {@link run()} completes, it can be called again.
   */
  async run() {
    assert(!this.#running, `storer is already running`);

    const {promise: stopped, resolve: signalStopped} = resolver();
    this.#running = true;
    this.#stopped = stopped;

    this.#lc.info?.('starting storer');
    this.#progressMonitor.start(); // Note: This is stopped in stop()
    let err: unknown;
    try {
      await this.#processQueue();
    } catch (e) {
      err = e; // used in finally
      throw e;
    } finally {
      // Release any pending backpressure so the upstream can proceed
      if (this.#readyForMore !== null) {
        this.#readyForMore.resolve();
        this.#readyForMore = null;
      }
      this.#cancelQueueEntries(
        this.#queue.drain().filter(entry => entry !== undefined),
        err,
      );
      this.#running = false;
      signalStopped();
      this.#lc.info?.('storer stopped');
    }
  }

  #cancelQueueEntries(queue: QueueEntry[], e: unknown) {
    if (queue.length === 0) {
      return;
    }
    this.#lc.info?.(
      `canceling ${queue.length} entries from the changeLog queue`,
    );
    const err = e instanceof Error ? e : new AbortError('server shutting down');
    for (const entry of queue) {
      if (entry === 'stop') {
        continue;
      }
      const type = entry[0];
      switch (type) {
        case 'subscriber': {
          // Disconnect subscribers waiting to be caught up so that they can
          // reconnect and try again.
          const {subscriber} = entry[1];
          this.#lc.info?.(`disconnecting ${subscriber.id}`);
          subscriber.fail(err);
          break;
        }
      }
    }
  }

  async #processQueue() {
    let tx: PendingTransaction | SkippedTransaction | null = null;
    let msg: QueueEntry | false;

    // Track the progress of each (previous and next) queue entry before it is
    // processed in the loop.
    let lastTaskDone: TaskDoneFn | undefined;
    const nextMessage = async () => {
      lastTaskDone?.();
      lastTaskDone = undefined;
      const entry = await this.#queue.dequeue();
      if (entry !== 'stop') {
        lastTaskDone = this.#progressMonitor.trackTask({
          task: 'queue-entry',
          // Note: TaskKeys are logged, so only include entry[0] (the type) to
          // avoid logging application data.
          type: entry[0],
        });
      }
      return entry;
    };

    const catchupQueue: SubscriberAndMode[] = [];
    try {
      while ((msg = await nextMessage()) !== 'stop') {
        const [msgType] = msg;
        switch (msgType) {
          case 'ready': {
            const signalReady = msg[1];
            signalReady();
            continue;
          }
          case 'subscriber': {
            const subscriber = msg[1];
            if (tx) {
              catchupQueue.push(subscriber); // Wait for the current tx to complete.
            } else {
              await this.#startCatchup([subscriber]); // Catch up immediately.
            }
            continue;
          }
          case 'status': {
            const pending = this.#pendingOwnership;
            if (pending && !tx && msg[1] >= pending.head) {
              const pool = new TransactionPool(this.#lc, {
                mode: Mode.READ_COMMITTED,
              });
              pool.run(this.#inserter);
              const assumed = await this.#tryAssumeOwnership(
                pool,
                msg[1],
                false,
              );
              if (assumed === null) {
                pool.abort();
                await pool.done();
              } else {
                pool.setDone();
                await pool.done();
                this.#ownershipAssumed(assumed);
              }
            }
            continue;
          }
          case 'abort': {
            if (tx && !tx.skipped) {
              tx.pool.abort();
              await tx.pool.done();
            }
            tx = null;
            continue;
          }
        }
        // msgType === 'change'
        this.#approximateQueuedBytes -= msg[2].length;
        tx = await this.#processChange(tx, msg);
        // Released once the change has been processed (e.g. flushed), so that
        // the upstream is held back by the progress of the change log.
        this.#maybeReleaseBackPressure();
        if (tx === null) {
          // Before beginning the next transaction, open a READONLY snapshot to
          // concurrently catchup subscribers that were waiting for the
          // transaction to end.
          await this.#startCatchup(catchupQueue.splice(0));
        }
      }
      // Subscribers waiting for a transaction that was interrupted (i.e.
      // aborted before stopping) are disconnected so that they can reconnect,
      // like those that are still in the queue.
      this.#cancelQueueEntries(
        catchupQueue.splice(0).map(sub => ['subscriber', sub]),
        undefined,
      );
    } catch (e) {
      catchupQueue.forEach(({subscriber}) => subscriber.fail(e));
      throw e;
    }
  }

  /**
   * Processes a change from the queue, i.e. stores it in the change log as
   * part of the current transaction (or skips it, see
   * {@link assumeOwnershipAtHead}).
   *
   * @returns The transaction that is open after the change, or `null` if the
   *     change ended (or was outside of) a transaction.
   */
  async #processChange(
    tx: PendingTransaction | SkippedTransaction | null,
    msg: Extract<QueueEntry, ['change', ...unknown[]]>,
  ): Promise<PendingTransaction | SkippedTransaction | null> {
    const [_, watermark, json, storedChange, change] = msg;
    const tag = change?.tag;

    if (tag === 'begin') {
      assert(!tx, 'received BEGIN in the middle of a transaction');
      const pending = this.#pendingOwnership;
      if (pending) {
        // Backfill transactions are neither stored nor used to assume
        // ownership. See assumeOwnershipAtHead().
        if (change.skipAck) {
          pending.skippedBackfill = true;
          return {skipped: true, ack: false};
        }
        if (watermark <= pending.head) {
          return {skipped: true, ack: true};
        }
      }
      const pool = new TransactionPool(
        this.#lc.withContext('watermark', watermark),
        {mode: Mode.READ_COMMITTED},
      );
      pool.run(this.#inserter);

      const {promise, resolve, reject} = resolver<ReplicationOwner>();
      void promise.catch(() => {}); // handle rejections before the await

      const assumed = pending
        ? await this.#tryAssumeOwnership(pool, watermark, true)
        : undefined;
      if (assumed === null) {
        // Stored by the previous owner in the meantime.
        pool.abort();
        await pool.done();
        return {skipped: true, ack: true};
      }
      if (pending) {
        // Ownership was assumed in this transaction.
        resolve({owner: this.#taskID});
      } else {
        // Acquire a lock on the replicationState row to detect and/or prevent
        // a concurrent ownership change.
        void pool.process(tx => {
          tx<ReplicationOwner[]> /*sql*/ `
            SELECT "owner" FROM ${this.#cdc('replicationState')} FOR UPDATE`.then(
            ([result]) => resolve(result),
            reject,
          );
          return [];
        });
      }

      tx = {
        pool,
        preCommitWatermark: watermark,
        pos: 0,
        startingReplicationState: promise,
        ack: !change.skipAck,
        batch: [],
        lastFlush: undefined,
        ownershipAssumed: assumed,
      };
    } else if (tx?.skipped) {
      if (tag === 'commit' && tx.ack) {
        this.#onCommitted(['commit', change, {watermark}]);
      }
      return tag === 'commit' || tag === 'rollback' ? null : tx;
    } else {
      assert(tx, () => `received change outside of transaction: ${json}`);
      tx.pos++;
    }

    const entry: ChangeLogRow = {
      watermark: tag === 'commit' ? watermark : tx.preCommitWatermark,
      precommit: tag === 'commit' ? tx.preCommitWatermark : null,
      pos: tx.pos,
      // For backwards compatibility, only the change message is stored
      // in the cdc changeLog. It was serialized separately with the
      // downstream envelope, so no full-message scan is needed here.
      change: storedChange,
    };

    if (change !== null && isSchemaChange(change)) {
      // Schema changes carry backfill / table-metadata statements that
      // must be applied in stream order relative to the changeLog rows.
      // Flush any buffered rows first, then write this row together with
      // its metadata statements as a single unit (preserving the previous
      // per-change ordering for schema changes).
      await this.#flushChangeLog(tx);
      tx.lastFlush = tx.pool.process(sql => [
        sql`INSERT INTO ${this.#cdc('changeLog')} ${sql(entry)}`,
        ...this.#trackBackfillMetadata(sql, change),
      ]);
    } else {
      // Accumulate plain changeLog rows (begin, data changes, commit) and
      // write them as a single multi-row INSERT. Collapsing the per-change
      // single-row INSERTs into batches is the dominant cost reduction for
      // large transactions, where the previous one-statement-per-change
      // path dominated the upstream replication lag.
      tx.batch.push(entry);
      if (tx.batch.length >= this.#changeLogBatchSize) {
        // Bound pipeline depth (and thus memory) by awaiting the previous
        // flush before issuing the next. This is the batched analog of the
        // previous per-100-statement backpressure await, and likewise
        // guards against memory blowup on very large transactions.
        const prevFlush = tx.lastFlush;
        void this.#flushChangeLog(tx);
        await prevFlush;
      }
    }
    if (tag === 'commit') {
      // Flush any remaining buffered changeLog rows (including this commit
      // row) before updating the replication state, so the state update is
      // ordered after all changeLog inserts for this transaction.
      void this.#flushChangeLog(tx);

      const {owner} = await tx.startingReplicationState;
      if (owner !== this.#taskID) {
        // Ownership change reflected in the replicationState read in 'begin'.
        tx.pool.fail(
          new AbortError(`changeLog ownership has been assumed by ${owner}`),
        );
      } else {
        // Update the replication state.
        const lastWatermark = watermark;
        void tx.pool.process(tx => [
          tx`
        UPDATE ${this.#cdc('replicationState')} SET ${tx({lastWatermark})}`,
        ]);
        tx.pool.setDone();
      }

      await tx.pool.done();

      const {ownershipAssumed, ack} = tx;
      tx = null;
      if (ownershipAssumed) {
        this.#ownershipAssumed(ownershipAssumed);
      }
      // ACK the LSN to the upstream Postgres.
      if (ack) {
        this.#onCommitted(['commit', change, {watermark}]);
      }
    } else if (tag === 'rollback') {
      // Aborted transactions are not stored in the changeLog.
      tx.pool.abort();
      await tx.pool.done();
      tx = null;
    }
    return tx;
  }

  async #startCatchup(subs: SubscriberAndMode[]) {
    const numCatchups = subs.length;
    if (numCatchups === 0) {
      return;
    }

    const lc = this.#lc.withContext('pool', 'catchup');
    const {init, cleanup, snapshotID} = sharedSnapshot();
    const reader = new TransactionPool(lc, {
      mode: Mode.READONLY,
      init,
      cleanup,
      initialWorkers: subs.length,
    });

    // A dynamic connection pool is created for catchup, sized to the number
    // of subscribers being caught up.
    const catchupConns = this.#makeConnectionPool(
      'subscriber-catchup',
      numCatchups,
    );
    reader.run(catchupConns);

    let lastWatermark: string | undefined;
    const catchupSnapshotted = this.#progressMonitor.trackTask({
      task: 'capture-catchup-snapshot',
      subscribers: subs.map(({subscriber: s}) => s.id),
    });
    try {
      // Ensure that the transaction has started (and is thus holding a snapshot
      // of the database) before continuing on to commit more changes. This is
      // done by performing a single read on the db, which determines the
      // snapshot for the REPEATABLE_READ transaction.
      [{lastWatermark}] = await reader.processReadTask(
        sql => sql<{lastWatermark: string}[]>`
        SELECT "lastWatermark" FROM ${this.#cdc('replicationState')}
      `,
      );
      lc.info?.(
        `snapshotted db at ${lastWatermark} (${await snapshotID}) ` +
          `to catchup ${numCatchups} subscriber(s)`,
      );
    } catch (e) {
      subs.map(({subscriber}) => subscriber.fail(e));
      // Nothing will run the catchup tasks that normally finalize the pool
      // (below), so tear it down here. Otherwise its workers (each holding an
      // open READ ONLY transaction) and the connection pool are leaked.
      reader.abort();
      void catchupConns.end().catch(() => {});
      throw e;
    } finally {
      catchupSnapshotted();
    }

    // Run the actual catchup queries in the background. Errors are handled in
    // #catchup() by disconnecting the associated subscriber.
    void Promise.allSettled(
      subs.map(sub =>
        this.#catchup(
          lc.withContext('subscriber', sub.subscriber.id),
          sub,
          lastWatermark,
          reader,
        ),
      ),
    ).finally(() => {
      reader.setDone();
      void catchupConns.end().catch(() => {});
    });
  }

  async #catchup(
    lc: LogContext,
    {subscriber: sub, mode}: SubscriberAndMode,
    lastWatermark: string,
    reader: TransactionPool,
  ) {
    let entriesReceived = 0;
    let catchupProgressed = this.#progressMonitor.trackTask({
      task: 'catchup',
      subscriber: sub.id,
      entriesReceived,
    });
    try {
      lc.info?.(`starting catchup`);
      await reader.processReadTask(async tx => {
        lc.info?.(`catching up`);
        const start = Date.now();

        // When starting from initial-sync, there won't be a change with a watermark
        // equal to the replica version. This is the empty changeLog scenario.
        let watermarkFound = sub.watermark === this.#replicaVersion;
        let count = 0;
        let lastBatchConsumed: Promise<unknown> | undefined;

        for await (const entries of tx<ChangeLogEntry[]> /*sql*/ `
          SELECT watermark, change->'tag' as tag, change::text FROM ${this.#cdc('changeLog')}
           WHERE watermark >= ${sub.watermark}
             AND watermark <= ${lastWatermark}
           ORDER BY watermark, pos`.cursor(2000)) {
          catchupProgressed();
          entriesReceived += entries.length;

          try {
            // Wait for the last batch of entries to be consumed by the
            // subscriber before sending down the current batch. This pipelining
            // allows one batch of changes to be received from the change-db
            // while the previous batch of changes are sent to the subscriber,
            // resulting in flow control that caps the number of changes
            // referenced in memory to 2 * batch-size.
            const start = performance.now();
            await lastBatchConsumed;
            const elapsed = performance.now() - start;
            if (lastBatchConsumed) {
              lc[elapsed > 100 ? 'info' : 'debug']?.(
                `waited ${elapsed.toFixed(3)} ms for ${sub.id} to consume last batch of catchup entries`,
              );
            }

            for (const entry of entries) {
              if (entry.watermark === sub.watermark) {
                // This should be the first entry.
                // Catchup starts from *after* the watermark.
                watermarkFound = true;
              } else if (watermarkFound) {
                lastBatchConsumed = sub.catchup(
                  reconstructWatermarkedChange(entry),
                );
                count++;
              } else if (mode === 'backup') {
                throw new AutoResetSignal(
                  `backup replica at watermark ${sub.watermark} is behind change db: ${entry.watermark})`,
                );
              } else {
                lc.warn?.(
                  `rejecting subscriber at watermark ${sub.watermark} (earliest watermark: ${entry.watermark})`,
                );
                sub.close(
                  ErrorType.WatermarkTooOld,
                  `earliest supported watermark is ${entry.watermark} (requested ${sub.watermark})`,
                );
                return;
              }
            }
          } finally {
            // Track the db's read of the next chunk of changes.
            catchupProgressed = this.#progressMonitor.trackTask({
              task: 'catchup',
              subscriber: sub.id,
              entriesReceived,
            });
          }
        }
        catchupProgressed();

        if (watermarkFound) {
          await lastBatchConsumed;
          lc.info?.(
            `caught up ${sub.id} with ${count} changes (${
              Date.now() - start
            } ms)`,
          );
        } else {
          // The subscriber is ahead of the latest durable changeLog entry
          // (lastWatermark). This can legitimately happen: changes are
          // forwarded to subscribers (the backup replica and view-syncers)
          // concurrently with — and can outrun — the durable store, so a
          // replica may briefly lead the change DB after the storer falls
          // behind or the change-streamer restarts. No catchup is possible or
          // needed; once the change DB catches back up, forwarding resumes and
          // the subscriber dedups any watermarks it already has. Unlike the
          // AutoResetSignal / WatermarkTooOld cases above, this is not a gap in
          // replication history, so the subscriber is simply marked caught up.
          lc.warn?.(
            `subscriber ${sub.id} at watermark ${sub.watermark} is ahead of ` +
              `the latest durable watermark ${lastWatermark}; waiting for the ` +
              `change DB to catch up`,
          );
        }
        // Start draining messages buffered during catchup. The returned promise
        // is intentionally not awaited here: while the drain is in progress,
        // new sends keep appending to the subscriber backlog and inherit its
        // byte-based backpressure.
        void sub.setCaughtUp();
      });
    } catch (err) {
      lc.error?.(`error while catching up subscriber ${sub.id}`, err);
      if (err instanceof AutoResetSignal) {
        await markResetRequired(this.#db, this.#shard);
        this.#onFatal(err);
      }
      sub.fail(err);
    } finally {
      catchupProgressed();
    }
  }

  /**
   * Returns the db statements necessary to track backfill and table metadata
   * presented in the `change`, if any.
   *
   * This is the Postgres interpreter of {@link cookieOps}, which is also
   * interpreted against the SQLite change log by `ChangeLogCookieWriter`. The
   * two stores have to agree on every transition forever, and the failure mode
   * if they drift is a backfill that is silently never re-requested — so the
   * decision of what a schema change *means* lives in one place and only its
   * transport lives here.
   */
  #trackBackfillMetadata(sql: PostgresTransaction, change: SchemaChange) {
    return cookieOps(change).flatMap(op => this.#cookieStmts(sql, op));
  }

  #cookieStmts(sql: PostgresTransaction, op: CookieOp): PendingQuery<Row[]>[] {
    switch (op.op) {
      case 'upsert-metadata':
        return [this.#upsertTableMetadataStmt(sql, op.table, op.metadata)];

      case 'upsert-backfill':
        return [
          this.#upsertColumnBackfillStmt(sql, op.table, op.column, op.backfill),
        ];

      case 'rename-table': {
        const {old} = op;
        const row = {schema: op.new.schema, table: op.new.name};
        return [
          sql`UPDATE ${this.#cdc('tableMetadata')} SET ${sql(row)}
                WHERE "schema" = ${old.schema} AND "table" = ${old.name}`,
          sql`UPDATE ${this.#cdc('backfilling')} SET ${sql(row)}
                WHERE "schema" = ${old.schema} AND "table" = ${old.name}`,
        ];
      }

      case 'drop-table': {
        const {schema, name} = op.table;
        return [
          sql`DELETE FROM ${this.#cdc('tableMetadata')}
                WHERE "schema" = ${schema} AND "table" = ${name}`,
          sql`DELETE FROM ${this.#cdc('backfilling')}
                WHERE "schema" = ${schema} AND "table" = ${name}`,
        ];
      }

      case 'rename-column': {
        const {schema, name: table} = op.table;
        return [
          sql`UPDATE ${this.#cdc('backfilling')} SET "column" = ${op.new}
                WHERE "schema" = ${schema} AND "table" = ${table} AND "column" = ${op.old}`,
        ];
      }

      case 'drop-column': {
        const {schema, name} = op.table;
        return [
          sql`DELETE FROM ${this.#cdc('backfilling')}
                WHERE "schema" = ${schema} AND "table" = ${name} AND "column" = ${op.column}`,
        ];
      }

      case 'complete-backfill': {
        const {schema, name: table} = op.table;
        return [
          sql`DELETE FROM ${this.#cdc('backfilling')}
                WHERE "schema" = ${schema} AND "table" = ${table} AND "column" IN ${sql([...op.columns])}`,
        ];
      }

      default:
        unreachable(op);
    }
  }

  #upsertTableMetadataStmt(
    sql: PostgresTransaction,
    {schema, name: table}: Identifier,
    metadata: TableMetadata,
  ) {
    const row: TableMetadataRow = {schema, table, metadata};
    return sql`
        INSERT INTO ${this.#cdc('tableMetadata')} ${sql(row)}
          ON CONFLICT ("schema", "table")
          DO UPDATE SET ${sql(row)};
    `;
  }

  #upsertColumnBackfillStmt(
    sql: PostgresTransaction,
    {schema, name: table}: Identifier,
    column: string,
    backfill: BackfillID,
  ) {
    const row: BackfillingColumn = {schema, table, column, backfill};
    return sql`
        INSERT INTO ${this.#cdc('backfilling')} ${sql(row)}
          ON CONFLICT ("schema", "table", "column")
          DO UPDATE SET ${sql(row)};
    `;
  }

  /**
   * Waits until all currently queued entries have been processed.
   * This is only used in tests.
   */
  async allProcessed() {
    if (this.#running) {
      const {promise, resolve} = resolver();
      this.#queue.enqueue(['ready', resolve]);
      await promise;
    }
  }

  /**
   * Stops the storer and waits up to a drain timeout for entries to drain,
   * throwing an exception if it doesn't drain in time, in order to abort
   * the server when PG (or the connection to it) appears to be wedged.
   */
  async stop() {
    if (this.#running) {
      this.#progressMonitor.stop();
      this.#lc.info?.(`draining ${this.#queue.size()} changeLog entries`);
      this.abort(); // for cleanliness, abort any open transactions
      this.#queue.enqueue('stop');
    }
    if (
      (await orTimeout(this.#stopped, this.#drainTimeoutMs)) === 'timed-out'
    ) {
      throw new AbortError(
        `changeLog did not drain within ${this.#drainTimeoutMs}ms`,
      );
    }
  }
}

export class PurgeLock {
  readonly #lc: LogContext;
  readonly #tx: TransactionPool;
  readonly replicaVersion: string;
  readonly minWatermark: string;

  constructor(
    lc: LogContext,
    tx: TransactionPool,
    replicaVersion: string,
    watermark: string,
  ) {
    this.#lc = lc;
    this.#tx = tx;
    this.replicaVersion = replicaVersion;
    this.minWatermark = watermark;
  }

  #released = false;

  async release() {
    if (this.#released) {
      return;
    }
    this.#released = true;
    this.#tx.setDone();
    await this.#tx
      .done()
      .catch(e => this.#lc.warn?.(`error from purge-lock release`, e));
    this.#lc.info?.(`released purge lock on ${this.minWatermark}`);
  }
}

export class PurgeLocker {
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #db: PostgresDB;

  constructor(lc: LogContext, shard: ShardID, db: PostgresDB) {
    this.#lc = lc.withContext('component', 'purge-locker');
    this.#shard = shard;
    this.#db = db;
  }

  // For readability in SQL statements.
  #cdc(table: string) {
    return this.#db(`${cdcSchema(this.#shard)}.${table}`);
  }

  /**
   * Locks the oldest row of the change log, preventing it from being purged.
   *
   * @param slotWatermark The position of the replication slot from which the
   *     change log is to be resumed (as a watermark). If specified and the
   *     head of the change log is behind it, the change log may be missing
   *     transactions that the slot will not stream, in which case nothing is
   *     locked and `'behind-slot'` is returned. Conversely, a head at or past
   *     the slot's position means that the stream (which resumes from the
   *     head) will not skip anything.
   * @returns `null` if the change log is empty.
   */
  acquire(): Promise<PurgeLock | null>;
  acquire(
    slotWatermark?: string | undefined,
  ): Promise<PurgeLock | null | 'behind-slot'>;
  async acquire(
    slotWatermark?: string,
  ): Promise<PurgeLock | null | 'behind-slot'> {
    const tx = new TransactionPool(this.#lc, {mode: Mode.READ_COMMITTED}).run(
      this.#db,
    );
    if (slotWatermark !== undefined) {
      const [{lastWatermark}] = await tx.processReadTask(
        sql => sql<{lastWatermark: string}[]>`
        SELECT "lastWatermark" FROM ${this.#cdc('replicationState')}`,
      );
      if (lastWatermark < slotWatermark) {
        this.#lc.info?.(
          `changeLog@${lastWatermark} is behind the replication slot@${slotWatermark}`,
        );
        tx.setDone();
        await tx.done();
        return 'behind-slot';
      }
    }
    const row = await tx.processReadTask(
      sql => sql<{watermark: string}[]>`
      SELECT watermark FROM ${this.#cdc('changeLog')}
        ORDER BY watermark, pos LIMIT 1
        FOR SHARE 
    `,
    );
    if (row.length === 0) {
      this.#lc.info?.(`changeLog is empty. No rows to purge-lock.`);
      tx.setDone();
      await tx.done();
      return null;
    }
    const [{watermark}] = row;
    const [{replicaVersion}] = await tx.processReadTask(
      sql => sql<{replicaVersion: string}[]>`
        SELECT "replicaVersion" FROM ${this.#cdc('replicationConfig')}
      `,
    );
    this.#lc.info?.(
      `locked watermark ${watermark} from being purged from replica@${replicaVersion}`,
    );
    return new PurgeLock(this.#lc, tx, replicaVersion, watermark);
  }
}

// A TaskKey, keyed by identity, representing an db task monitored by the
// ProgressMonitor. The contents are stringified in error messages if the
// task fails to make progress for more than a failure threshold interval.
type TaskKey = JSONObject;

type TaskDoneFn = () => void;

/**
 * Periodic monitor for detecting the pathological condition in which a db
 * query hangs forever because of a connection problem, such as a half-closed
 * socket.
 *
 * Use this for db calls that recur or accumulate progress within a single
 * logical operation on the main storer loop (e.g. the queue-processing loop,
 * or a multi-batch catchup read), where "no progress for N ms" is the
 * meaningful signal. For a one-off db call -- including ones made before
 * {@link Storer.run} starts this monitor's polling -- use a plain timeout
 * (see `Storer.#withTimeout`) instead.
 *
 * The failure handler should stop the server. The ProgressMonitor will
 * assume this and stop itself after calling it.
 *
 * Internal to the Storer, but exported for testing.
 */
export class ProgressMonitor {
  readonly #lc: LogContext;
  readonly #failureThresholdMs;
  readonly #onFailure: (err: Error) => void;
  readonly #taskUpdates = new Map<TaskKey, Date>();

  #timer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    failureThresholdMs: number,
    onFailure: (err: Error) => void,
  ) {
    this.#lc = lc.withContext('component', 'storer-progress-monitor');
    this.#failureThresholdMs = failureThresholdMs;
    this.#onFailure = onFailure;
  }

  start() {
    clearInterval(this.#timer);
    this.#timer = setInterval(
      () => this.checkTaskProgress(new Date()),
      this.#failureThresholdMs,
    ).unref();
  }

  // Called by an internal periodic timer, but exported for testing.
  checkTaskProgress(now: Date) {
    for (const [task, lastUpdate] of this.#taskUpdates.entries()) {
      if (now.getTime() - lastUpdate.getTime() >= this.#failureThresholdMs) {
        this.#lc.error?.(
          `Last task update was more than ${this.#failureThresholdMs}ms ago`,
          {task, lastUpdate},
        );
        this.#onFailure(
          new Error(
            `Task failed to progress for over ${this.#failureThresholdMs}ms: ${BigIntJSON.stringify(task)}`,
          ),
        );
        this.stop();
        return;
      }
    }
  }

  /**
   * Starts a task to be tracked.
   *
   * @return a callback to invoke when the task is done.
   */
  trackTask(task: TaskKey, now = new Date()): TaskDoneFn {
    this.#taskUpdates.set(task, now);
    return () => {
      this.#taskUpdates.delete(task);
    };
  }

  stop() {
    clearInterval(this.#timer);
    this.#taskUpdates.clear();
  }
}
