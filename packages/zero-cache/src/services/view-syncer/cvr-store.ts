import {trace} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import type {MaybeRow, PendingQuery, Row} from 'postgres';
import {startAsyncSpan} from '../../../../otel/src/span.js';
import {version} from '../../../../otel/src/version.js';
import {assert} from '../../../../shared/src/asserts.js';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.js';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.js';
import {must} from '../../../../shared/src/must.js';
import {promiseVoid} from '../../../../shared/src/resolved-promises.js';
import {sleep} from '../../../../shared/src/sleep.js';
import {astSchema} from '../../../../zero-protocol/src/ast.js';
import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.js';
import * as Mode from '../../db/mode-enum.js';
import {TransactionPool} from '../../db/transaction-pool.js';
import type {JSONValue} from '../../types/bigint-json.js';
import {ErrorForClient, ErrorWithLevel} from '../../types/error-for-client.js';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.js';
import {rowIDString} from '../../types/row-key.js';
import type {Patch, PatchToVersion} from './client-handler.js';
import type {CVR, CVRSnapshot} from './cvr.js';
import {
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  rowRecordToRowsRow,
  type RowsRow,
  rowsRowToRowRecord,
} from './schema/cvr.js';
import {
  type ClientQueryRecord,
  type ClientRecord,
  cmpVersions,
  type CVRVersion,
  EMPTY_CVR_VERSION,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryPatch,
  type QueryRecord,
  type RowID,
  type RowRecord,
  versionFromString,
  versionString,
  versionToNullableCookie,
} from './schema/types.js';

type NotNull<T> = T extends null ? never : T;

export type CVRFlushStats = {
  instances: number;
  queries: number;
  desires: number;
  clients: number;
  rows: number;
  rowsDeferred: number;
  statements: number;
};

const tracer = trace.getTracer('cvr-store', version);

/**
 * The RowRecordCache is an in-memory cache of the `cvr.rows` tables that
 * operates as both a write-through and write-back cache.
 *
 * For "small" CVR updates (i.e. zero or small numbers of rows) the
 * RowRecordCache operates as write-through, executing commits in
 * {@link executeRowUpdates()} before they are {@link apply}-ed to the
 * in-memory state.
 *
 * For "large" CVR updates (i.e. with many rows), the cache switches to a
 * write-back mode of operation, in which {@link executeRowUpdates()} is a
 * no-op, and {@link apply()} initiates a background task to flush the pending
 * row changes to the store. This allows the client poke to be completed and
 * committed on the client without waiting for the heavyweight operation of
 * committing the row records to the CVR store.
 *
 * Note that when the cache is in write-back mode, all updates become
 * write-back (i.e. asynchronously flushed) until the pending update queue is
 * fully flushed. This is required because updates must be applied in version
 * order. As with all pending work systems in zero-cache, multiple pending
 * updates are coalesced to reduce buildup of work.
 *
 * ### High level consistency
 *
 * Note that the above caching scheme only applies to the row data in `cvr.rows`
 * and corresponding `cvr.rowsVersion` tables. CVR metadata and query
 * information, on the other hand, are always committed before completing the
 * client poke. In this manner, the difference between the `version` column in
 * `cvr.instances` and the analogous column in `cvr.rowsVersion` determines
 * whether the data in the store is consistent, or whether it is awaiting a
 * pending update.
 *
 * The logic in {@link CVRStore.load()} takes this into account by loading both
 * the `cvr.instances` version and the `cvr.rowsVersion` version and checking
 * if they are in sync, waiting for a configurable delay until they are.
 *
 * ### Eventual conversion
 *
 * In the event of a continual stream of mutations (e.g. an animation-style
 * app), it is conceivable that the row record data be continually behind
 * the CVR metadata. In order to effect eventual convergence, a new view-syncer
 * signals the current view-syncer to stop updating by writing new `owner`
 * information to the `cvr.instances` row. This effectively stops the mutation
 * processing (in {@link CVRStore.#checkVersionAndOwnership}) so that the row
 * data can eventually catch up, allowing the new view-syncer to take over.
 *
 * Of course, there is the pathological situation in which a view-syncer
 * process crashes before the pending row updates are flushed. In this case,
 * the wait timeout will elapse and the CVR considered invalid.
 */
class RowRecordCache {
  // The state in the #cache is always in sync with the CVR metadata
  // (i.e. cvr.instances). It may contain information that has not yet
  // been flushed to cvr.rows.
  #cache: Promise<CustomKeyMap<RowID, RowRecord>> | undefined;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #cvrID: string;
  readonly #failService: (e: unknown) => void;
  readonly #deferredRowFlushThreshold: number;
  readonly #setTimeout: typeof setTimeout;

  // Write-back cache state.
  readonly #pending = new CustomKeyMap<RowID, RowRecord>(rowIDString);
  #pendingRowsVersion: CVRVersion | null = null;
  #flushedRowsVersion: CVRVersion | null = null;
  #flushing: Resolver<void> | null = null;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    cvrID: string,
    failService: (e: unknown) => void,
    deferredRowFlushThreshold = 100,
    setTimeoutFn = setTimeout,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#cvrID = cvrID;
    this.#failService = failService;
    this.#deferredRowFlushThreshold = deferredRowFlushThreshold;
    this.#setTimeout = setTimeoutFn;
  }

  async #ensureLoaded(): Promise<CustomKeyMap<RowID, RowRecord>> {
    if (this.#cache) {
      return this.#cache;
    }
    const r = resolver<CustomKeyMap<RowID, RowRecord>>();
    // Set this.#cache immediately (before await) so that only one db
    // query is made even if there are multiple callers.
    this.#cache = r.promise;

    const cache: CustomKeyMap<RowID, RowRecord> = new CustomKeyMap(rowIDString);
    for await (const rows of this.#db<
      RowsRow[]
    >`SELECT * FROM cvr.rows WHERE "clientGroupID" = ${
      this.#cvrID
    } AND "refCounts" IS NOT NULL`
      // TODO(arv): Arbitrary page size
      .cursor(5000)) {
      for (const row of rows) {
        const rowRecord = rowsRowToRowRecord(row);
        cache.set(rowRecord.id, rowRecord);
      }
    }
    r.resolve(cache);
    return this.#cache;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#ensureLoaded();
  }

  /**
   * Applies the `rowRecords` corresponding to the `rowsVersion`
   * to the cache, indicating whether the corresponding updates
   * (generated by {@link executeRowUpdates}) were `flushed`.
   *
   * If `flushed` is false, the RowRecordCache will flush the records
   * asynchronously.
   *
   * Note that `apply()` indicates that the CVR metadata associated with
   * the `rowRecords` was successfully committed, which essentially means
   * that this process has the unconditional right (and responsibility) of
   * following up with a flush of the `rowRecords`. In particular, the
   * commit of row records are not conditioned on the version or ownership
   * columns of the `cvr.instances` row.
   */
  async apply(
    rowRecords: Iterable<RowRecord>,
    rowsVersion: CVRVersion,
    flushed: boolean,
  ) {
    const cache = await this.#ensureLoaded();
    for (const row of rowRecords) {
      if (row.refCounts === null) {
        cache.delete(row.id);
      } else {
        cache.set(row.id, row);
      }
      if (!flushed) {
        this.#pending.set(row.id, row);
      }
    }
    this.#pendingRowsVersion = rowsVersion;
    // Initiate a flush if not already flushing.
    if (!flushed && this.#flushing === null) {
      this.#flushing = resolver();
      this.#setTimeout(() => this.#flush(), 0);
    }
  }

  async #flush() {
    const flushing = must(this.#flushing);
    try {
      while (this.#pendingRowsVersion !== this.#flushedRowsVersion) {
        const start = Date.now();

        const {rows, rowsVersion} = await this.#db.begin(tx => {
          // Note: This code block is synchronous, guaranteeing that the
          // #pendingRowsVersion is consistent with the #pending rows.
          const rows = this.#pending.size;
          const rowsVersion = must(this.#pendingRowsVersion);
          this.executeRowUpdates(
            tx,
            rowsVersion,
            [...this.#pending.values()],
            'force',
          );
          this.#pending.clear();
          return {rows, rowsVersion};
        });
        this.#lc.debug?.(
          `flushed ${rows} rows@${versionString(rowsVersion)} (${
            Date.now() - start
          } ms)`,
        );
        this.#flushedRowsVersion = rowsVersion;
        // Note: apply() may have called while the transaction was committing,
        //       which will result in looping to commit the next #pendingRowsVersion.
      }
      this.#lc.debug?.(
        `up to date rows@${versionToNullableCookie(this.#flushedRowsVersion)}`,
      );
      flushing.resolve();
      this.#flushing = null;
    } catch (e) {
      flushing.reject(e);
      this.#failService(e);
    }
  }

  hasPendingUpdates() {
    return this.#flushing !== null;
  }

  /**
   * Returns a promise that resolves when all outstanding row-records
   * have been committed.
   */
  flushed(lc: LogContext): Promise<void> {
    if (this.#flushing) {
      lc.debug?.('awaiting pending row flush');
      return this.#flushing.promise;
    }
    return promiseVoid;
  }

  clear() {
    // Note: Only the #cache is cleared. #pending updates, on the other hand,
    // comprise canonical (i.e. already flushed) data and must be flushed
    // even if the snapshot of the present state (the #cache) is cleared.
    this.#cache = undefined;
  }

  async *catchupRowPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): AsyncGenerator<RowsRow[], void, undefined> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      return;
    }

    const startMs = Date.now();
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);
    lc.debug?.(`scanning row patches for clients from ${start}`);

    // Before accessing the CVR db, pending row records must be flushed.
    // Note that because catchupRowPatches() is called from within the
    // view syncer lock, this flush is guaranteed to complete since no
    // new CVR updates can happen while the lock is held.
    await this.flushed(lc);
    const flushMs = Date.now() - startMs;

    const reader = new TransactionPool(lc, Mode.READONLY).run(this.#db);
    try {
      // Verify that we are reading the right version of the CVR.
      await reader.processReadTask(tx =>
        checkVersion(tx, this.#cvrID, current),
      );

      const {query} = await reader.processReadTask(tx => {
        const query =
          excludeQueryHashes.length === 0
            ? tx<RowsRow[]>`SELECT * FROM cvr.rows
        WHERE "clientGroupID" = ${this.#cvrID}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}`
            : // Exclude rows that were already sent as part of query hydration.
              tx<RowsRow[]>`SELECT * FROM cvr.rows
        WHERE "clientGroupID" = ${this.#cvrID}
          AND "patchVersion" > ${start}
          AND "patchVersion" <= ${end}
          AND ("refCounts" IS NULL OR NOT "refCounts" ?| ${excludeQueryHashes})`;
        return {query};
      });

      yield* query.cursor(10000);
    } finally {
      reader.setDone();
    }

    const totalMs = Date.now() - startMs;
    lc.debug?.(
      `finished row catchup (flush: ${flushMs} ms, total: ${totalMs} ms)`,
    );
  }

  executeRowUpdates(
    tx: PostgresTransaction,
    version: CVRVersion,
    rowRecordsToFlush: RowRecord[],
    mode: 'allow-defer' | 'force',
  ): PendingQuery<Row[]>[] {
    if (
      mode === 'allow-defer' &&
      // defer if pending rows are being flushed
      (this.#flushing !== null ||
        // or if the new batch is above the limit.
        rowRecordsToFlush.length > this.#deferredRowFlushThreshold)
    ) {
      return [];
    }
    const rowRecordRows = rowRecordsToFlush.map(r =>
      rowRecordToRowsRow(this.#cvrID, r),
    );
    const rowsVersion = {
      clientGroupID: this.#cvrID,
      version: versionString(version),
    };
    const pending: PendingQuery<Row[]>[] = [
      tx`INSERT INTO cvr."rowsVersion" ${tx(rowsVersion)}
           ON CONFLICT ("clientGroupID") 
           DO UPDATE SET ${tx(rowsVersion)}`.execute(),
    ];

    if (rowRecordRows.length) {
      pending.push(
        tx`
  INSERT INTO cvr.rows(
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
  ) SELECT
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
    FROM json_to_recordset(${rowRecordRows}) AS x(
      "clientGroupID" TEXT,
      "schema" TEXT,
      "table" TEXT,
      "rowKey" JSONB,
      "rowVersion" TEXT,
      "patchVersion" TEXT,
      "refCounts" JSONB
  ) ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
    DO UPDATE SET "rowVersion" = excluded."rowVersion",
      "patchVersion" = excluded."patchVersion",
      "refCounts" = excluded."refCounts"
    `.execute(),
      );
      this.#lc.debug?.(`flushing ${rowRecordRows.length} rows`);
    }
    return pending;
  }
}

type QueryRow = {
  queryHash: string;
  clientAST: NotNull<JSONValue>;
  patchVersion: string | null;
  transformationHash: string | null;
  transformationVersion: string | null;
  internal: boolean | null;
  deleted: boolean | null;
};

function asQuery(row: QueryRow): QueryRecord {
  const ast = astSchema.parse(row.clientAST);
  const maybeVersion = (s: string | null) =>
    s === null ? undefined : versionFromString(s);
  return row.internal
    ? ({
        id: row.queryHash,
        ast,
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
        internal: true,
      } satisfies InternalQueryRecord)
    : ({
        id: row.queryHash,
        ast,
        patchVersion: maybeVersion(row.patchVersion),
        desiredBy: {},
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
      } satisfies ClientQueryRecord);
}

// The time to wait between load attempts.
const LOAD_ATTEMPT_INTERVAL_MS = 500;
// The maximum number of load() attempts if the rowsVersion is behind.
// This currently results in a maximum catchup time of ~5 seconds, after
// which we give up and consider the CVR invalid.
//
// TODO: Make this configurable with something like --max-catchup-wait-ms,
//       as it is technically application specific.
const MAX_LOAD_ATTEMPTS = 10;

export class CVRStore {
  readonly #taskID: string;
  readonly #id: string;
  readonly #db: PostgresDB;
  readonly #writes: Set<{
    stats: Partial<CVRFlushStats>;
    write: (
      tx: PostgresTransaction,
      lastConnectTime: number,
    ) => PendingQuery<MaybeRow[]>;
  }> = new Set();
  readonly #pendingRowRecordPuts = new CustomKeyMap<RowID, RowRecord>(
    rowIDString,
  );
  readonly #rowCache: RowRecordCache;
  readonly #loadAttemptIntervalMs: number;
  readonly #maxLoadAttempts: number;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    taskID: string,
    cvrID: string,
    failService: (e: unknown) => void,
    loadAttemptIntervalMs = LOAD_ATTEMPT_INTERVAL_MS,
    maxLoadAttempts = MAX_LOAD_ATTEMPTS,
    deferredRowFlushThreshold = 100, // somewhat arbitrary
    setTimeoutFn = setTimeout,
  ) {
    this.#db = db;
    this.#taskID = taskID;
    this.#id = cvrID;
    this.#rowCache = new RowRecordCache(
      lc,
      db,
      cvrID,
      failService,
      deferredRowFlushThreshold,
      setTimeoutFn,
    );
    this.#loadAttemptIntervalMs = loadAttemptIntervalMs;
    this.#maxLoadAttempts = maxLoadAttempts;
  }

  load(lc: LogContext, lastConnectTime: number): Promise<CVR> {
    return startAsyncSpan(tracer, 'cvr.load', async () => {
      let err: RowsVersionBehindError | undefined;
      for (let i = 0; i < this.#maxLoadAttempts; i++) {
        if (i > 0) {
          await sleep(this.#loadAttemptIntervalMs);
        }
        const result = await this.#load(lc, lastConnectTime);
        if (result instanceof RowsVersionBehindError) {
          lc.info?.(`attempt ${i + 1}: ${String(result)}`);
          err = result;
          continue;
        }
        return result;
      }
      assert(err);
      throw new ErrorForClient({
        kind: ErrorKind.ClientNotFound,
        message: `max attempts exceeded waiting for CVR@${err.cvrVersion} to catch up from ${err.rowsVersion}`,
      });
    });
  }

  async #load(
    lc: LogContext,
    lastConnectTime: number,
  ): Promise<CVR | RowsVersionBehindError> {
    const start = Date.now();

    const id = this.#id;
    const cvr: CVR = {
      id,
      version: EMPTY_CVR_VERSION,
      lastActive: 0,
      replicaVersion: null,
      clients: {},
      queries: {},
    };

    const [instance, clientsRows, queryRows, desiresRows] =
      await this.#db.begin(tx => [
        tx<
          (Omit<InstancesRow, 'clientGroupID'> & {rowsVersion: string | null})[]
        >`SELECT cvr."version", 
                 "lastActive", 
                 "replicaVersion", 
                 "owner", 
                 "grantedAt", 
                 rows."version" as "rowsVersion"
            FROM cvr.instances AS cvr
            LEFT JOIN cvr."rowsVersion" AS rows 
            ON cvr."clientGroupID" = rows."clientGroupID"
            WHERE cvr."clientGroupID" = ${id}`,
        tx<
          Pick<ClientsRow, 'clientID' | 'patchVersion'>[]
        >`SELECT "clientID", "patchVersion" FROM cvr.clients WHERE "clientGroupID" = ${id}`,
        tx<
          QueryRow[]
        >`SELECT * FROM cvr.queries WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
        tx<
          DesiresRow[]
        >`SELECT * FROM cvr.desires WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
      ]);

    if (instance.length === 0) {
      // This is the first time we see this CVR.
      this.putInstance({
        version: cvr.version,
        lastActive: 0,
        replicaVersion: null,
      });
    } else {
      assert(instance.length === 1);
      const {
        version,
        lastActive,
        replicaVersion,
        owner,
        grantedAt,
        rowsVersion,
      } = instance[0];

      if (owner !== this.#taskID) {
        if ((grantedAt ?? 0) > lastConnectTime) {
          throw new OwnershipError(owner, grantedAt);
        } else {
          // Fire-and-forget an ownership change to signal the current owner.
          // Note that the query is structured such that it only succeeds in the
          // correct conditions (i.e. gated on `grantedAt`).
          void this.#db`
            UPDATE cvr.instances SET "owner"     = ${this.#taskID}, 
                                     "grantedAt" = ${lastConnectTime}
              WHERE "clientGroupID" = ${this.#id} AND
                    ("grantedAt" IS NULL OR
                     "grantedAt" <= to_timestamp(${lastConnectTime / 1000}))
        `.execute();
        }
      }

      if (version !== (rowsVersion ?? EMPTY_CVR_VERSION.stateVersion)) {
        // This will cause the load() method to wait for row catchup and retry.
        // Assuming the ownership signal succeeds, the current owner will stop
        // modifying the CVR and flush its pending row changes.
        return new RowsVersionBehindError(version, rowsVersion);
      }

      cvr.version = versionFromString(version);
      cvr.lastActive = lastActive;
      cvr.replicaVersion = replicaVersion;
    }

    for (const row of clientsRows) {
      const version = versionFromString(row.patchVersion);
      cvr.clients[row.clientID] = {
        id: row.clientID,
        patchVersion: version,
        desiredQueryIDs: [],
      };
    }

    for (const row of queryRows) {
      const query = asQuery(row);
      cvr.queries[row.queryHash] = query;
    }

    for (const row of desiresRows) {
      const client = cvr.clients[row.clientID];
      if (!client) {
        // should be impossible unless CVR is corrupted
        lc.error?.(`Client ${row.clientID} not found`, cvr);
        throw new Error(`Client ${row.clientID} not found`);
      }
      client.desiredQueryIDs.push(row.queryHash);

      const query = cvr.queries[row.queryHash];
      if (query && !query.internal) {
        query.desiredBy[row.clientID] = versionFromString(row.patchVersion);
      }
    }
    lc.debug?.(
      `loaded cvr@${versionString(cvr.version)} (${Date.now() - start} ms)`,
    );

    return cvr;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#rowCache.getRowRecords();
  }

  getPendingRowRecord(id: RowID): RowRecord | undefined {
    return this.#pendingRowRecordPuts.get(id);
  }

  putRowRecord(row: RowRecord): void {
    this.#pendingRowRecordPuts.set(row.id, row);
  }

  putInstance({
    version,
    replicaVersion,
    lastActive,
  }: Pick<CVRSnapshot, 'version' | 'replicaVersion' | 'lastActive'>): void {
    this.#writes.add({
      stats: {instances: 1},
      write: (tx, lastConnectTime) => {
        const change: InstancesRow = {
          clientGroupID: this.#id,
          version: versionString(version),
          lastActive,
          replicaVersion,
          owner: this.#taskID,
          grantedAt: lastConnectTime,
        };
        return tx`
        INSERT INTO cvr.instances ${tx(change)} 
          ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`;
      },
    });
  }

  markQueryAsDeleted(version: CVRVersion, queryPatch: QueryPatch): void {
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE cvr.queries SET ${tx({
        patchVersion: versionString(version),
        deleted: true,
        transformationHash: null,
        transformationVersion: null,
      })}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${queryPatch.id}`,
    });
  }

  putQuery(query: QueryRecord): void {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: QueriesRow = query.internal
      ? {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: null,
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: true,
          deleted: false, // put vs del "got" query
        }
      : {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: maybeVersionString(query.patchVersion),
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: null,
          deleted: false, // put vs del "got" query
        };
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`INSERT INTO cvr.queries ${tx(change)}
      ON CONFLICT ("clientGroupID", "queryHash")
      DO UPDATE SET ${tx(change)}`,
    });
  }

  updateQuery(query: QueryRecord) {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: Pick<
      QueriesRow,
      | 'patchVersion'
      | 'transformationHash'
      | 'transformationVersion'
      | 'deleted'
    > = {
      patchVersion: query.internal
        ? null
        : maybeVersionString(query.patchVersion),
      transformationHash: query.transformationHash ?? null,
      transformationVersion: maybeVersionString(query.transformationVersion),
      deleted: false,
    };

    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE cvr.queries SET ${tx(change)}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${query.id}`,
    });
  }

  updateClientPatchVersion(clientID: string, patchVersion: CVRVersion): void {
    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`UPDATE cvr.clients
      SET "patchVersion" = ${versionString(patchVersion)}
      WHERE "clientGroupID" = ${this.#id} AND "clientID" = ${clientID}`,
    });
  }

  insertClient(client: ClientRecord): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      patchVersion: versionString(client.patchVersion),
      // TODO(arv): deleted is never set to true
      deleted: false,
    };

    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`INSERT INTO cvr.clients ${tx(change)}`,
    });
  }

  insertDesiredQuery(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    deleted: boolean,
  ): void {
    const change: DesiresRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      queryHash: query.id,
      patchVersion: versionString(newVersion),
      deleted,
    };
    this.#writes.add({
      stats: {desires: 1},
      write: tx => tx`
      INSERT INTO cvr.desires ${tx(change)}
        ON CONFLICT ("clientGroupID", "clientID", "queryHash")
        DO UPDATE SET ${tx(change)}
      `,
    });
  }

  catchupRowPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): AsyncGenerator<RowsRow[], void, undefined> {
    return this.#rowCache.catchupRowPatches(
      lc,
      afterVersion,
      upToCVR,
      current,
      excludeQueryHashes,
    );
  }

  async catchupConfigPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
  ): Promise<PatchToVersion[]> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      return [];
    }

    const startMs = Date.now();
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);
    lc.debug?.(`scanning config patches for clients from ${start}`);

    const reader = new TransactionPool(lc, Mode.READONLY).run(this.#db);
    try {
      // Verify that we are reading the right version of the CVR.
      await reader.processReadTask(tx => checkVersion(tx, this.#id, current));

      const [allDesires, clientRows, queryRows] = await reader.processReadTask(
        tx =>
          Promise.all([
            tx<DesiresRow[]>`SELECT * FROM cvr.desires
       WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
            tx<ClientsRow[]>`SELECT * FROM cvr.clients
       WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
            tx<
              Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]
            >`SELECT deleted, "queryHash", "patchVersion" FROM cvr.queries
      WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
          ]),
      );

      const ast = (id: string) => must(upToCVR.queries[id]).ast;

      const patches: PatchToVersion[] = [];
      for (const row of queryRows) {
        const {queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id}
          : {type: 'query', op: 'put', id, ast: ast(id)};
        const v = row.patchVersion;
        assert(v);
        patches.push({patch, toVersion: versionFromString(v)});
      }
      for (const row of clientRows) {
        const patch: Patch = {
          type: 'client',
          op: row.deleted ? 'del' : 'put',
          id: row.clientID,
        };
        patches.push({patch, toVersion: versionFromString(row.patchVersion)});
      }
      for (const row of allDesires) {
        const {clientID, queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id, clientID}
          : {type: 'query', op: 'put', id, clientID, ast: ast(id)};
        patches.push({patch, toVersion: versionFromString(row.patchVersion)});
      }

      lc.debug?.(
        `${patches.length} config patches (${Date.now() - startMs} ms)`,
      );
      return patches;
    } finally {
      reader.setDone();
    }
  }

  async #checkVersionAndOwnership(
    tx: PostgresTransaction,
    expectedCurrentVersion: CVRVersion,
    lastConnectTime: number,
  ): Promise<void> {
    const expected = versionString(expectedCurrentVersion);
    const result = await tx<
      Pick<InstancesRow, 'version' | 'owner' | 'grantedAt'>[]
    >`SELECT "version", "owner", "grantedAt" FROM cvr.instances 
        WHERE "clientGroupID" = ${this.#id}
        FOR UPDATE`.execute(); // Note: execute() immediately to send the query before others.
    const {version, owner, grantedAt} =
      result.length > 0
        ? result[0]
        : {
            version: EMPTY_CVR_VERSION.stateVersion,
            owner: null,
            grantedAt: null,
          };
    if (owner !== this.#taskID && (grantedAt ?? 0) > lastConnectTime) {
      throw new OwnershipError(owner, grantedAt);
    }
    if (version !== expected) {
      throw new ConcurrentModificationException(expected, version);
    }
  }

  async #flush(
    expectedCurrentVersion: CVRVersion,
    newVersion: CVRVersion,
    lastConnectTime: number,
  ): Promise<CVRFlushStats> {
    const stats: CVRFlushStats = {
      instances: 0,
      queries: 0,
      desires: 0,
      clients: 0,
      rows: 0,
      rowsDeferred: 0,
      statements: 0,
    };
    let rowRecordsToFlush: RowRecord[];
    if (this.#pendingRowRecordPuts.size === 0) {
      // Avoid fetching/awaiting this.getRowRecords() for config-only changes.
      rowRecordsToFlush = [];
    } else {
      const existingRowRecords = await this.getRowRecords();
      rowRecordsToFlush = [...this.#pendingRowRecordPuts.values()].filter(
        row => {
          const existing = existingRowRecords.get(row.id);
          return (
            (existing !== undefined || row.refCounts !== null) &&
            !deepEqual(
              row as ReadonlyJSONValue,
              existing as ReadonlyJSONValue | undefined,
            )
          );
        },
      );
    }
    const rowsFlushed = await this.#db.begin(async tx => {
      const pipelined: Promise<unknown>[] = [
        // #checkVersionAndOwnership() executes a `SELECT ... FOR UPDATE`
        // query to acquire a row-level lock so that version-updating
        // transactions are effectively serialized per cvr.instance.
        //
        // Note that `rowsVersion` updates, on the other hand, are not subject
        // to this lock and can thus commit / be-committed independently of
        // cvr.instances.
        this.#checkVersionAndOwnership(
          tx,
          expectedCurrentVersion,
          lastConnectTime,
        ),
      ];

      for (const write of this.#writes) {
        stats.instances += write.stats.instances ?? 0;
        stats.queries += write.stats.queries ?? 0;
        stats.desires += write.stats.desires ?? 0;
        stats.clients += write.stats.clients ?? 0;

        pipelined.push(write.write(tx, lastConnectTime).execute());
        stats.statements++;
      }

      const rowUpdates = this.#rowCache.executeRowUpdates(
        tx,
        newVersion,
        rowRecordsToFlush,
        'allow-defer',
      );
      pipelined.push(...rowUpdates);
      stats.statements += rowUpdates.length;

      // Make sure Errors thrown by pipelined statements
      // are propagated up the stack.
      await Promise.all(pipelined);

      if (rowUpdates.length === 0) {
        stats.rowsDeferred = rowRecordsToFlush.length;
        return false;
      }
      stats.rows = rowRecordsToFlush.length;
      return true;
    });
    await this.#rowCache.apply(rowRecordsToFlush, newVersion, rowsFlushed);
    return stats;
  }

  async flush(
    expectedCurrentVersion: CVRVersion,
    newVersion: CVRVersion,
    lastConnectTime: number,
  ): Promise<CVRFlushStats> {
    try {
      return await this.#flush(
        expectedCurrentVersion,
        newVersion,
        lastConnectTime,
      );
    } catch (e) {
      // Clear cached state if an error (e.g. ConcurrentModificationException) is encountered.
      this.#rowCache.clear();
      throw e;
    } finally {
      this.#writes.clear();
      this.#pendingRowRecordPuts.clear();
    }
  }

  hasPendingUpdates(): boolean {
    return this.#rowCache.hasPendingUpdates();
  }

  /** Resolves when all pending updates are flushed. */
  flushed(lc: LogContext): Promise<void> {
    return this.#rowCache.flushed(lc);
  }
}

/**
 * This is similar to {@link CVRStore.#checkVersionAndOwnership} except
 * that it only checks the version and is suitable for snapshot reads
 * (i.e. by doing a plain `SELECT` rather than a `SELECT ... FOR UPDATE`).
 */
async function checkVersion(
  tx: PostgresTransaction,
  clientGroupID: string,
  expectedCurrentVersion: CVRVersion,
): Promise<void> {
  const expected = versionString(expectedCurrentVersion);
  const result = await tx<Pick<InstancesRow, 'version'>[]>`
    SELECT version FROM cvr.instances WHERE "clientGroupID" = ${clientGroupID}`;
  const {version} =
    result.length > 0 ? result[0] : {version: EMPTY_CVR_VERSION.stateVersion};
  if (version !== expected) {
    throw new ConcurrentModificationException(expected, version);
  }
}

export class ConcurrentModificationException extends ErrorWithLevel {
  readonly name = 'ConcurrentModificationException';

  constructor(expectedVersion: string, actualVersion: string) {
    super(
      `CVR has been concurrently modified. Expected ${expectedVersion}, got ${actualVersion}`,
      'warn',
    );
  }
}

export class OwnershipError extends ErrorWithLevel {
  readonly name = 'OwnershipError';

  constructor(owner: string | null, grantedAt: number | null) {
    super(
      `CVR ownership was transferred to ${owner} at ${new Date(
        grantedAt ?? 0,
      ).toISOString()}`,
      'info',
    );
  }
}

export class RowsVersionBehindError extends Error {
  readonly name = 'RowsVersionBehindError';
  readonly cvrVersion: string;
  readonly rowsVersion: string | null;

  constructor(cvrVersion: string, rowsVersion: string | null) {
    super(`rowsVersion (${rowsVersion}) is behind CVR ${cvrVersion}`);
    this.cvrVersion = cvrVersion;
    this.rowsVersion = rowsVersion;
  }
}
