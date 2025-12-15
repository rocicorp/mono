import {PG_SERIALIZATION_FAILURE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {type JSONValue} from '../../../../shared/src/bigint-json.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as Mode from '../../db/mode-enum.ts';
import {TransactionPool} from '../../db/transaction-pool.ts';
import {disableStatementTimeout, type PostgresDB} from '../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import {type Commit} from '../change-source/protocol/current/downstream.ts';
import type {StatusMessage} from '../change-source/protocol/current/status.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {Service} from '../service.ts';
import type {WatermarkedChange} from './change-streamer-service.ts';
import {type ChangeEntry} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {
  AutoResetSignal,
  markResetRequired,
  type ReplicationState,
} from './schema/tables.ts';
import type {Subscriber} from './subscriber.ts';

type SubscriberAndMode = {
  subscriber: Subscriber;
  mode: ReplicatorMode;
};

type QueueEntry =
  | ['change', WatermarkedChange]
  | ['ready', callback: () => void]
  | ['subscriber', SubscriberAndMode]
  | StatusMessage
  | ['abort']
  | 'stop';

type PendingTransaction = {
  pool: TransactionPool;
  preCommitWatermark: string;
  pos: number;
  startingReplicationState: Promise<ReplicationState>;
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
  readonly #db: PostgresDB;
  readonly #replicaVersion: string;
  readonly #onConsumed: (c: Commit | StatusMessage) => void;
  readonly #onFatal: (err: Error) => void;
  readonly #queue = new Queue<QueueEntry>();
  readonly #backPressureThreshold: number;

  #running = false;

  constructor(
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    discoveryAddress: string,
    discoveryProtocol: string,
    db: PostgresDB,
    replicaVersion: string,
    onConsumed: (c: Commit | StatusMessage) => void,
    onFatal: (err: Error) => void,
    backPressureThreshold: number,
  ) {
    this.#lc = lc;
    this.#shard = shard;
    this.#taskID = taskID;
    this.#discoveryAddress = discoveryAddress;
    this.#discoveryProtocol = discoveryProtocol;
    this.#db = db;
    this.#replicaVersion = replicaVersion;
    this.#onConsumed = onConsumed;
    this.#onFatal = onFatal;
    this.#backPressureThreshold = backPressureThreshold;
  }

  // For readability in SQL statements.
  #cdc(table: string) {
    return this.#db(`${cdcSchema(this.#shard)}.${table}`);
  }

  async assumeOwnership() {
    const db = this.#db;
    const owner = this.#taskID;
    const ownerAddress = this.#discoveryAddress;
    const ownerProtocol = this.#discoveryProtocol;
    // we omit `ws://` so that old view syncer versions that are not expecting the protocol continue to not get it
    const addressWithProtocol =
      ownerProtocol === 'ws'
        ? ownerAddress
        : `${ownerProtocol}://${ownerAddress}`;
    await db`UPDATE ${this.#cdc('replicationState')} SET ${db({owner, ownerAddress: addressWithProtocol})}`;
    this.#lc.info?.(`assumed ownership at ${addressWithProtocol}`);
  }

  async getLastWatermarkToStartStream(): Promise<string> {
    // Before starting or restarting a stream from the change source,
    // wait for all queued changes to be processed so that we pick up
    // from the right spot.
    const {promise: ready, resolve} = resolver();
    this.#queue.enqueue(['ready', resolve]);
    await ready;

    const [{lastWatermark}] = await this.#db<{lastWatermark: string}[]>`
      SELECT "lastWatermark" FROM ${this.#cdc('replicationState')}`;
    return lastWatermark;
  }

  async getMinWatermarkForCatchup(): Promise<string | null> {
    const [{minWatermark}] = await this.#db<
      {minWatermark: string | null}[]
    > /*sql*/ `
      SELECT min(watermark) as "minWatermark" FROM ${this.#cdc('changeLog')}`;
    return minWatermark;
  }

  purgeRecordsBefore(watermark: string): Promise<number> {
    return this.#db.begin(Mode.SERIALIZABLE, async sql => {
      disableStatementTimeout(sql);

      // Check ownership before performing the purge. The server is expected to
      // exit immediately when an ownership change is detected, but checking
      // explicitly guards against race conditions.
      const [{owner}] = await sql<ReplicationState[]>`
        SELECT * FROM ${this.#cdc('replicationState')}`;
      if (owner !== this.#taskID) {
        this.#lc.warn?.(
          `Ignoring change log purge request (${watermark}) while not owner`,
        );
        return 0;
      }

      const [{deleted}] = await sql<{deleted: bigint}[]>`
        WITH purged AS (
          DELETE FROM ${this.#cdc('changeLog')} WHERE watermark < ${watermark} 
            RETURNING watermark, pos
        ) SELECT COUNT(*) as deleted FROM purged;`;
      return Number(deleted);
    });
  }

  store(entry: WatermarkedChange) {
    this.#queue.enqueue(['change', entry]);
  }

  abort() {
    this.#queue.enqueue(['abort']);
  }

  status(s: StatusMessage) {
    this.#queue.enqueue(s);
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
      this.#queue.size() > this.#backPressureThreshold
    ) {
      this.#lc.warn?.(
        `applying back pressure with ${this.#queue.size()} queued changes (threshold: ${this.#backPressureThreshold})`,
      );
      this.#readyForMore = resolver();
    }
    return this.#readyForMore?.promise;
  }

  #maybeReleaseBackPressure() {
    if (
      this.#readyForMore !== null &&
      // Wait for at least 10% of the threshold to free up.
      this.#queue.size() < this.#backPressureThreshold * 0.9
    ) {
      this.#lc.info?.(
        `releasing back pressure with ${this.#queue.size()} queued changes`,
      );
      this.#readyForMore.resolve();
      this.#readyForMore = null;
    }
  }

  async run() {
    this.#running = true;
    try {
      await this.#processQueue();
    } finally {
      this.#running = false;
      this.#lc.info?.('storer stopped');
    }
  }

  async #processQueue() {
    let tx: PendingTransaction | null = null;
    let msg: QueueEntry | false;

    const catchupQueue: SubscriberAndMode[] = [];
    while ((msg = await this.#queue.dequeue()) !== 'stop') {
      this.#maybeReleaseBackPressure();

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
        case 'status':
          this.#onConsumed(msg);
          continue;
        case 'abort': {
          if (tx) {
            tx.pool.abort();
            await tx.pool.done();
            tx = null;
          }
          continue;
        }
      }
      // msgType === 'change'
      const [watermark, downstream] = msg[1];
      const [tag, change] = downstream;
      if (tag === 'begin') {
        assert(!tx, 'received BEGIN in the middle of a transaction');
        const {promise, resolve, reject} = resolver<ReplicationState>();
        tx = {
          pool: new TransactionPool(
            this.#lc.withContext('watermark', watermark),
            Mode.SERIALIZABLE,
          ),
          preCommitWatermark: watermark,
          pos: 0,
          startingReplicationState: promise,
        };
        tx.pool.run(this.#db);
        // Pipeline a read of the current ReplicationState,
        // which will be checked before committing.
        void tx.pool.process(tx => {
          tx<ReplicationState[]>`
          SELECT * FROM ${this.#cdc('replicationState')}`.then(
            ([result]) => resolve(result),
            reject,
          );
          return [];
        });
      } else {
        assert(tx, `received ${tag} outside of transaction`);
        tx.pos++;
      }

      const entry = {
        watermark: tag === 'commit' ? watermark : tx.preCommitWatermark,
        precommit: tag === 'commit' ? tx.preCommitWatermark : null,
        pos: tx.pos,
        change: change as unknown as JSONValue,
      };

      const processed = tx.pool.process(tx => [
        tx`
        INSERT INTO ${this.#cdc('changeLog')} ${tx(entry)}`,
      ]);

      if (tag === 'data' && tx.pos % 10_000 === 0) {
        // Backpressure is exerted on commit when awaiting tx.pool.done().
        // However, backpressure checks need to be regularly done for
        // very large transactions in order to avoid memory blowup.
        await processed;
      }

      if (tag === 'commit') {
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

        try {
          await tx.pool.done();
        } catch (e) {
          if (
            e instanceof postgres.PostgresError &&
            e.code === PG_SERIALIZATION_FAILURE
          ) {
            // Ownership change happened after the replicationState was read in 'begin'.
            throw new AbortError(`changeLog ownership has changed`, {cause: e});
          }
          throw e;
        }

        tx = null;

        // ACK the LSN to the upstream Postgres.
        this.#onConsumed(downstream);

        // Before beginning the next transaction, open a READONLY snapshot to
        // concurrently catchup any queued subscribers.
        await this.#startCatchup(catchupQueue.splice(0));
      } else if (tag === 'rollback') {
        // Aborted transactions are not stored in the changeLog. Abort the current tx
        // and process catchup of subscribers that were waiting for it to end.
        tx.pool.abort();
        await tx.pool.done();
        tx = null;

        await this.#startCatchup(catchupQueue.splice(0));
      }
    }
  }

  async #startCatchup(subs: SubscriberAndMode[]) {
    if (subs.length === 0) {
      return;
    }

    const reader = new TransactionPool(
      this.#lc.withContext('pool', 'catchup'),
      Mode.READONLY,
    );
    reader.run(this.#db);

    // Ensure that the transaction has started (and is thus holding a snapshot
    // of the database) before continuing on to commit more changes. This is
    // done by waiting for a no-op task to be processed by the pool, which
    // indicates that the BEGIN statement has been sent to the database.
    await reader.processReadTask(() => {});

    // Run the actual catchup queries in the background. Errors are handled in
    // #catchup() by disconnecting the associated subscriber.
    void Promise.all(subs.map(sub => this.#catchup(sub, reader))).finally(() =>
      reader.setDone(),
    );
  }

  async #catchup(
    {subscriber: sub, mode}: SubscriberAndMode,
    reader: TransactionPool,
  ) {
    try {
      await reader.processReadTask(async tx => {
        const start = Date.now();

        // When starting from initial-sync, there won't be a change with a watermark
        // equal to the replica version. This is the empty changeLog scenario.
        let watermarkFound = sub.watermark === this.#replicaVersion;
        let count = 0;
        let lastBatchConsumed: Promise<unknown> | undefined;

        for await (const entries of tx<ChangeEntry[]>`
          SELECT watermark, change FROM ${this.#cdc('changeLog')}
           WHERE watermark >= ${sub.watermark}
           ORDER BY watermark, pos`.cursor(2000)) {
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
            (elapsed > 100 ? this.#lc.info : this.#lc.debug)?.(
              `waited ${elapsed.toFixed(3)} ms for ${sub.id} to consume last batch of catchup entries`,
            );
          }

          for (const entry of entries) {
            if (entry.watermark === sub.watermark) {
              // This should be the first entry.
              // Catchup starts from *after* the watermark.
              watermarkFound = true;
            } else if (watermarkFound) {
              lastBatchConsumed = sub.catchup(toDownstream(entry)).result;
              count++;
            } else if (mode === 'backup') {
              throw new AutoResetSignal(
                `backup replica at watermark ${sub.watermark} is behind change db: ${entry.watermark})`,
              );
            } else {
              this.#lc.warn?.(
                `rejecting subscriber at watermark ${sub.watermark} (earliest watermark: ${entry.watermark})`,
              );
              sub.close(
                ErrorType.WatermarkTooOld,
                `earliest supported watermark is ${entry.watermark} (requested ${sub.watermark})`,
              );
              return;
            }
          }
        }
        if (watermarkFound) {
          await lastBatchConsumed;
          this.#lc.info?.(
            `caught up ${sub.id} with ${count} changes (${
              Date.now() - start
            } ms)`,
          );
        } else {
          this.#lc.warn?.(
            `subscriber at watermark ${sub.watermark} is ahead of latest watermark`,
          );
        }
        // Flushes the backlog of messages buffered during catchup and
        // allows the subscription to forward subsequent messages immediately.
        sub.setCaughtUp();
      });
    } catch (err) {
      this.#lc.error?.(`error while catching up subscriber ${sub.id}`, err);
      if (err instanceof AutoResetSignal) {
        await markResetRequired(this.#db, this.#shard);
        this.#onFatal(err);
      }
      sub.fail(err);
    }
  }

  stop() {
    this.#queue.enqueue('stop');
    return promiseVoid;
  }
}

function toDownstream(entry: ChangeEntry): WatermarkedChange {
  const {watermark, change} = entry;
  switch (change.tag) {
    case 'begin':
      return [watermark, ['begin', change, {commitWatermark: watermark}]];
    case 'commit':
      return [watermark, ['commit', change, {watermark}]];
    case 'rollback':
      return [watermark, ['rollback', change]];
    default:
      return [watermark, ['data', change]];
  }
}
