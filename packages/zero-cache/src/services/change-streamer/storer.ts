import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.js';
import {Queue} from '../../../../shared/src/queue.js';
import {promiseVoid} from '../../../../shared/src/resolved-promises.js';
import {Mode, TransactionPool} from '../../db/transaction-pool.js';
import type {JSONValue} from '../../types/bigint-json.js';
import type {PostgresDB} from '../../types/pg.js';
import type {Service} from '../service.js';
import type {WatermarkedChange} from './change-streamer-service.js';
import {ErrorType, type ChangeEntry, type Commit} from './change-streamer.js';
import {Subscriber} from './subscriber.js';

type QueueEntry = ['change', WatermarkedChange] | ['subscriber', Subscriber];

type PendingTransaction = {
  pool: TransactionPool;
  preCommitWatermark: string;
  pos: number;
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
  readonly #db: PostgresDB;
  readonly #replicaVersion: string;
  readonly #onCommit: (c: Commit) => void;
  readonly #queue = new Queue<QueueEntry>();
  readonly stopped = resolver<false>();

  constructor(
    lc: LogContext,
    db: PostgresDB,
    replicaVersion: string,
    onCommit: (c: Commit) => void,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#replicaVersion = replicaVersion;
    this.#onCommit = onCommit;
  }

  async getLastStoredWatermark(): Promise<string | null> {
    const result = await this.#db<
      {max: string | null}[]
    >`SELECT MAX(watermark) as max FROM cdc."changeLog"`;
    return result[0].max;
  }

  async purgeRecordsBefore(watermark: string): Promise<number> {
    // This is a sanity check to guarantee the invariant of the "changeLog"
    // that it always contains at least one entry (from which catchup can proceed),
    // unless no replication changes have flowed through the system
    // (i.e. watermark === replicaVersion).
    const exists = await this.#db`
      SELECT watermark FROM cdc."changeLog" WHERE watermark = ${watermark}`;
    // Watermark boundaries should always be "commit" entries, which are the sole
    // entry with that watermark (i.e. exists.length === 1). It follows that
    // catchup, which proceeds from the next entry, always starts with a
    // "begin" entry.
    if (exists.length !== 1 && watermark !== this.#replicaVersion) {
      this.#lc.warn?.(
        `rejecting attempted to purge up to watermark ${watermark} with ${exists.length} entries`,
      );
      return 0;
    }
    const result = await this.#db<{deleted: bigint}[]>`
      WITH purged AS (
        DELETE FROM cdc."changeLog" WHERE watermark < ${watermark} 
          RETURNING watermark, pos
      ) SELECT COUNT(*) as deleted FROM purged;`;

    return Number(result[0].deleted);
  }

  store(entry: WatermarkedChange) {
    void this.#queue.enqueue(['change', entry]);
  }

  catchup(sub: Subscriber) {
    void this.#queue.enqueue(['subscriber', sub]);
  }

  async run() {
    let tx: PendingTransaction | null = null;
    let next: QueueEntry | false;

    const catchupQueue: Subscriber[] = [];
    while (
      (next = await Promise.race([this.#queue.dequeue(), this.stopped.promise]))
    ) {
      if (next[0] === 'subscriber') {
        const subscriber = next[1];
        if (tx) {
          catchupQueue.push(subscriber); // Wait for the current tx to complete.
        } else {
          this.#processCatchup([subscriber]); // Catch up immediately.
        }
        continue;
      }
      // next[0] === 'change'
      const [watermark, downstream] = next[1];
      const [tag, change] = downstream;
      if (tag === 'begin') {
        assert(!tx, 'received BEGIN in the middle of a transaction');
        tx = {
          pool: new TransactionPool(
            this.#lc.withContext('watermark', watermark),
            Mode.SERIALIZABLE,
          ),
          preCommitWatermark: watermark,
          pos: 0,
        };
        tx.pool.run(this.#db);
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

      tx.pool.process(tx => [
        // Ignore conflicts to take into account transaction replay when an
        // acknowledgement doesn't reach upstream.
        tx`INSERT INTO cdc."changeLog" ${tx(entry)} ON CONFLICT DO NOTHING`,
      ]);

      if (tag === 'commit') {
        // Sanity check that there are no records between the preCommitWatermark
        // and the commit watermark.
        const {count} = await tx.pool.processReadTask(async db => {
          assert(tx);
          const results = await db<{count: number}[]>`
          SELECT COUNT(*) as count FROM cdc."changeLog"
              WHERE watermark > ${tx.preCommitWatermark} AND
                    watermark < ${entry.watermark}
          `;
          return results[0];
        });
        if (count > 0) {
          const err = new Error(
            `Unexpected entries between precommit ${tx.preCommitWatermark} and commit ${watermark}`,
          );
          tx.pool.fail(err);
          await tx.pool.done();
          throw err; // tx.pool.done() throws, but this makes it clearer.
        }

        tx.pool.setDone();
        await tx.pool.done();
        tx = null;

        // ACK the LSN to the upstream Postgres.
        this.#onCommit(downstream);

        // Before beginning the next transaction, open a READONLY snapshot to
        // concurrently catchup any queued subscribers.
        this.#processCatchup(catchupQueue.splice(0));
      }
    }

    this.#lc.info?.('storer stopped');
  }

  #processCatchup(subs: Subscriber[]) {
    if (subs.length === 0) {
      return;
    }

    const reader = new TransactionPool(
      this.#lc.withContext('pool', 'catchup'),
      Mode.READONLY,
    );
    reader.run(this.#db);

    // Run in the background. Errors are handled in #catchup() by disconnecting
    // the associated subscriber.
    void Promise.all(subs.map(sub => this.#catchup(sub, reader))).finally(() =>
      reader.setDone(),
    );
  }

  async #catchup(sub: Subscriber, reader: TransactionPool) {
    try {
      await reader.processReadTask(async tx => {
        const start = Date.now();

        // When starting from initial-sync, there won't be a change with a watermark
        // equal to the replica version. This is the empty changeLog scenario.
        let watermarkFound = sub.watermark === this.#replicaVersion;
        let count = 0;
        for await (const entries of tx<ChangeEntry[]>`
          SELECT watermark, change FROM cdc."changeLog"
           WHERE watermark >= ${sub.watermark}
           ORDER BY watermark, pos`.cursor(10000)) {
          for (const entry of entries) {
            if (entry.watermark === sub.watermark) {
              // This should be the first entry.
              // Catchup starts from *after* the watermark.
              watermarkFound = true;
            } else if (watermarkFound) {
              sub.catchup(toDownstream(entry));
              count++;
            } else {
              this.#lc.warn?.(
                `rejecting subscriber at watermark ${sub.watermark}`,
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
          // Flushes the backlog of messages buffered during catchup and
          // allows the subscription to forward subsequent messages immediately.
          sub.setCaughtUp();

          this.#lc.info?.(
            `caught up ${sub.id} with ${count} changes (${
              Date.now() - start
            } ms)`,
          );
        } else {
          this.#lc.warn?.(`rejecting subscriber at watermark ${sub.watermark}`);
          sub.close(
            ErrorType.WatermarkNotFound,
            `cannot catch up from requested watermark ${sub.watermark}`,
          );
        }
      });
    } catch (err) {
      sub.fail(err);
      this.#lc.error?.(`error while catching up subscriber ${sub.id}`, err);
    }
  }

  stop() {
    this.stopped.resolve(false);
    return promiseVoid;
  }
}

function toDownstream(entry: ChangeEntry): WatermarkedChange {
  const {watermark, change} = entry;
  switch (change.tag) {
    case 'begin':
      return [watermark, ['begin', change]];
    case 'commit':
      return [watermark, ['commit', change, {watermark}]];
    default:
      return [watermark, ['data', change]];
  }
}
