import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {Queue} from 'shared/src/queue.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import {Mode, TransactionPool} from 'zero-cache/src/db/transaction-pool.js';
import {JSONValue} from 'zero-cache/src/types/bigint-json.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Service} from '../service.js';
import {WatermarkedChange} from './change-streamer-service.js';
import {ChangeEntry, Commit} from './change-streamer.js';
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
 */
export class Storer implements Service {
  readonly id = 'storer';
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #onCommit: (c: Commit) => void;
  readonly #queue = new Queue<QueueEntry>();
  readonly stopped = resolver<false>();

  constructor(lc: LogContext, db: PostgresDB, onCommit: (c: Commit) => void) {
    this.#lc = lc;
    this.#db = db;
    this.#onCommit = onCommit;
  }

  async getLastStoredWatermark(): Promise<string | null> {
    const result = await this.#db<
      {max: string | null}[]
    >`SELECT MAX(watermark) as max FROM cdc."ChangeLog"`;
    return result[0].max;
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
        tx`INSERT INTO cdc."ChangeLog" ${tx(entry)} ON CONFLICT DO NOTHING`,
      ]);

      if (tag === 'commit') {
        // Sanity check that there are no records between the preCommitWatermark
        // and the commit watermark.
        const {count} = await tx.pool.processReadTask(async db => {
          assert(tx);
          const results = await db<{count: number}[]>`
          SELECT COUNT(*) as count FROM cdc."ChangeLog"
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
        let count = 0;
        for await (const entries of tx<ChangeEntry[]>`
          SELECT watermark, change FROM cdc."ChangeLog"
           WHERE watermark > ${sub.watermark}
           ORDER BY watermark, pos`.cursor(10000)) {
          for (const entry of entries) {
            sub.catchup(toDownstream(entry));
            count++;
          }
        }
        // Flushes the backlog of messages buffered during catchup and
        // allows the subscription to forward subsequent messages immediately.
        sub.setCaughtUp();

        this.#lc.info?.(
          `caught up ${sub.id} with ${count} changes (${
            Date.now() - start
          } ms)`,
        );
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
