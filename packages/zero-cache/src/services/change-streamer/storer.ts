import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {Queue} from 'shared/src/queue.js';
import {promiseVoid} from 'shared/src/resolved-promises.js';
import {Mode, TransactionPool} from 'zero-cache/src/db/transaction-pool.js';
import {JSONValue} from 'zero-cache/src/types/bigint-json.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Service} from '../service.js';
import {ChangeEntry} from './change-streamer.js';
import {MessageCommit} from './schema/change.js';
import {Subscriber} from './subscriber.js';

type QueueEntry = ['change', ChangeEntry] | ['subscriber', Subscriber];

/**
 * Handles the storage of changes and the catchup of subscribers
 * that are behind.
 */
export class Storer implements Service {
  readonly id = 'storer';
  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #onCommit: (c: MessageCommit) => void;
  readonly #queue = new Queue<QueueEntry>();
  readonly stopped = resolver<false>();

  constructor(
    lc: LogContext,
    db: PostgresDB,
    onCommit: (c: MessageCommit) => void,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#onCommit = onCommit;
  }

  store(entry: ChangeEntry) {
    void this.#queue.enqueue(['change', entry]);
  }

  catchup(sub: Subscriber) {
    void this.#queue.enqueue(['subscriber', sub]);
  }

  async run() {
    let tx: TransactionPool | null = null;
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
      const {watermark, change} = next[1];
      const {tag} = change;
      if (tag === 'begin') {
        assert(!tx, 'received BEGIN in the middle of a transaction');
        tx = new TransactionPool(
          this.#lc.withContext('watermark', watermark),
          Mode.SERIALIZABLE,
        );
        void tx.run(this.#db);
      }

      assert(tx, `received ${tag} outside of transaction`);
      const entry = {watermark, change: change as unknown as JSONValue};
      tx.process(tx => [
        // Ignore conflicts to take into account transaction replay when an
        // acknowledgement doesn't reach upstream.
        tx`INSERT INTO cdc."ChangeLog" ${tx(entry)} ON CONFLICT DO NOTHING`,
      ]);

      if (tag === 'commit') {
        tx.setDone();
        await tx.done();
        tx = null;

        // ACK the LSN to the upstream Postgres.
        this.#onCommit(change);

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
    void reader.run(this.#db);

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
           WHERE watermark > ${sub.watermark}`.cursor(10000)) {
          for (const entry of entries) {
            sub.catchup(entry);
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
