import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {Queue} from 'shared/src/queue.js';
import {
  Mode,
  TransactionPool,
  synchronizedSnapshots,
} from '../../db/transaction-pool.js';
import type {LexiVersion} from 'zqlite-zero-cache-shared/src/lexi-version.js';
import type {PostgresDB} from '../../types/pg.js';

export type TransactionFn<T> = (
  writer: TransactionPool,
  readers: TransactionPool,
  preStateVersion: LexiVersion,
  invalidationRegistryVersion: LexiVersion | null,
  preStateSnapshotID: string,
) => Promise<T> | T;

export interface TransactionTrain {
  /**
   * Runs the next {@link TransactionFn} after others have completed.
   *
   * When the `fn` completes, the train will automatically call {@link TransactionPool.setDone setDone()}
   * on both the `readers` and `writer` pools (ref-count sharing is not supported) if
   * not already set done. It is also fine for the `fn` to explicitly call `setDone()` to
   * await transaction completion if required for the correctness of subsequent actions.
   */
  runNext<T>(fn: TransactionFn<T>): Promise<T>;
}

/**
 * Transactions handled by the Replicator are inherently serial.
 * In Postgres [logical replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW):
 *
 * > The logical replication protocol sends individual transactions one by one. This means that
 * > all messages between a pair of Begin and Commit messages belong to the same transaction.
 *
 * For correctness, it is important that each transaction be committed before the next one begun.
 * (Theoretically, more parallelism may be achieved by enabling streaming of in-progress
 * transactions. This will add a considerable amount of complexity and should be assessed judiciously.)
 *
 * In addition, the replication process supports filter registered by the application to produce
 * invalidation tags committed with each processed transaction. It is important that the
 * registration of new filters be done in such a way that the version of the registration
 * corresponds with the version at which the filter becomes active in the replication stream.
 * Consequently, changes to the set of registered filters (an uncommon event), are also serialized
 * in the stream of transactions processed by the replicator.
 *
 * ```
 * >-------->------->------->------->------->------->------->
 * TX 1 | TX 2 | Filter 1 | TX 3 | Filter 2 | TX 4 | TX 5 ...
 * >-------->------->------->------->------->------->------->
 * ```
 *
 * This serialization is guaranteed by two mechanisms.
 * 1. Postgres row-level locks are acquired via `SELECT ... FOR UPDATE` statements.
 * 2. Transaction processing and filter registration are serialized by this TransactionManager class.
 *
 * The first guarantees correctness in the midst of multiple Replicators running (e.g. during
 * a rolling update). The second prevents the holding of a connection while waiting for the lock
 * in the common case.
 *
 * The other job of the TransactionTrain is preemptive transaction initialization.
 * Every transaction executed by the Replicator needs to both (1) acquire the `SELECT ... FOR UPDATE`
 * lock and (2) read the current database state version (by querying the `TxLog` table.).
 * To reduce replication latency, whenever a transaction is committed, the next one is preemptively
 * initialized so that the initialization cost is incurred while the Replicator is idle rather
 * than when the next action needs to be processed.
 */
export class TransactionTrainService implements TransactionTrain {
  readonly #lc: LogContext;
  readonly #replica: PostgresDB;
  readonly #lock = new Lock();

  #idleTimeout: ReturnType<typeof setTimeout> | undefined;
  #started = false;
  #isStopped = false;
  #txPools = new Queue<TxPools>();

  constructor(lc: LogContext, replica: PostgresDB) {
    this.#lc = lc;
    this.#replica = replica;
  }

  runNext<T>(fn: TransactionFn<T>): Promise<T> {
    return this.#lock.withLock(async () => {
      const {
        writer,
        readers,
        stateVersion,
        invalidationRegistryVersion,
        preStateSnapshotID,
      } = await this.#txPools.dequeue();

      this.#clearIdleTimeout();

      try {
        return await fn(
          writer,
          readers,
          stateVersion,
          invalidationRegistryVersion,
          preStateSnapshotID,
        );
      } finally {
        if (writer.isRunning()) {
          writer.setDone();
          await writer.done(); // Required for correctness (row lock must be released).
        }
        if (readers.isRunning()) {
          readers.unref(); // Allow the readers to be shared with ref counting.
        }
      }
    });
  }

  #setIdleTimeout() {
    // The TransactionPool keeps its (initial) connections alive via keepalive pings.
    // However, because the TransactionTrain holds locks, unused transactions are
    // closed and refreshed periodically to avoid blocking database vacuuming.
    this.#idleTimeout = setTimeout(
      () => this.runNext(() => this.#lc.debug?.(`refreshing idle transaction`)),
      IDLE_TIMEOUT_MS,
    );
  }

  #clearIdleTimeout() {
    clearTimeout(this.#idleTimeout);
    this.#idleTimeout = undefined;
  }

  #createSnapshotSynchronizedPools() {
    const {exportSnapshot, cleanupExport, setSnapshot, snapshotID} =
      synchronizedSnapshots();
    const readers = new TransactionPool(
      this.#lc.withContext('pool', 'readers'),
      Mode.SERIALIZABLE, // exportSnapshot will set READ ONLY after exporting.
      exportSnapshot,
      cleanupExport,
      2,
      4, // TODO: Parameterize the max workers for the readers pool.
    );
    const writer = new TransactionPool(
      this.#lc.withContext('pool', 'writer'),
      Mode.SERIALIZABLE,
      setSnapshot,
    );
    return {writer, readers, snapshotID};
  }

  async run(): Promise<void> {
    assert(!this.#started, `Already started`);
    this.#started = true;

    while (!this.#isStopped) {
      const start = Date.now();

      const {writer, readers, snapshotID} =
        this.#createSnapshotSynchronizedPools();
      const writerDone = writer.run(this.#replica);
      const readersDone = readers.run(this.#replica);
      let txPools: TxPools | undefined;

      try {
        const versions = await writer.processReadTask(tx =>
          tx`
        SELECT MAX("stateVersion") FROM _zero."TxLog";
        SELECT "stateVersionAtLastSpecChange" as version
               FROM _zero."InvalidationRegistryVersion" 
               FOR UPDATE;`.simple(),
        );
        const stateVersion = versions[0][0].max ?? '00';
        const invalidationRegistryVersion = versions[1][0].version;
        writer.addLoggingContext('version', stateVersion);
        readers.addLoggingContext('version', stateVersion);
        this.#lc.debug?.(
          `initialized tx pools at version ${stateVersion} (${
            Date.now() - start
          } ms)`,
        );

        txPools = {
          writer,
          readers,
          stateVersion,
          invalidationRegistryVersion,
          preStateSnapshotID: await snapshotID,
        };

        this.#setIdleTimeout();
        void this.#txPools.enqueue(txPools);
      } catch (e) {
        // This may happen if the InvalidationRegistryVersion was concurrently updated (e.g. by another Replicator),
        // i.e. "PostgresError: could not serialize access due to concurrent update".
        // Terminate the pools and loop to retry.
        writer.fail(e);
        readers.fail(e);
      } finally {
        readersDone.catch(e => this.#lc.error?.('error from reader pool', e));
        const writerCleanup = writerDone
          .catch(e => this.#lc.error?.('error from writer pool', e))
          .finally(() => txPools && this.#txPools.delete(txPools));
        // Always wait for the _writer_ to complete before the next loop iteration to
        // ensure the desired tx visibility semantics. Because errors may occur while
        // the pools are still queued (connection errors, etc.), also ensure that the
        // txPools entry is deleted from the Queue.
        //
        // The readers pool, on the other hand, may be longer lived and shared with other
        // components such as the InvalidationWatcher, so do not await that pool.
        await writerCleanup;
      }
    }
  }

  // eslint-disable-next-line require-await
  async stop(): Promise<void> {
    assert(!this.#isStopped, 'Already stopped');
    this.#isStopped = true;

    // Stop any waiting tx pools.
    void this.#txPools.dequeue().then(({writer, readers}) => {
      writer.unref();
      readers.unref();
    });
  }
}

type TxPools = {
  writer: TransactionPool;
  readers: TransactionPool;
  stateVersion: LexiVersion;
  invalidationRegistryVersion: LexiVersion | null;
  preStateSnapshotID: string;
};

// Close and refresh idle transactions after 5 minutes
const IDLE_TIMEOUT_MS = 5 * 60_000;
