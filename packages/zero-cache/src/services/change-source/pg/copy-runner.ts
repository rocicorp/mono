import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {Queue} from '../../../../../shared/src/queue.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {orTimeoutWith} from '../../../types/timeout.ts';

const CONNECTION_REUSE_TIMEOUT = 100;
const TIMED_OUT = {reason: 'commit timed out'};

type Task<T> = {
  copy: (tx: PostgresTransaction, lc: LogContext) => Promise<T>;
  result: Resolver<T>;
};

/**
 * The CopyRunner is designed to run a single `COPY` command within a
 * READ ONLY transaction, optionally at a specific SNAPSHOT, and reuse
 * the connection if it is healthy.
 *
 * This works around a bug in Postgres in which the database stops
 * responding to commands after a certain type / sequence of COPY
 * streams.
 */
export class CopyRunner {
  readonly #lc: LogContext;
  readonly #connect: () => PostgresDB;
  readonly #snapshotID: string | undefined;
  readonly #workQueue = new Queue<Task<unknown> | 'done'>();
  readonly #numWorkers: number;
  readonly #pool: {db: PostgresDB; connID: number}[] = [];

  #connID = 0;

  constructor(
    lc: LogContext,
    connect: () => PostgresDB,
    maxActive: number,
    snapshotID: string | undefined,
  ) {
    this.#lc = lc.withContext('component', 'copy-runner');
    this.#connect = connect;
    this.#numWorkers = maxActive;
    this.#snapshotID = snapshotID;

    for (let i = 0; i < this.#numWorkers; i++) {
      void this.#runWorker();
    }
  }

  /**
   * Runs the given `copy` logic when a connection is available,
   * ensuring that at most `maxActive` copy commands are running
   * at a given time.
   */
  run<T>(
    copy: (tx: PostgresTransaction, lc: LogContext) => Promise<T>,
  ): Promise<T> {
    const result = resolver<T>();
    this.#workQueue.enqueue({copy, result: result as Resolver<unknown>});
    return result.promise;
  }

  async #runWorker(): Promise<void> {
    for (;;) {
      const task = await this.#workQueue.dequeue();
      if (task === 'done') {
        break;
      }
      // The task is awaited to ensure that only one COPY is running at a time
      // for each worker. However, errors do not need to be handled in this
      // loop as they are propagated to the caller of run().
      await this.#run(task).catch(() => {});
    }
  }

  async #run(task: Task<unknown>): Promise<unknown> {
    const {db, connID} = this.#getConnection();
    const lc = this.#lc.withContext('conn', connID.toString());

    const {copy, result} = task;
    const txDone = db.begin(READONLY, tx => {
      if (this.#snapshotID) {
        void tx
          .unsafe(`SET TRANSACTION SNAPSHOT '${this.#snapshotID}'`)
          .execute();
      }
      return copy(tx, lc).then(result.resolve, result.reject);
    });

    function closeConnection() {
      void db.end().catch(e => lc.warn?.(`error closing connection`, e));
    }

    // If the transaction successfully completes, the `COMMIT` succeeded
    // and the connection is healthy, so the connection can be reused.
    // If the transaction does not complete within a timeout, it is considered
    // hung and the connection is closed.
    const committed = await orTimeoutWith(
      txDone,
      CONNECTION_REUSE_TIMEOUT,
      TIMED_OUT,
    ).catch(e => (e instanceof Error ? e : new Error(String(e))));

    if (committed === TIMED_OUT || committed instanceof Error) {
      lc.debug?.(`closing connection`, committed);
      closeConnection();
    } else {
      this.#pool.push({db, connID});
    }

    return result.promise;
  }

  #getConnection(): {db: PostgresDB; connID: number} {
    const reusable = this.#pool.pop();
    if (reusable) {
      this.#lc.debug?.(`reusing connection ${reusable.connID}`);
      return reusable;
    }
    const connID = ++this.#connID;
    this.#lc.debug?.(`establishing connection ${connID}`);
    return {db: this.#connect(), connID};
  }

  close() {
    for (let i = 0; i < this.#numWorkers; i++) {
      this.#workQueue.enqueue('done');
    }
    this.#pool.forEach(
      ({db, connID}) =>
        void db
          .end()
          .catch(e =>
            this.#lc
              .withContext('conn', connID.toString())
              .warn?.(`error closing connection`, e),
          ),
    );
  }
}
