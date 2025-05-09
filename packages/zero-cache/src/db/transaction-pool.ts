import type {LogContext} from '@rocicorp/logger';
import {type Resolver, resolver} from '@rocicorp/resolver';
import type postgres from 'postgres';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import {Queue} from '../../../shared/src/queue.ts';
import type {PostgresDB, PostgresTransaction} from '../types/pg.ts';
import * as Mode from './mode-enum.ts';

type Mode = Enum<typeof Mode>;

type MaybePromise<T> = Promise<T> | T;

export type Statement =
  | postgres.PendingQuery<(postgres.Row & Iterable<postgres.Row>)[]>
  | postgres.PendingQuery<postgres.Row[]>;

/**
 * A {@link Task} is logic run from within a transaction in a {@link TransactionPool}.
 * It returns a list of `Statements` that the transaction executes asynchronously and
 * awaits when it receives the 'done' signal.
 *
 */
export type Task = (
  tx: PostgresTransaction,
  lc: LogContext,
) => MaybePromise<Statement[]>;

/**
 * A {@link ReadTask} is run from within a transaction, but unlike a {@link Task},
 * the results of a ReadTask are opaque to the TransactionPool and returned to the
 * caller of {@link TransactionPool.processReadTask}.
 */
export type ReadTask<T> = (
  tx: PostgresTransaction,
  lc: LogContext,
) => MaybePromise<T>;

/**
 * A TransactionPool is a pool of one or more {@link postgres.TransactionSql}
 * objects that participate in processing a dynamic queue of tasks.
 *
 * This can be used for serializing a set of tasks that arrive asynchronously
 * to a single transaction (for writing) or performing parallel reads across
 * multiple connections at the same snapshot (e.g. read only snapshot transactions).
 */
export class TransactionPool {
  #lc: LogContext;
  readonly #mode: Mode;
  readonly #init: QueuedTask | undefined;
  readonly #cleanup: QueuedTask | undefined;
  readonly #tasks = new Queue<TaskTracker | Error | 'done'>();
  readonly #workers: Promise<unknown>[] = [];
  readonly #initialWorkers: number;
  readonly #maxWorkers: number;
  readonly #timeoutTask: TimeoutTasks;
  #numWorkers: number;
  #numWorking = 0;
  #db: PostgresDB | undefined; // set when running. stored to allow adaptive pool sizing.

  #refCount = 1;
  #done = false;
  #failure: Error | undefined;

  /**
   * @param init A {@link Task} that is run in each Transaction before it begins
   *             processing general tasks. This can be used to to set the transaction
   *             mode, export/set snapshots, etc. This will be run even if
   *             {@link fail} has been called on the pool.
   * @param cleanup A {@link Task} that is run in each Transaction before it closes.
   *                This will be run even if {@link fail} has been called, or if a
   *                preceding Task threw an Error.
   * @param initialWorkers The initial number of transaction workers to process tasks.
   *                       This is the steady state number of workers that will be kept
   *                       alive if the TransactionPool is long lived.
   *                       This must be greater than 0. Defaults to 1.
   * @param maxWorkers When specified, allows the pool to grow to `maxWorkers`. This
   *                   must be greater than or equal to `initialWorkers`. On-demand
   *                   workers will be shut down after an idle timeout of 5 seconds.
   */
  constructor(
    lc: LogContext,
    mode: Mode,
    init?: Task,
    cleanup?: Task,
    initialWorkers = 1,
    maxWorkers = initialWorkers,
    timeoutTasks = TIMEOUT_TASKS, // Overridden for tests.
  ) {
    assert(initialWorkers > 0);
    assert(maxWorkers >= initialWorkers);

    this.#lc = lc;
    this.#mode = mode;
    this.#init = init ? queuedStmt(init) : undefined;
    this.#cleanup = cleanup ? queuedStmt(cleanup) : undefined;
    this.#initialWorkers = initialWorkers;
    this.#numWorkers = initialWorkers;
    this.#maxWorkers = maxWorkers;
    this.#timeoutTask = timeoutTasks;
  }

  /**
   * Starts the pool of workers to process Tasks with transactions opened from the
   * specified {@link db}.
   */
  run(db: PostgresDB): this {
    assert(!this.#db, 'already running');
    this.#db = db;
    for (let i = 0; i < this.#numWorkers; i++) {
      this.#addWorker(db);
    }
    return this;
  }

  /**
   * Adds context parameters to internal LogContext. This is useful for context values that
   * are not known when the TransactionPool is constructed (e.g. determined after a database
   * call when the pool is running).
   *
   * Returns an object that can be used to add more parameters.
   */
  addLoggingContext(key: string, value: string) {
    this.#lc = this.#lc.withContext(key, value);

    return {
      addLoggingContext: (key: string, value: string) =>
        this.addLoggingContext(key, value),
    };
  }

  /**
   * Returns a promise that:
   *
   * * resolves after {@link setDone} has been called (or the the pool as been {@link unref}ed
   *   to a 0 ref count), once all added tasks have been processed and all transactions have been
   *   committed or closed.
   *
   * * rejects if processing was aborted with {@link fail} or if processing any of
   *   the tasks resulted in an error. All uncommitted transactions will have been
   *   rolled back.
   *
   * Note that partial failures are possible if processing writes with multiple workers
   * (e.g. `setDone` is called, allowing some workers to commit, after which other
   *  workers encounter errors). Using a TransactionPool in this manner does not make
   * sense in terms of transactional semantics, and is thus not recommended.
   *
   * For reads, however, multiple workers is useful for performing parallel reads
   * at the same snapshot. See {@link synchronizedSnapshots} for an example.
   * Resolves or rejects when all workers are done or failed.
   */
  async done() {
    const numWorkers = this.#workers.length;
    await Promise.all(this.#workers);

    if (numWorkers < this.#workers.length) {
      // If workers were added after the initial set, they must be awaited to ensure
      // that the results (i.e. rejections) of all workers are accounted for. This only
      // needs to be re-done once, because the fact that the first `await` completed
      // guarantees that the pool is in a terminal state and no new workers can be added.
      await Promise.all(this.#workers);
    }
    this.#lc.debug?.('transaction pool done');
  }

  #addWorker(db: PostgresDB) {
    const id = this.#workers.length + 1;
    const lc = this.#lc.withContext('tx', id);

    const tt: TimeoutTask =
      this.#workers.length < this.#initialWorkers
        ? this.#timeoutTask.forInitialWorkers
        : this.#timeoutTask.forExtraWorkers;
    const {timeoutMs} = tt;
    const timeoutTask =
      tt.task === 'done' ? 'done' : {task: queuedStmt(tt.task)};

    const worker = async (tx: PostgresTransaction) => {
      try {
        const start = performance.now();
        lc.debug?.('started transaction');

        const pending: Promise<unknown>[] = [];

        let stmts = 0;
        const executeTask = async (taskTracker: TaskTracker) => {
          const {task, resolver} = taskTracker;

          task !== this.#init && this.#numWorking++;
          let result;
          try {
            result = await task(tx, lc);
            if (result instanceof Statements) {
              // Execute the statements (i.e. send to the db) immediately and add them to
              // `pending` for the final await.
              //
              // Optimization: Fail immediately on rejections to prevent more tasks from
              // queueing up. This can save a lot of time if an initial task fails before
              // many subsequent tasks (e.g. transaction replay detection).
              pending.push(
                ...result.stmts.map(stmt =>
                  stmt
                    .execute()
                    .then(() => {
                      if (++stmts % 1000 === 0) {
                        lc.debug?.(
                          `executed ${stmts} statements (${performance.now() - start} ms)`,
                        );
                      }
                    })
                    .catch(e => this.fail(e)),
                ),
              );
            }
          } catch (e) {
            if (resolver) {
              resolver.reject(e); // Reject the ReadTask only.
            } else {
              throw e; // Fail the pool for (write) Tasks.
            }
          } finally {
            task !== this.#init && this.#numWorking--;
            resolver?.resolve(
              // Note: This must be a ReadResult because of the TaskTracker type.
              result instanceof ReadResult ? result.result : result?.stmts,
            );
          }
        };

        let task: TaskTracker | Error | 'done' = this.#init
          ? {task: this.#init}
          : await this.#tasks.dequeue(timeoutTask, timeoutMs);

        try {
          while (task !== 'done') {
            if (
              task instanceof Error ||
              (task.task !== this.#init && this.#failure)
            ) {
              await Promise.allSettled(pending); // avoid unhandled rejections
              throw this.#failure ?? task;
            }
            await executeTask(task);

            // await the next task.
            task = await this.#tasks.dequeue(timeoutTask, timeoutMs);
          }
        } finally {
          // Execute the cleanup task even on failure.
          if (this.#cleanup) {
            await executeTask({task: this.#cleanup});
          }
        }

        lc.debug?.('closing transaction');
        return Promise.all(pending);
      } catch (e) {
        if (e !== this.#failure) {
          lc.error?.('error from worker', e);
          this.fail(e); // A failure in any worker should fail the pool.
        }
        throw e;
      }
    };

    this.#workers.push(
      db
        .begin(this.#mode, worker)
        .catch(e => {
          if (e instanceof RollbackSignal) {
            // A RollbackSignal is used to gracefully rollback the postgres.js
            // transaction block. It should not be thrown up to the application.
            lc.debug?.('aborted transaction');
          } else {
            throw e;
          }
        })
        .finally(() => this.#numWorkers--),
    );

    // After adding the worker, enqueue a terminal signal if we are in either of the
    // terminal states (both of which prevent more tasks from being enqueued), to ensure
    // that the added worker eventually exits.
    if (this.#done) {
      this.#tasks.enqueue('done');
    }
    if (this.#failure) {
      this.#tasks.enqueue(this.#failure);
    }
  }

  process(task: Task): void {
    this.#trackAndProcess({task: queuedStmt(task)});
  }

  #trackAndProcess(taskTracker: TaskTracker): void {
    assert(!this.#done, 'already set done');
    if (this.#failure) {
      taskTracker.resolver?.reject(this.#failure);
      return;
    }

    this.#tasks.enqueue(taskTracker);

    // Check if the pool size can and should be increased.
    if (this.#numWorkers < this.#maxWorkers) {
      const outstanding = this.#tasks.size();

      if (outstanding > this.#numWorkers - this.#numWorking) {
        this.#db && this.#addWorker(this.#db);
        this.#numWorkers++;
        this.#lc.debug?.(`Increased pool size to ${this.#numWorkers}`);
      }
    }
  }

  processReadTask<T>(readTask: ReadTask<T>): Promise<T> {
    const r = resolver<T>();
    this.#trackAndProcess({
      task: queuedRead(readTask) as QueuedTask<ReadResult<unknown>>,
      resolver: r as Resolver<unknown>,
    });
    return r.promise;
  }

  /**
   * Ends all workers with a ROLLBACK. Throws if the pool is already done
   * or aborted.
   */
  abort() {
    this.fail(new RollbackSignal());
  }

  /**
   * Signals to all workers to end their transaction once all pending tasks have
   * been completed. Throws if the pool is already done or aborted.
   */
  setDone() {
    assert(!this.#done, 'already set done');
    this.#done = true;

    for (let i = 0; i < this.#numWorkers; i++) {
      this.#tasks.enqueue('done');
    }
  }

  /**
   * An alternative to explicitly calling {@link setDone}, `ref()` increments an internal reference
   * count, and {@link unref} decrements it. When the reference count reaches 0, {@link setDone} is
   * automatically called. A TransactionPool is initialized with a reference count of 1.
   *
   * `ref()` should be called before sharing the pool with another component, and only after the
   * pool has been started with {@link run()}. It must not be called on a TransactionPool that is
   * already done (either via {@link unref()} or {@link setDone()}. (Doing so indicates a logical
   * error in the code.)
   *
   * It follows that:
   * * The creator of the TransactionPool is responsible for running it.
   * * The TransactionPool should be ref'ed before being sharing.
   * * The receiver of the TransactionPool is only responsible for unref'ing it.
   *
   * On the other hand, a transaction pool that fails with a runtime error can still be ref'ed;
   * attempts to use the pool will result in the runtime error as expected.
   */
  ref(count = 1) {
    assert(
      this.#db !== undefined && !this.#done,
      `Cannot ref() a TransactionPool that is not running`,
    );
    this.#refCount += count;
  }

  /**
   * Decrements the internal reference count, automatically invoking {@link setDone} when it reaches 0.
   */
  unref(count = 1) {
    assert(count <= this.#refCount);

    this.#refCount -= count;
    if (this.#refCount === 0) {
      this.setDone();
    }
  }

  isRunning(): boolean {
    return this.#db !== undefined && !this.#done && this.#failure === undefined;
  }

  /**
   * Signals all workers to fail their transactions with the given {@link err}.
   */
  fail(err: unknown) {
    if (!this.#failure) {
      this.#failure = ensureError(err); // Fail fast: this is checked in the worker loop.
      if (this.#failure instanceof ControlFlowError) {
        this.#lc.debug?.(this.#failure);
      } else {
        this.#lc.error?.(this.#failure);
      }

      for (let i = 0; i < this.#numWorkers; i++) {
        // Enqueue the Error to terminate any workers waiting for tasks.
        this.#tasks.enqueue(this.#failure);
      }
    }
  }
}

type SynchronizeSnapshotTasks = {
  /**
   * The `init` Task for the TransactionPool from which the snapshot originates.
   * The pool must have Mode.SERIALIZABLE, and will be set to READ ONLY by the
   * `exportSnapshot` init task. If the TransactionPool has multiple workers, the
   * first worker will export a snapshot that the others set.
   */
  exportSnapshot: Task;

  /**
   * The `cleanup` Task for the TransactionPool from which the snapshot
   * originates. This Task will wait for the follower pool to `setSnapshot`
   * to ensure that the snapshot is successfully shared before the originating
   * transaction is closed.
   */
  cleanupExport: Task;

  /**
   * The `init` Task for the TransactionPool in which workers will
   * consequently see the same snapshot as that of the first pool. The pool
   * must have Mode.SERIALIZABLE, and will have the ability to perform writes.
   */
  setSnapshot: Task;

  /** The ID of the shared snapshot. */
  snapshotID: Promise<string>;
};

/**
 * Init Tasks for Postgres snapshot synchronization across transactions.
 *
 * https://www.postgresql.org/docs/9.3/functions-admin.html#:~:text=Snapshot%20Synchronization%20Functions,identical%20content%20in%20the%20database.
 */
export function synchronizedSnapshots(): SynchronizeSnapshotTasks {
  const {
    promise: snapshotExported,
    resolve: exportSnapshot,
    reject: failExport,
  } = resolver<string>();

  const {
    promise: snapshotCaptured,
    resolve: captureSnapshot,
    reject: failCapture,
  } = resolver<unknown>();

  // Set by the first worker to run its initTask, who becomes responsible for
  // exporting the snapshot. TODO: Plumb the workerNum and use that instead.
  let firstWorkerRun = false;

  // Note: Neither init task should `await`, as processing in each pool can proceed
  //       as soon as the statements have been sent to the db. However, the `cleanupExport`
  //       task must `await` the result of `setSnapshot` to ensure that exporting transaction
  //       does not close before the snapshot has been captured.
  return {
    exportSnapshot: tx => {
      if (!firstWorkerRun) {
        firstWorkerRun = true;
        const stmt =
          tx`SELECT pg_export_snapshot() AS snapshot; SET TRANSACTION READ ONLY;`.simple();
        // Intercept the promise to propagate the information to `snapshotExported`.
        stmt.then(result => exportSnapshot(result[0].snapshot), failExport);
        return [stmt]; // Also return the stmt so that it gets awaited (and errors handled).
      }
      return snapshotExported.then(snapshotID => [
        tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`),
        tx`SET TRANSACTION READ ONLY`.simple(),
      ]);
    },

    setSnapshot: tx =>
      snapshotExported.then(snapshotID => {
        const stmt = tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`);
        // Intercept the promise to propagate the information to `cleanupExport`.
        stmt.then(captureSnapshot, failCapture);
        return [stmt];
      }),

    cleanupExport: async () => {
      await snapshotCaptured;
      return [];
    },

    snapshotID: snapshotExported,
  };
}

/**
 * Returns `init` and `cleanup` {@link Task}s for a TransactionPool that ensure its workers
 * share a single view of the database. This is used for View Notifier and View Syncer logic
 * that allows multiple entities to perform parallel reads on the same snapshot of the database.
 */
export function sharedSnapshot(): {
  init: Task;
  cleanup: Task;
  snapshotID: Promise<string>;
} {
  const {
    promise: snapshotExported,
    resolve: exportSnapshot,
    reject: failExport,
  } = resolver<string>();

  // Set by the first worker to run its initTask, who becomes responsible for
  // exporting the snapshot.
  let firstWorkerRun = false;

  // Set when any worker is done, signalling that all non-sentinel Tasks have been
  // dequeued, and thus any subsequently spawned workers should skip their initTask
  // since the snapshot is no longer needed (and soon to become invalid).
  let firstWorkerDone = false;

  return {
    init: (tx, lc) => {
      if (!firstWorkerRun) {
        firstWorkerRun = true;
        const stmt = tx`SELECT pg_export_snapshot() AS snapshot;`.simple();
        // Intercept the promise to propagate the information to `snapshotExported`.
        stmt.then(result => exportSnapshot(result[0].snapshot), failExport);
        return [stmt]; // Also return the stmt so that it gets awaited (and errors handled).
      }
      if (!firstWorkerDone) {
        return snapshotExported.then(snapshotID => [
          tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`),
        ]);
      }
      lc.debug?.('All work is done. No need to set snapshot');
      return [];
    },

    cleanup: () => {
      firstWorkerDone = true;
      return [];
    },

    snapshotID: snapshotExported,
  };
}

/**
 * @returns An `init` Task for importing a snapshot from another transaction.
 */
export function importSnapshot(snapshotID: string): {
  init: Task;
  imported: Promise<void>;
} {
  const {promise: imported, resolve, reject} = resolver<void>();

  return {
    init: tx => {
      const stmt = tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`);
      stmt.then(() => resolve(), reject);
      return [stmt];
    },

    imported,
  };
}

/**
 * A superclass of Errors used for control flow that is needed to handle
 * another Error but does not constitute an error condition itself (e.g.
 * aborting transactions after a previous one fails). Subclassing this Error
 * will result in lowering the log level from `error` to `debug`.
 */
export class ControlFlowError extends Error {
  constructor(cause?: unknown) {
    super();
    this.cause = cause;
  }
}

/**
 * Internal error used to rollback the worker transaction. This is used
 * instead of executing a `ROLLBACK` statement because the postgres.js
 * library will otherwise try to execute an extraneous `COMMIT`, which
 * results in outputting a "no transaction in progress" warning to the
 * database logs.
 *
 * Throwing an exception, on the other hand, executes the postgres.js
 * codepath that calls `ROLLBACK` instead.
 */
class RollbackSignal extends ControlFlowError {
  readonly name = 'RollbackSignal';
  readonly message = 'rolling back transaction';
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const error = new Error();
  error.cause = err;
  return error;
}

// Internal classes for handling read and write tasks in the same queue.
class Statements {
  readonly stmts: readonly Statement[];
  constructor(stmts: readonly Statement[]) {
    this.stmts = stmts;
  }
}

class ReadResult<T> {
  readonly result: T;
  constructor(result: T) {
    this.result = result;
  }
}

function queuedStmt(task: Task): QueuedTask {
  return async (tx, lc) => new Statements(await task(tx, lc));
}

function queuedRead<T>(task: ReadTask<T>): QueuedTask<T> {
  return async (tx, lc) => new ReadResult<T>(await task(tx, lc));
}

type QueuedTask<T = unknown> = (
  tx: PostgresTransaction,
  lc: LogContext,
) => MaybePromise<Statements | ReadResult<T>>;

type TaskTracker<T = unknown> =
  | {
      readonly task: QueuedTask;
      readonly resolver?: undefined;
    }
  | {
      readonly task: QueuedTask<ReadResult<T>>;
      readonly resolver: Resolver<T>;
    };

const IDLE_TIMEOUT_MS = 5_000;

const KEEPALIVE_TIMEOUT_MS = 60_000;

const KEEPALIVE_TASK: Task = tx => [tx`SELECT 1`.simple()];

type TimeoutTask = {
  timeoutMs: number;
  task: Task | 'done';
};

type TimeoutTasks = {
  forInitialWorkers: TimeoutTask;
  forExtraWorkers: TimeoutTask;
};

// Production timeout tasks. Overridden in tests.
export const TIMEOUT_TASKS: TimeoutTasks = {
  forInitialWorkers: {
    timeoutMs: KEEPALIVE_TIMEOUT_MS,
    task: KEEPALIVE_TASK,
  },
  forExtraWorkers: {
    timeoutMs: IDLE_TIMEOUT_MS,
    task: 'done',
  },
};
