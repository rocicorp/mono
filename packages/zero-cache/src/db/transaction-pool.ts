import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {Queue} from 'shared/src/queue.js';

type MaybePromise<T> = Promise<T> | T;

export type Statement = postgres.PendingQuery<
  (postgres.Row & Iterable<postgres.Row>)[]
>;

/**
 * A {@link Task} is logic run from within a transaction in a {@link TransactionPool}.
 * It can return a list of `Statements` (for write transactions), which the transaction
 * will execute asynchronously and await when it receives the 'done' signal, or it can
 * return a `Promise<void>` (typical for read transactions) that is awaited when
 * the task is processed.
 */
export type Task = (
  tx: postgres.TransactionSql,
  lc: LogContext,
) => MaybePromise<Statement[] | void>;

/**
 * A TransactionPool is a pool of one or more {@link postgres.TransactionSql}
 * objects that participate in processing a dynamic queue of tasks.
 *
 * This can be used for serializing a set of tasks that arrive asynchronously
 * to a single transaction (for writing) or performing parallel reads across
 * multiple connections at the same snapshot (e.g. read only snapshot transactions).
 */
export class TransactionPool {
  readonly #lc: LogContext;
  readonly #init: Task | undefined;
  readonly #tasks = new Queue<Task | Error | 'done'>();
  readonly #workers: Promise<unknown>[] = [];

  #numWorkers: number;
  #db: postgres.Sql | undefined; // set when running. stored to allow dynamic pool sizing.

  #done = false;
  #failure: Error | undefined;

  /**
   * @param numWorkers The number of transaction objects to use to process tasks.
   * TODO: Add `maxWorkers` option for dynamic pool sizing.
   * @param init A {@link Task} that is run in each Transaction before it begins
   *             processing general tasks. This can be used to to set the transaction
   *             mode, export/set snapshots, etc.
   */
  constructor(lc: LogContext, numWorkers = 1, init?: Task) {
    this.#lc = lc;
    this.#numWorkers = numWorkers;
    this.#init = init;
  }

  /**
   * Starts the pool of workers to process Tasks with transactions opened from the
   * specified {@link db}.
   *
   * Returns a promise that resolves when all workers have closed their transactions,
   * which happens after they have all received the {@link setDone} signal, or if
   * processing was aborted with {@link fail}.
   */
  async run(db: postgres.Sql): Promise<void> {
    assert(!this.#db, 'already running');
    this.#db = db;
    for (let i = 0; i < this.#numWorkers; i++) {
      this.#addWorker(db);
    }
    await Promise.all(this.#workers);
    this.#lc.debug?.('transaction pool done');
  }

  #addWorker(db: postgres.Sql) {
    const lc = this.#lc.withContext('worker', `#${this.#workers.length + 1}`);
    const worker = async (tx: postgres.TransactionSql) => {
      lc.debug?.('started worker');

      const pending: Promise<unknown>[] = [];

      let task: Task | Error | 'done' =
        this.#init ?? (await this.#tasks.dequeue());

      while (task !== 'done') {
        if (this.#failure || task instanceof Error) {
          await Promise.all(pending); // avoid unhandled rejections
          throw this.#failure ?? task;
        }
        const result = await task(tx, this.#lc);
        if (Array.isArray(result)) {
          // Execute the statements (i.e. send to the db) immediately and add them to
          // `pending` for the final await.
          //
          // Optimization: Fail immediately on rejections to prevent more tasks from
          // queueing up. This can save a lot of time if an initial task fails before
          // many subsequent tasks (e.g. transaction replay detection).
          pending.push(
            ...result.map(stmt => stmt.execute().catch(e => this.fail(e))),
          );
          lc.debug?.(`executed ${result.length} statement(s)`, result);
        }
        // await the next task.
        task = await this.#tasks.dequeue();
      }

      lc.debug?.('worker done');
      return Promise.all(pending);
    };

    this.#workers.push(db.begin(worker));

    // After adding the worker, enqueue a signal if we are in either of the terminal
    // states (both of which prevent more tasks from being enqueued), to ensure that
    // the new worker eventually exits.
    if (this.#done) {
      void this.#tasks.enqueue('done');
    }
    if (this.#failure) {
      void this.#tasks.enqueue(this.#failure);
    }
  }

  process(task: Task) {
    assert(!this.#done, 'already set done');
    if (this.#failure) {
      this.#lc.debug?.('dropping task after failure');
      return;
    }
    // TODO: Add dynamic pool sizing with a maxWorkers parameter.
    void this.#tasks.enqueue(task);
  }

  /**
   * Signals to all workers to end their transaction once all pending tasks have
   * been completed.
   */
  setDone() {
    assert(!this.#done, 'already set done');
    this.#done = true;

    for (let i = 0; i < this.#workers.length; i++) {
      void this.#tasks.enqueue('done');
    }
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

      for (let i = 0; i < this.#workers.length; i++) {
        // Enqueue the Error to terminate any workers waiting for tasks.
        void this.#tasks.enqueue(this.#failure);
      }
    }
  }
}

/**
 * A superclass of Errors used for control flow that is needed to handle
 * another Error but does not constitute an error condition itself (e.g.
 * aborting transactions after a previous one fails). Subclassing this Error
 * will result in lowering the log level from `error` to `debug`.
 */
export class ControlFlowError extends Error {
  constructor(err: unknown) {
    super();
    this.cause = err;
  }
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const error = new Error();
  error.cause = err;
  return error;
}
