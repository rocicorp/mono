import {type Resolver, resolver} from '@rocicorp/resolver';
import {AbortError} from '../../shared/src/abort-error.ts';
import {assert} from '../../shared/src/asserts.ts';
import {sleep} from '../../shared/src/sleep.ts';
import {requestIdle as defaultRequestIdle} from './request-idle.ts';

export class ProcessScheduler {
  readonly #process: () => Promise<void>;
  readonly #idleTimeoutMs: number;
  readonly #throttleMs: number;
  readonly #abortSignal: AbortSignal;
  readonly #requestIdle: typeof defaultRequestIdle;
  #scheduledResolver: Resolver<void> | undefined = undefined;
  #runResolver: Resolver<void> | undefined = undefined;
  // oxlint-disable-next-line no-unused-private-class-members -- False positive, this is used in #run
  #runPromise = Promise.resolve();
  // oxlint-disable-next-line no-unused-private-class-members -- False positive, this is used in #run
  #throttlePromise = Promise.resolve();

  /**
   * Supports scheduling a `process` to be run with certain constraints.
   *  - Process runs are never concurrent.
   *  - Multiple calls to schedule will be fulfilled by a single process
   *    run started after the call to schedule.  A call is never fulfilled by an
   *    already running process run.  This can be thought of as debouncing.
   *  - Process runs are throttled so that the process runs at most once every
   *    `throttleMs`.
   *  - Process runs try to run during an idle period, but will delay at most
   *    `idleTimeoutMs`.
   *  - Scheduled runs which have not completed when `abortSignal` is aborted
   *    will reject with an `AbortError`.
   */
  constructor(
    process: () => Promise<void>,
    idleTimeoutMs: number,
    throttleMs: number,
    abortSignal: AbortSignal,
    requestIdle = defaultRequestIdle,
  ) {
    this.#process = process;
    this.#idleTimeoutMs = idleTimeoutMs;
    this.#throttleMs = throttleMs;
    this.#abortSignal = abortSignal;
    this.#requestIdle = requestIdle;
    this.#abortSignal.addEventListener(
      'abort',
      () => {
        const abortError = new AbortError('Aborted');
        this.#runResolver?.reject(abortError);
        this.#scheduledResolver?.reject(abortError);
        this.#runResolver = undefined;
        this.#scheduledResolver = undefined;
      },
      {once: true},
    );
  }

  /**
   * Schedules the process to run.
   *
   * The returned promise resolves when the process has completed running.
   * If the process throws an error, the returned promise rejects with that error.
   *
   * If `schedule()` is called multiple times while a process is running/scheduled,
   * they will be debounced into a single run.
   */
  schedule(): Promise<void> {
    if (this.#abortSignal.aborted) {
      return Promise.reject(new AbortError('Aborted'));
    }
    if (this.#scheduledResolver) {
      return this.#scheduledResolver.promise;
    }
    this.#scheduledResolver = resolver();
    void this.#scheduleInternal();
    return this.#scheduledResolver.promise;
  }

  /**
   * Runs the process immediately, skipping throttle and idle checks.
   *
   * The returned promise resolves when the process has completed running.
   * If the process throws an error, the returned promise rejects with that error.
   *
   * If there is a scheduled run pending (waiting for idle or throttle), this run
   * will effectively "take over" that scheduled run, and the promise returned
   * by `schedule()` will resolve when this run completes.
   *
   * If there is a process currently running, this run will wait for it to finish
   * before starting, satisfying the non-concurrency constraint.
   */
  async run(): Promise<void> {
    if (this.#abortSignal.aborted) {
      return Promise.reject(new AbortError('Aborted'));
    }

    // "Steal" the scheduled resolver if it exists.
    // This effectively cancels the pending scheduled run (the idle/throttle wait),
    // and fulfills that promise with this run.
    const resolverToResolve = this.#scheduledResolver;
    this.#scheduledResolver = undefined;

    // Wait for any currently running process to finish.
    // We create a new promise that represents "Wait for current, then run me"
    const prevRunPromise = this.#runPromise;

    const runTask = async () => {
      try {
        await prevRunPromise;
      } catch {
        // ignore errors from previous run
      }

      if (this.#abortSignal.aborted) {
        throw new AbortError('Aborted');
      }

      return this.#process();
    };

    const executionPromise = runTask();
    this.#runPromise = executionPromise;

    // Reset throttle promise so future runs respect the throttle relative to this run.
    this.#throttlePromise = throttle(this.#throttleMs, this.#abortSignal);

    try {
      await executionPromise;
      resolverToResolve?.resolve();
    } catch (e) {
      resolverToResolve?.reject(e);
      throw e;
    }
  }

  async #scheduleInternal(): Promise<void> {
    try {
      await this.#runPromise;
      // Prevent errors thrown by process from cancelling scheduled runs.
      // this._runPromise is also awaited below and errors are explicitly
      // propagated to promises returned from schedule.
    } catch {}
    await this.#throttlePromise;
    if (!this.#scheduledResolver) {
      return;
    }
    await this.#requestIdle(this.#idleTimeoutMs);
    if (!this.#scheduledResolver) {
      return;
    }
    this.#throttlePromise = throttle(this.#throttleMs, this.#abortSignal);
    this.#runResolver = this.#scheduledResolver;
    this.#scheduledResolver = undefined;
    try {
      this.#runPromise = this.#process();
      await this.#runPromise;
      this.#runResolver?.resolve();
    } catch (e) {
      this.#runResolver?.reject(e);
    }
    this.#runResolver = undefined;
  }
}

async function throttle(
  timeMs: number,
  abortSignal: AbortSignal,
): Promise<void> {
  try {
    await sleep(timeMs, abortSignal);
  } catch (e) {
    assert(e instanceof AbortError);
  }
}
