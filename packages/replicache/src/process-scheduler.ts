import {Resolver, resolver} from '@rocicorp/resolver';
import {AbortError} from 'shared/src/abort-error.js';
import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {requestIdle as defaultRequestIdle} from './request-idle.js';

export class ProcessScheduler {
  readonly #process: () => Promise<void>;
  readonly #idleTimeoutMs: number;
  readonly #throttleMs: number;
  readonly #abortSignal: AbortSignal;
  readonly #requestIdle: typeof defaultRequestIdle;
  #scheduledResolver: Resolver<void> | undefined = undefined;
  #runResolver: Resolver<void> | undefined = undefined;
  #runPromise = Promise.resolve();
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
    this.#abortSignal.addEventListener('abort', () => {
      const abortError = new AbortError('Aborted');
      this.#runResolver?.reject(abortError);
      this.#scheduledResolver?.reject(abortError);
      this.#runResolver = undefined;
      this.#scheduledResolver = undefined;
    });
  }

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

  async #scheduleInternal(): Promise<void> {
    try {
      await this.#runPromise;
      // Prevent errors thrown by process from cancelling scheduled runs.
      // this._runPromise is also awaited below and errors are explicitly
      // propagated to promises returned from schedule.
      // eslint-disable-next-line no-empty
    } catch (e) {}
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
