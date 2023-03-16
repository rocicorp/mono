import {Resolver, resolver} from '@rocicorp/resolver';
import {AbortError} from './abort-error.js';
import {assert} from 'shared/asserts.js';
import {requestIdle as defaultRequestIdle} from './request-idle.js';
import {sleep} from './sleep.js';

export class ProcessScheduler {
  private readonly _process: () => Promise<void>;
  private readonly _idleTimeoutMs: number;
  private readonly _throttleMs: number;
  private readonly _abortSignal: AbortSignal;
  private readonly _requestIdle: typeof defaultRequestIdle;
  private _scheduledResolver: Resolver<void> | undefined = undefined;
  private _runResolver: Resolver<void> | undefined = undefined;
  private _runPromise = Promise.resolve();
  private _throttlePromise = Promise.resolve();

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
    this._process = process;
    this._idleTimeoutMs = idleTimeoutMs;
    this._throttleMs = throttleMs;
    this._abortSignal = abortSignal;
    this._requestIdle = requestIdle;
    this._abortSignal.addEventListener('abort', () => {
      const abortError = new AbortError('Aborted');
      this._runResolver?.reject(abortError);
      this._scheduledResolver?.reject(abortError);
      this._runResolver = undefined;
      this._scheduledResolver = undefined;
    });
  }

  schedule(): Promise<void> {
    if (this._abortSignal.aborted) {
      return Promise.reject(new AbortError('Aborted'));
    }
    if (this._scheduledResolver) {
      return this._scheduledResolver.promise;
    }
    this._scheduledResolver = resolver();
    void this._scheduleInternal();
    return this._scheduledResolver.promise;
  }

  private async _scheduleInternal(): Promise<void> {
    try {
      await this._runPromise;
      // Prevent errors thrown by process from cancelling scheduled runs.
      // this._runPromise is also awaited below and errors are explicitly
      // propagated to promises returned from schedule.
      // eslint-disable-next-line no-empty
    } catch (e) {}
    await this._throttlePromise;
    if (!this._scheduledResolver) {
      return;
    }
    await this._requestIdle(this._idleTimeoutMs);
    if (!this._scheduledResolver) {
      return;
    }
    this._throttlePromise = throttle(this._throttleMs, this._abortSignal);
    this._runResolver = this._scheduledResolver;
    this._scheduledResolver = undefined;
    try {
      this._runPromise = this._process();
      await this._runPromise;
      this._runResolver?.resolve();
    } catch (e) {
      this._runResolver?.reject(e);
    }
    this._runResolver = undefined;
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
