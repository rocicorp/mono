import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../../shared/src/abort-error.ts';
import {sleepWithAbort} from '../../../shared/src/sleep.ts';

const DEFAULT_INITIAL_RETRY_DELAY_MS = 25;
export const DEFAULT_MAX_RETRY_DELAY_MS = 10000;

export type RetryConfig = {
  initialRetryDelay?: number;
  maxRetryDelay?: number;
};

export interface Cancelable {
  cancel(): void;
}

export type UnregisterFn = () => void;

/**
 * Facilitates lifecycle control with exponential backoff.
 */
export class RunningState {
  readonly #serviceName: string;
  readonly #controller: AbortController;
  readonly #sleep: typeof sleepWithAbort;
  readonly #setTimeout: typeof setTimeout;
  readonly #stopped: Promise<void>;

  readonly #initialRetryDelay: number;
  readonly #maxRetryDelay: number;
  readonly #pendingTimeouts = new Set<NodeJS.Timeout>();
  #retryDelay: number;

  constructor(
    serviceName: string,
    retryConfig?: RetryConfig,
    setTimeoutFn = setTimeout,
    sleeper = sleepWithAbort,
  ) {
    const {
      initialRetryDelay = DEFAULT_INITIAL_RETRY_DELAY_MS,
      maxRetryDelay = DEFAULT_MAX_RETRY_DELAY_MS,
    } = retryConfig ?? {};

    this.#serviceName = serviceName;
    this.#initialRetryDelay = initialRetryDelay;
    this.#maxRetryDelay = maxRetryDelay;
    this.#retryDelay = initialRetryDelay;

    this.#controller = new AbortController();
    this.#sleep = sleeper;
    this.#setTimeout = setTimeoutFn;

    const {promise, resolve} = resolver();
    this.#stopped = promise;
    this.#controller.signal.addEventListener(
      'abort',
      () => {
        resolve();
        for (const timeout of this.#pendingTimeouts) {
          clearTimeout(timeout);
        }
        this.#pendingTimeouts.clear();
      },
      {once: true},
    );
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  /**
   * Returns `true` until {@link stop()} has been called.
   *
   * This is usually called as part of the service's main loop
   * conditional to determine if the next iteration should execute.
   */
  shouldRun(): boolean {
    return !this.#controller.signal.aborted;
  }

  /**
   * Registers a Cancelable object to be invoked when {@link stop()} is called.
   * Returns a method to unregister the object.
   */
  cancelOnStop(c: Cancelable): UnregisterFn {
    const onStop = () => c.cancel();
    this.#controller.signal.addEventListener('abort', onStop, {once: true});
    return () => this.#controller.signal.removeEventListener('abort', onStop);
  }

  /**
   * Sets a Timeout that is automatically cancelled if the service is cancelled.
   */
  setTimeout<TArgs extends unknown[]>(
    fn: (...args: TArgs) => void,
    timeoutMs: number,
    ...args: TArgs
  ) {
    const timeout = this.#setTimeout(() => {
      clearTimeout(timeout);
      this.#pendingTimeouts.delete(timeout);
      return fn(...args);
    }, timeoutMs);

    this.#pendingTimeouts.add(timeout);
  }

  /**
   * Returns a promise that resolves after `ms` milliseconds or when
   * the service is stopped.
   */
  async sleep(ms: number): Promise<void> {
    await Promise.race(this.#sleep(ms, this.#controller.signal));
  }

  /**
   * Called to stop the service. After this is called, {@link shouldRun()}
   * will return `false` and the {@link stopped()} Promise will be resolved.
   */
  stop(lc: LogContext, err?: unknown): void {
    if (this.shouldRun()) {
      const log = !err || err instanceof AbortError ? 'info' : 'error';
      lc[log]?.(`stopping ${this.#serviceName}`, err ?? '');
      this.#controller.abort();
    }
  }

  /**
   * Returns a Promise that resolves when {@link stop()} is called.
   */
  stopped(): Promise<void> {
    return this.#stopped;
  }

  /**
   * Call in response to an error or unexpected termination in the main
   * loop of the service. The returned Promise will resolve after an
   * exponential delay, or once {@link stop()} is called.
   *
   * If the supplied `err` is an `AbortError`, the service will shut down.
   */
  async backoff(lc: LogContext, err: unknown): Promise<void> {
    const delay = this.#retryDelay;
    this.#retryDelay = Math.min(delay * 2, this.#maxRetryDelay);

    if (err instanceof AbortError || err instanceof UnrecoverableError) {
      this.stop(lc, err);
    } else if (this.shouldRun()) {
      // Use delay-based log level: higher delay means more retries
      const log: 'info' | 'warn' | 'error' =
        delay < 1000 ? 'info' : delay < 6500 ? 'warn' : 'error';

      lc[log]?.(`retrying ${this.#serviceName} in ${delay} ms`, err);
      await this.sleep(delay);
    }
  }

  /**
   * When using {@link backoff()}, this method should be called when the
   * implementation receives a healthy signal (e.g. a successful
   * response). This resets the delay used in {@link backoff()}.
   */
  resetBackoff() {
    this.#retryDelay = this.#initialRetryDelay;
  }
}

/**
 * Superclass for Errors that should bypass exponential backoff
 * and immediately shut down the server.
 */
export class UnrecoverableError extends Error {
  readonly name = 'UnrecoverableError';
}
