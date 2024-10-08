import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../../shared/src/abort-error.js';
import {sleepWithAbort} from '../../../shared/src/sleep.js';

const DEFAULT_INITIAL_RETRY_DELAY_MS = 100;
const DEFAULT_MAX_RETRY_DELAY_MS = 10000;

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
  readonly #stopped: Promise<void>;

  readonly #initialRetryDelay: number;
  readonly #maxRetryDelay: number;
  #retryDelay: number;

  constructor(
    serviceName: string,
    retryConfig?: RetryConfig,
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

    const {promise, resolve} = resolver();
    this.#stopped = promise;
    this.#controller.signal.addEventListener('abort', () => resolve(), {
      once: true,
    });
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
   * Called to stop the service. After this is called, {@link shouldRun()}
   * will return `false` and the {@link stopped()} Promise will be resolved.
   */
  stop(lc: LogContext, err?: unknown): void {
    if (this.shouldRun()) {
      if (err) {
        lc.error?.(`stopping ${this.#serviceName} with error`, err);
      } else {
        lc.info?.(`stopping ${this.#serviceName}`);
      }
      this.#controller.abort();
    }
  }

  /**
   * Returns a Promise that resolves when {@link stop()} is called.
   * This is used internally to cut off a {@link backoff()} delay, but
   * can also be used explicitly in a `Promise.race(...)` call to stop
   * stop waiting for work.
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
  async backoff(lc: LogContext, err?: unknown): Promise<void> {
    const delay = this.#retryDelay;
    this.#retryDelay = Math.min(delay * 2, this.#maxRetryDelay);

    if (err instanceof AbortError) {
      this.stop(lc, err);
    } else if (this.shouldRun()) {
      if (err) {
        lc.error?.(`retrying ${this.#serviceName} in ${delay} ms`, err);
      } else {
        lc.info?.(`retrying ${this.#serviceName} in ${delay} ms`);
      }
      await Promise.race(this.#sleep(delay, this.#controller.signal));
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
