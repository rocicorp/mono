import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {sleep} from 'shared/src/sleep.js';

const DEFAULT_INITIAL_RETRY_DELAY_MS = 100;
const DEFAULT_MAX_RETRY_DELAY_MS = 10000;

export type RetryConfig = {
  initialRetryDelay?: number;
  maxRetryDelay?: number;
};

/**
 * Facilitates lifecycle control with exponential backoff.
 */
export class RunningState {
  readonly #initialRetryDelay: number;
  readonly #maxRetryDelay: number;
  #retryDelay: number;

  #shouldRun = true;
  #stopped = resolver();

  constructor(retryConfig?: RetryConfig) {
    const {
      initialRetryDelay = DEFAULT_INITIAL_RETRY_DELAY_MS,
      maxRetryDelay = DEFAULT_MAX_RETRY_DELAY_MS,
    } = retryConfig ?? {};

    this.#initialRetryDelay = initialRetryDelay;
    this.#maxRetryDelay = maxRetryDelay;
    this.#retryDelay = initialRetryDelay;
  }

  /**
   * Returns `true` until {@link stop()} has been called.
   *
   * This is usually called as part of the service's main loop
   * conditional to determine if the next iteration should execute.
   */
  shouldRun(): boolean {
    return this.#shouldRun;
  }

  /**
   * Called to stop the service. After this is called, {@link shouldRun()}
   * will return `false` and the {@link stopped()} Promise will be resolved.
   */
  stop(): void {
    this.#shouldRun = false;
    this.#stopped.resolve();
  }

  /**
   * Returns a Promise that resolves when {@link stop()} is called.
   * This is used internally to cut off a {@link backoff()} delay, but
   * can also be used explicitly in a `Promise.race(...)` call to stop
   * stop waiting for work.
   */
  stopped(): Promise<void> {
    return this.#stopped.promise;
  }

  /**
   * Call in response to an error or unexpected termination in the main
   * loop of the service. The returned Promise will resolve after an
   * exponential delay, or once {@link stop()} is called.
   */
  backoff(lc?: LogContext): Promise<void> {
    const delay = this.#retryDelay;
    this.#retryDelay = Math.min(delay * 2, this.#maxRetryDelay);

    lc?.info?.(`Retrying in ${delay} ms`);
    return Promise.race([sleep(delay), this.#stopped.promise]);
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
