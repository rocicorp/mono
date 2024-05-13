import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';
import {randInt} from './rand.js';

export class LoggingLock {
  readonly #minThresholdMs: number;

  readonly #lock = new Lock();
  readonly #waiters: string[] = [];
  #holder: string | undefined;

  // By default, logs all timings.
  constructor(loggingMinThresholdMs = 0) {
    this.#minThresholdMs = loggingMinThresholdMs;
  }

  #trackWaiters(lc: LogContext, name: string) {
    this.#waiters.push(name);

    if (this.#waiters.length > 1) {
      // Flush the log if the number of waiters is a multiple of 10.
      const flush = this.#waiters.length % 10 === 0;

      (flush ? lc.info : lc.debug)?.(
        `${name} waiting for ${this.#holder} with ${
          this.#waiters.length - 1
        } other waiter(s): ${this.#waiters}`,
      );
      if (flush) {
        // Note: Do not await. See documentation in withLock().
        void lc.flush();
      }
    }
  }

  async withLock<T>(
    lc: LogContext,
    name: string,
    fn: (lc: LogContext) => MaybePromise<T>,
    flushLogsIfLockHeldForMs = 100,
  ): Promise<T> {
    // Note: It is important that there are no `await`s before the lock
    // acquisition is attempted (i.e. withLock()), as the calling logic relies
    // on lock acquisition happening in chronological order.
    lc = lc.withContext('lockFn', name);
    this.#trackWaiters(lc, name);

    let flushAfterLock = false;
    const t0 = Date.now();

    const result: T = await this.#lock.withLock(async () => {
      const t1 = Date.now();

      const lockHoldID = randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
      this.#waiters.splice(this.#waiters.indexOf(name), 1);
      this.#holder = `${name}#${lockHoldID}`;
      lc = lc.withContext('lockHoldID', lockHoldID);

      const elapsed = t1 - t0;
      if (elapsed >= this.#minThresholdMs) {
        lc
          .withContext('lockStage', 'acquired')
          .withContext('lockTiming', elapsed)
          .debug?.(`${name} acquired lock in ${elapsed} ms`);
      }

      try {
        return await fn(lc);
      } finally {
        const t2 = Date.now();
        const elapsed = t2 - t1;
        if (elapsed >= this.#minThresholdMs) {
          flushAfterLock = elapsed >= flushLogsIfLockHeldForMs;
          const tlc = lc
            .withContext('lockStage', 'held')
            .withContext('lockTiming', elapsed);
          (flushAfterLock ? tlc.info : tlc.debug)?.(
            `${name} held lock for ${elapsed} ms`,
          );
        }
        // Note: Leave the #holder variable set until it is replaced
        // by the next holder. This makes the logging output (when
        // there are multiple waiters) more useful.
      }
    });
    if (flushAfterLock) {
      // Do not await, log flushes can be very slow (10s of seconds).
      // The logical success/failure of withLock does not
      // depend on this calls result.
      void lc.flush();
    }
    return result;
  }
}
