import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';

export class LoggingLock {
  readonly #minThresholdMs: number;

  readonly #lock = new Lock();
  readonly #waiters: string[] = [];
  #holder: string | undefined;

  // By default, logs timings over 0 ms.
  constructor(loggingMinThresholdMs = 1) {
    this.#minThresholdMs = loggingMinThresholdMs;
  }

  async withLock(
    lc: LogContext,
    name: string,
    fn: () => MaybePromise<void>,
    flushLogsIfLockHeldForMs = 100,
  ): Promise<void> {
    this.#waiters.push(name);
    lc = lc.withContext('function', name);

    if (this.#waiters.length > 1) {
      // Flush the log if the number of waiters is a multiple of 5.
      const flush = this.#waiters.length % 5 === 0;

      (flush ? lc.info : lc.debug)?.(
        `${name} waiting for ${this.#holder} with ${
          this.#waiters.length - 1
        } other waiter(s): ${this.#waiters}`,
      );
      if (flush) {
        await lc.flush();
      }
    }

    let flushAfterLock = false;
    const t0 = Date.now();

    await this.#lock.withLock(async () => {
      const t1 = Date.now();

      this.#waiters.splice(this.#waiters.indexOf(name), 1);
      this.#holder = name;
      const elapsed = t1 - t0;
      if (elapsed >= this.#minThresholdMs) {
        lc.withContext('timing', 'lock-acquired').debug?.(
          `${name} acquired lock in ${elapsed} ms`,
        );
      }

      try {
        await fn();
      } finally {
        const t2 = Date.now();
        const elapsed = t2 - t1;
        if (elapsed >= this.#minThresholdMs) {
          flushAfterLock = elapsed >= flushLogsIfLockHeldForMs;
          lc = lc.withContext('timing', 'lock-held');
          (flushAfterLock ? lc.info : lc.debug)?.(
            `${name} held lock for ${elapsed} ms`,
          );
        }
        // Note: Leave the #holder variable set until it is replaced
        // by the next holder. This makes the logging output (when
        // there are multiple waiters) more useful.
      }
    });
    if (flushAfterLock) {
      await lc.flush();
    }
  }
}
