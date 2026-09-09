import {assert} from '../../../../shared/src/asserts.ts';

export type MonotonicClock = () => number;

/**
 * A soft wall-clock budget for one view-syncer hydration pass.
 *
 * Callers check the budget only at query boundaries. Consequently, a query
 * that starts before the limit is reached is always allowed to finish.
 *
 * The budget measures hydration work. Intervals that are not hydration --
 * notably the remote custom-query transform round trips -- are discounted via
 * {@link excluding}, so that endpoint latency cannot spend a budget that only
 * evicting queries could recover.
 */
export class HydrationBudget {
  readonly limitMs: number;
  readonly #now: MonotonicClock;
  readonly #startedAt: number;
  #excludedMs = 0;
  #exhaustedAtMs: number | undefined;

  constructor(
    limitMs: number,
    now: MonotonicClock = performance.now.bind(performance),
  ) {
    assert(
      Number.isSafeInteger(limitMs) && limitMs >= 0,
      'Hydration budget must be a nonnegative integer',
    );
    this.limitMs = limitMs;
    this.#now = now;
    this.#startedAt = now();
  }

  /**
   * Runs `fn`, discounting the time it takes as non-hydration work. The
   * interval is measured with this budget's own clock, so an injected clock
   * governs both the elapsed time and what is excluded from it.
   */
  async excluding<T>(fn: () => Promise<T>): Promise<T> {
    const start = this.#now();
    try {
      return await fn();
    } finally {
      this.#excludedMs += this.#now() - start;
    }
  }

  elapsedMs(): number {
    return this.#now() - this.#startedAt - this.#excludedMs;
  }

  /**
   * Whether the budget is spent. A disabled budget (`limitMs === 0`) is never
   * exhausted. The elapsed time at the first exhausted check is retained in
   * {@link exhaustedAtMs} so that reporting does not re-read the clock.
   */
  exhausted(): boolean {
    if (this.limitMs === 0) {
      return false;
    }
    const elapsed = this.elapsedMs();
    if (elapsed < this.limitMs) {
      return false;
    }
    this.#exhaustedAtMs ??= elapsed;
    return true;
  }

  /** The elapsed time at which {@link exhausted} first returned true. */
  get exhaustedAtMs(): number | undefined {
    return this.#exhaustedAtMs;
  }
}
