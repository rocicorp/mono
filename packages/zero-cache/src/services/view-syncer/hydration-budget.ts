import {assert} from '../../../../shared/src/asserts.ts';

export type MonotonicClock = () => number;

/**
 * A soft wall-clock budget for one view-syncer hydration pass.
 *
 * Callers check the budget only at query boundaries. Consequently, a query
 * that starts before the limit is reached is always allowed to finish.
 */
export class HydrationBudget {
  readonly limitMs: number;
  readonly #now: MonotonicClock;
  readonly #startedAt: number;

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

  elapsedMs(): number {
    return this.#now() - this.#startedAt;
  }

  exhausted(): boolean {
    return this.limitMs !== 0 && this.elapsedMs() >= this.limitMs;
  }
}
