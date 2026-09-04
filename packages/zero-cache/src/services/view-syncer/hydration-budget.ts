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

  elapsedMs(): number {
    return this.#now() - this.#startedAt;
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
