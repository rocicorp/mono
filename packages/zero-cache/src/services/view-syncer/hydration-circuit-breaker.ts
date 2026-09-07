import {assert} from '../../../../shared/src/asserts.ts';
import type {MonotonicClock} from './hydration-budget.ts';

/**
 * How long a tripped query stays rejected before a retry is allowed.
 *
 * A query that blows the timeout is almost always slow because of the shape
 * of its data rather than because of transient load, so retrying it on every
 * reconnect would burn a full timeout each time. The cooldown keeps that cost
 * to at most one timeout per cooldown period per query.
 */
export const DEFAULT_CIRCUIT_BREAKER_OPEN_MS = 5 * 60_000;

/**
 * A per-query circuit breaker for view-syncer hydration.
 *
 * Hydration of a single query is aborted once its processing time exceeds
 * {@link timeoutMs}. Callers check {@link exceeded} at the yield points of a
 * hydration, so a query is only ever aborted between time slices.
 *
 * Once a query has been aborted the breaker is "open" for its transformation
 * hash: {@link isOpen} reports true for {@link openMs}, and callers reject the
 * query up front instead of hydrating it again. After that the breaker is
 * implicitly half-open: the next hydration attempt runs, and either succeeds
 * or trips the breaker again.
 *
 * The breaker is keyed by transformation hash rather than query hash because
 * the transformation (permissions, auth context) determines the pipeline that
 * runs, and a different transformation of the same query may well be fast.
 */
export class HydrationCircuitBreaker {
  readonly timeoutMs: number;
  readonly openMs: number;
  readonly #now: MonotonicClock;
  readonly #openedAt = new Map<string, number>();

  constructor(
    timeoutMs: number,
    openMs: number = DEFAULT_CIRCUIT_BREAKER_OPEN_MS,
    now: MonotonicClock = performance.now.bind(performance),
  ) {
    assert(
      Number.isSafeInteger(timeoutMs) && timeoutMs >= 0,
      'Hydration timeout must be a nonnegative integer',
    );
    assert(openMs >= 0, 'Circuit breaker open duration must be nonnegative');
    this.timeoutMs = timeoutMs;
    this.openMs = openMs;
    this.#now = now;
  }

  /** Whether the breaker does anything at all. */
  get enabled(): boolean {
    return this.timeoutMs > 0;
  }

  /**
   * Whether a hydration that has consumed `elapsedMs` of processing time must
   * be aborted. Always false when the breaker is disabled.
   */
  exceeded(elapsedMs: number): boolean {
    return this.enabled && elapsedMs >= this.timeoutMs;
  }

  /**
   * Records that hydrating `transformationHash` was aborted.
   *
   * Trips are rare, so this is also when expired entries are swept, which
   * keeps the map bounded by the number of hashes tripped within one cooldown
   * rather than by every hash ever tripped.
   */
  trip(transformationHash: string): void {
    const now = this.#now();
    for (const [hash, openedAt] of this.#openedAt) {
      if (now - openedAt >= this.openMs) {
        this.#openedAt.delete(hash);
      }
    }
    this.#openedAt.set(transformationHash, now);
  }

  /** Whether `transformationHash` is currently rejected. */
  isOpen(transformationHash: string): boolean {
    const openedAt = this.#openedAt.get(transformationHash);
    if (openedAt === undefined) {
      return false;
    }
    if (this.#now() - openedAt >= this.openMs) {
      this.#openedAt.delete(transformationHash);
      return false;
    }
    return true;
  }
}
