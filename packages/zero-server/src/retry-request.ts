/**
 * The most times a mutator runs for one mutation within one push when it
 * keeps calling `tx.retry()`: the first run and four re-runs. A retry
 * requested on the last run is recorded as the mutation's error.
 */
export const MAX_MUTATOR_ATTEMPTS = 5;

/**
 * Thrown by `tx.retry()`. `transact` catches it after the run's transaction
 * has rolled back and re-runs the mutator in a fresh one, after `delayMs`.
 */
export class RetryRequest extends Error {
  name = 'RetryRequest';
  readonly delayMs: number | undefined;

  constructor(delayMs: number | undefined) {
    super('Mutator requested a retry');
    this.delayMs = delayMs;
  }
}
