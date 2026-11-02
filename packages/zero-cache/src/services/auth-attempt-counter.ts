/**
 * Counts failed password attempts and blocks further attempts after a few
 * failures, for a period that doubles with every failure, up to a cap.
 *
 * A caller that finds the counter blocked must not check the password at all:
 * it records the attempt as another failure, which extends the block, so that
 * a correct password submitted during the block is refused too and the block
 * cannot be ended by guessing right. The counter starts over after a quiet
 * period without failures, or when the caller resets it on success.
 *
 * A blocked attempt is refused immediately rather than delayed, so that the
 * caller never has to wait while holding a lock or an open request.
 */

const BASE_BLOCK_MS = 1_000;
const MAX_BLOCK_MS = 60_000;
/** A counter with no failures for this long starts over. */
const RESET_AFTER_MS = 15 * 60_000;

export class AuthAttemptCounter {
  readonly #freeAttempts: number;
  readonly #now: () => number;
  #failures = 0;
  #lastFailure = 0;
  #blockedUntil = 0;

  /**
   * @param freeAttempts Failures allowed before the counter blocks.
   */
  constructor(freeAttempts: number, now: () => number = () => Date.now()) {
    this.#freeAttempts = freeAttempts;
    this.#now = now;
  }

  /**
   * Milliseconds until attempts are allowed again, or 0 when they are
   * allowed now.
   */
  blockedFor(): number {
    return Math.max(0, this.#blockedUntil - this.#now());
  }

  /**
   * Records a failed (or blocked) attempt. Returns the number of failures
   * since the counter last started over.
   */
  recordFailure(): number {
    const now = this.#now();
    if (now - this.#lastFailure > RESET_AFTER_MS) {
      this.#failures = 0;
    }
    this.#failures++;
    this.#lastFailure = now;
    if (this.#failures >= this.#freeAttempts) {
      this.#blockedUntil =
        now +
        Math.min(
          BASE_BLOCK_MS * 2 ** (this.#failures - this.#freeAttempts),
          MAX_BLOCK_MS,
        );
    }
    return this.#failures;
  }

  /** Forgets all failures, as after a successful attempt. */
  reset(): void {
    this.#failures = 0;
    this.#lastFailure = 0;
    this.#blockedUntil = 0;
  }
}
