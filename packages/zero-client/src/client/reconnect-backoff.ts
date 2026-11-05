import type {BackoffBody} from '../../../zero-protocol/src/error.ts';

export const FAST_RECONNECT_ATTEMPTS = 2;
export const FAST_RECONNECT_MAX_MS = 250;
export const STABLE_CONNECTION_MS = 30_000;
export const MAX_RECONNECT_BACKOFF_MS = 30_000;

/** A handshake alone must not replenish a client's fast-reconnect budget. */
export class ReconnectBackoff {
  #connectedAt: number | undefined;
  #receivedPoke = false;
  #fastAttempts = FAST_RECONNECT_ATTEMPTS;
  #recovering = false;
  #slowAttempts = 0;
  readonly #baseDelayMs: number;

  constructor(baseDelayMs: number) {
    this.#baseDelayMs = baseDelayMs;
  }

  get recovering(): boolean {
    return this.#recovering;
  }

  connected(now: number): void {
    this.#connectedAt = now;
    this.#receivedPoke = false;
  }

  receivedPoke(): void {
    if (this.#connectedAt !== undefined) {
      this.#receivedPoke = true;
    }
  }

  disconnected(now: number): void {
    if (
      this.#connectedAt !== undefined &&
      this.#receivedPoke &&
      now - this.#connectedAt >= STABLE_CONNECTION_MS
    ) {
      this.#fastAttempts = FAST_RECONNECT_ATTEMPTS;
      this.#slowAttempts = 0;
      this.#recovering = true;
    }
    this.#connectedAt = undefined;
    this.#receivedPoke = false;
  }

  nextDelay(
    directive?: Pick<BackoffBody, 'minBackoffMs' | 'maxBackoffMs'>,
    overloaded = false,
    random = Math.random,
  ): number {
    // A server can also request a bounded fast handoff before the client's
    // first useful connection. Repeated connect/Rehome cycles share this
    // same allowance, rather than replenishing it at every handshake.
    const shortDirective =
      directive?.maxBackoffMs !== undefined &&
      directive.maxBackoffMs < this.#baseDelayMs;
    const fast =
      !overloaded &&
      this.#fastAttempts > 0 &&
      (this.#recovering || shortDirective);
    let delay: number;
    if (fast) {
      this.#fastAttempts--;
      this.#recovering = true;
      delay = Math.floor(random() * FAST_RECONNECT_MAX_MS);
    } else if (this.#recovering) {
      const cap = Math.min(
        MAX_RECONNECT_BACKOFF_MS,
        this.#baseDelayMs * 2 ** Math.min(this.#slowAttempts++, 4),
      );
      // Equal jitter keeps a lower bound once the fast budget is exhausted.
      delay = Math.floor(cap / 2 + (random() * cap) / 2);
    } else {
      // Preserve initial-connect retry behavior until useful service or an
      // explicit handoff establishes that this is recovery.
      delay = this.#baseDelayMs;
    }

    // Always honor overload directives. A short Rehome/Rebalance maximum is
    // bounded by the fast budget; otherwise a draining loop could bypass the
    // backoff indefinitely. Other maxima remain applicable.
    if (
      directive?.maxBackoffMs !== undefined &&
      (overloaded || fast || !shortDirective)
    ) {
      delay = Math.min(delay, directive.maxBackoffMs);
    }
    if (directive?.minBackoffMs !== undefined) {
      delay = Math.max(delay, directive.minBackoffMs);
    }
    return Math.max(0, delay);
  }
}
