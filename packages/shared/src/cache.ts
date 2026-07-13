/**
 * Stores values with an expiration time at time of insertion.
 * Does not update the expiration time on retrieval.
 * Values are automatically removed from the cache after the TTL expires.
 * The cache is cleaned up periodically based on the TTL so it does
 * not grow indefinitely. The cleanup interval is started lazily on the
 * first `set()` and is `unref`ed (where supported) so it never keeps
 * the process alive.
 *
 * Call `destroy()` to stop the cleanup interval and release the cached
 * values. `destroy()` is idempotent, and a destroyed cache is inert:
 * `get()` always misses, `set()` is a no-op, and the cleanup interval
 * is never restarted.
 */
export class TimedCache<T> {
  readonly #cache: Map<string, {value: T; expiresAt: number}>;
  readonly #ttlMs: number;
  #intervalHandle: ReturnType<typeof setInterval> | undefined;
  #destroyed = false;

  constructor(ttlMs: number) {
    this.#cache = new Map();
    this.#ttlMs = ttlMs;
  }

  set(key: string, value: T): void {
    if (this.#destroyed) {
      return;
    }
    if (this.#intervalHandle === undefined) {
      this.#intervalHandle = setInterval(
        () => this.#removeExpired(),
        this.#ttlMs * 2,
      );
      // In browsers setInterval returns a number, which has no unref.
      this.#intervalHandle.unref?.();
    }
    this.#cache.set(key, {value, expiresAt: Date.now() + this.#ttlMs});
  }

  get(key: string): T | undefined {
    const entry = this.#cache.get(key);
    if (entry === undefined || entry.expiresAt < Date.now()) {
      this.#cache.delete(key);
      return undefined;
    }
    return entry.value;
  }

  #removeExpired(): void {
    const now = Date.now();
    for (const [key, entry] of this.#cache.entries()) {
      if (entry.expiresAt < now) {
        this.#cache.delete(key);
      }
    }
  }

  destroy(): void {
    this.#destroyed = true;
    if (this.#intervalHandle !== undefined) {
      clearInterval(this.#intervalHandle);
      this.#intervalHandle = undefined;
    }
    this.#cache.clear();
  }
}
