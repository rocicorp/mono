import {assert} from '../../../shared/src/asserts.ts';

export type LogThrottleOptions = {
  /**
   * An admitted key is suppressed for this long. A key that is still
   * occurring after the window is admitted again, so a log that is still
   * relevant keeps showing up (e.g. after the log has rotated) without
   * repeating for every occurrence.
   */
  readonly windowMs: number;

  /**
   * The most keys tracked at once. The least recently admitted key is
   * forgotten beyond this, which bounds memory when the key space is
   * unbounded (e.g. dynamically built query shapes).
   */
  readonly maxKeys?: number | undefined;

  /**
   * The most occurrences admitted per window across all keys, which bounds
   * log volume when many distinct keys occur at once.
   */
  readonly maxPerWindow?: number | undefined;

  readonly now?: (() => number) | undefined;
};

type Entry = {
  admittedAt: number;
  suppressed: number;
};

/**
 * Rate limits a log statement per key, for logs that would otherwise be
 * written for every client group, such as the slow hydration of a popular
 * query.
 *
 * Deduplication alone (logging a key once and never again) is not enough:
 * the one line is lost once the log rotates, and the set of logged keys
 * grows without bound. Instead a key is admitted at most once per
 * {@link LogThrottleOptions.windowMs}, and the occurrences suppressed in
 * between are counted and reported with the next admitted one.
 */
export class LogThrottle {
  readonly #windowMs: number;
  readonly #maxKeys: number;
  readonly #maxPerWindow: number;
  readonly #now: () => number;

  // Insertion ordered by admission, so the first entry is the least recently
  // admitted key.
  readonly #entries = new Map<string, Entry>();
  #windowStart = -Infinity;
  #admittedInWindow = 0;

  constructor({
    windowMs,
    maxKeys = 1000,
    maxPerWindow = 100,
    now = performance.now.bind(performance),
  }: LogThrottleOptions) {
    assert(windowMs >= 0, 'windowMs must be nonnegative');
    assert(maxKeys > 0, 'maxKeys must be positive');
    assert(maxPerWindow > 0, 'maxPerWindow must be positive');
    this.#windowMs = windowMs;
    this.#maxKeys = maxKeys;
    this.#maxPerWindow = maxPerWindow;
    this.#now = now;
  }

  /**
   * Records an occurrence of `key`.
   *
   * @returns `undefined` if the occurrence should not be logged, or else the
   *   number of occurrences of `key` suppressed since it was last logged.
   */
  admit(key: string): number | undefined {
    const now = this.#now();
    const entry = this.#entries.get(key);
    if (entry && now - entry.admittedAt < this.#windowMs) {
      entry.suppressed++;
      return undefined;
    }

    if (now - this.#windowStart >= this.#windowMs) {
      this.#windowStart = now;
      this.#admittedInWindow = 0;
    }
    if (this.#admittedInWindow >= this.#maxPerWindow) {
      // Over the global budget. The occurrence is counted against the key so
      // that it is reported once the key is admitted.
      if (entry) {
        entry.suppressed++;
      } else {
        this.#set(key, {admittedAt: -Infinity, suppressed: 1});
      }
      return undefined;
    }

    this.#admittedInWindow++;
    const suppressed = entry?.suppressed ?? 0;
    this.#set(key, {admittedAt: now, suppressed: 0});
    return suppressed;
  }

  #set(key: string, entry: Entry): void {
    // Delete first so that the key moves to the end of the insertion order.
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    if (this.#entries.size > this.#maxKeys) {
      const oldest = this.#entries.keys().next();
      if (!oldest.done) {
        this.#entries.delete(oldest.value);
      }
    }
  }
}
