import {must} from './must.ts';

/**
 * This is a basic ref count implementation that uses a WeakMap to store the
 * reference count for each value.
 */
export class RefCount<T extends WeakKey = WeakKey> {
  readonly #map = new WeakMap<T, number>();

  /**
   * Returns true if the value was added.
   */
  inc(value: T): boolean {
    const rc = this.#map.get(value) ?? 0;
    this.#map.set(value, rc + 1);
    return rc === 0;
  }

  /**
   * Returns true if the value was removed.
   */
  dec(value: T): boolean {
    const rc = must(this.#map.get(value));
    if (rc === 1) {
      return this.#map.delete(value);
    }
    this.#map.set(value, rc - 1);
    return false;
  }
}
