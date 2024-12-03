import {must} from 'shared/src/must.js';

export class LRUCache<K, V> {
  readonly #maxSize: number;
  readonly #cache: Map<K, V>;

  constructor(maxSize: number) {
    this.#maxSize = maxSize;
    this.#cache = new Map();
  }

  get(key: K): V | undefined {
    const value = this.#cache.get(key);
    if (value === undefined) {
      return undefined;
    }
    this.#cache.delete(key);
    this.#cache.set(key, value);
    return value;
  }

  set(key: K, value: V): void {
    if (this.#cache.size >= this.#maxSize) {
      const firstKey = must(this.#cache.keys().next().value);
      this.#cache.delete(firstKey);
    }
    this.#cache.set(key, value);
  }
}
