type Primitive = undefined | null | boolean | string | number | symbol | bigint;

/**
 * A {@link Map} that uses a custom key transformation function to convert keys
 * to a primitive type that can be used as a {@link Map} key.
 *
 * This allows for using objects as keys in a {@link Map} without worrying about
 * reference equality.
 */
export class CustomKeyMap<K, V> implements Map<K, V> {
  readonly [Symbol.toStringTag] = 'CustomKeyMap';
  readonly #toKey: (key: K) => Primitive;
  readonly #map = new Map<Primitive, readonly [K, V]>();

  constructor(
    toKey: (key: K) => Primitive,
    iterable?: Iterable<readonly [K, V]> | null,
  ) {
    this.#toKey = toKey;
    if (iterable) {
      for (const [key, value] of iterable ?? []) {
        this.#map.set(toKey(key), [key, value]);
      }
    }
  }

  clear(): void {
    this.#map.clear();
  }

  delete(key: K): boolean {
    return this.#map.delete(this.#toKey(key));
  }

  forEach(
    callbackfn: (value: V, key: K, map: Map<K, V>) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    thisArg?: any,
  ): void {
    for (const [key, value] of this.#map.values()) {
      callbackfn.call(thisArg, value, key, this);
    }
  }

  get(key: K): V | undefined {
    return this.#map.get(this.#toKey(key))?.[1];
  }

  has(key: K): boolean {
    return this.#map.has(this.#toKey(key));
  }

  set(key: K, value: V): this {
    this.#map.set(this.#toKey(key), [key, value]);
    return this;
  }

  get size(): number {
    return this.#map.size;
  }

  *entries(): IterableIterator<[K, V]> {
    for (const entry of this.#map.values()) {
      yield entry.slice(0, 2) as [K, V];
    }
  }

  *keys(): IterableIterator<K> {
    for (const entry of this.#map.values()) {
      yield entry[0];
    }
  }

  *values(): IterableIterator<V> {
    for (const entry of this.#map.values()) {
      yield entry[1];
    }
  }

  [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.entries();
  }
}
