type Primitive = undefined | null | boolean | string | number | symbol | bigint;

/**
 * A {@link Set} that uses a custom value transformation function to convert values
 * to a primitive type that can be used as a {@link Set} value.
 *
 * This allows for using objects as values in a {@link Set} without worrying about
 * reference equality.
 */

export class CustomKeySet<V> implements Set<V> {
  readonly [Symbol.toStringTag] = 'CustomKeySet';
  readonly #toKey: (value: V) => Primitive;
  readonly #map = new Map<Primitive, V>();

  constructor(toKey: (value: V) => Primitive, iterable?: Iterable<V> | null) {
    this.#toKey = toKey;
    if (iterable) {
      for (const value of iterable ?? []) {
        this.#map.set(toKey(value), value);
      }
    }
  }

  add(value: V): this {
    this.#map.set(this.#toKey(value), value);
    return this;
  }

  clear(): void {
    this.#map.clear();
  }

  delete(value: V): boolean {
    return this.#map.delete(this.#toKey(value));
  }

  forEach(
    callbackfn: (value: V, value2: V, set: Set<V>) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    thisArg?: any,
  ): void {
    this.#map.forEach(value => {
      callbackfn.call(thisArg, value, value, this);
    });
  }

  has(value: V): boolean {
    return this.#map.has(this.#toKey(value));
  }

  get size(): number {
    return this.#map.size;
  }

  *entries(): IterableIterator<[V, V]> {
    for (const value of this.#map.values()) {
      yield [value, value];
    }
  }

  keys(): IterableIterator<V> {
    return this.#map.values();
  }

  values(): IterableIterator<V> {
    return this.#map.values();
  }

  [Symbol.iterator](): IterableIterator<V> {
    return this.#map.values();
  }
}
