const missing = Symbol();

/**
 * A proxy handler that lazily transforms and caches record values.
 *
 * When a property is accessed, the handler transforms the source value using
 * the provided `toValue` function and caches the result. Subsequent accesses
 * return the cached value without re-transforming.
 *
 * @template Source - The type of values in the source record
 * @template Dest - The type of transformed values
 */
class RecordProxyHandler<Source, Dest>
  implements ProxyHandler<Record<string, Source>>
{
  readonly #toValue: (source: Source, prop: string) => Dest;
  readonly #onMissing: ((prop: string) => void) | undefined;
  readonly #cache: Map<string, Dest | typeof missing> = new Map();

  /**
   * @param toValue - Function to transform source values to destination values
   * @param onMissing - Optional function called when accessing a non-existent
   *   property. Can throw an error if desired.
   */
  constructor(
    toValue: (source: Source, prop: string) => Dest,
    onMissing?: (prop: string) => void,
  ) {
    this.#toValue = toValue;
    this.#onMissing = onMissing;
  }

  #getOwnValue(
    target: Record<string, Source>,
    prop: string,
  ): Dest | typeof missing {
    const v = this.#cache.get(prop);
    if (v !== undefined) {
      return v;
    }

    if (Object.hasOwn(target, prop)) {
      const value = this.#toValue(target[prop], prop);
      this.#cache.set(prop, value);
      return value;
    }
    return missing;
  }

  get(target: Record<string, Source>, prop: string | symbol) {
    if (typeof prop !== 'string') {
      return undefined;
    }
    // Only transform own properties; return inherited properties as-is
    const ownValue = this.#getOwnValue(target, prop);
    if (ownValue !== missing) {
      return ownValue;
    }
    this.#onMissing?.(prop);

    // Inherited property - return without transformation
    return target[prop];
  }

  getOwnPropertyDescriptor(
    target: Record<string, Source>,
    p: string | symbol,
  ): PropertyDescriptor | undefined {
    if (typeof p !== 'string') {
      return undefined;
    }

    const value = this.#getOwnValue(target, p);
    if (value === missing) {
      return undefined;
    }
    const desc = Reflect.getOwnPropertyDescriptor(target, p);
    return {...desc, value};
  }
}

/**
 * Creates a proxy that lazily transforms and caches record values.
 *
 * @template Source - The type of values in the source record
 * @template Dest - The type of transformed values
 * @param target - The source record to proxy
 * @param toValue - Function to transform source values to destination values
 * @param onMissing - Optional function called when accessing a non-existent
 *   property. Can throw an error if desired.
 * @returns A proxy that presents transformed values
 */
export function recordProxy<Source, Dest>(
  target: Record<string, Source>,
  toValue: (source: Source, prop: string) => Dest,
  onMissing?: (prop: string) => void,
): Record<string, Dest> {
  return new Proxy(
    target,
    new RecordProxyHandler(toValue, onMissing),
  ) as unknown as Record<string, Dest>;
}
