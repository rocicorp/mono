export function mapValues<T extends Record<string, unknown>, U>(
  input: T,
  mapper: (value: T[keyof T], key: keyof T & string) => U,
): {[K in keyof T]: U} {
  const output = {} as {[K in keyof T]: U};

  for (const entry of Object.entries(input) as [
    keyof T & string,
    T[keyof T],
  ][]) {
    assignProperty(output, entry[0], mapper(entry[1], entry[0]));
  }

  return output;
}

/**
 * Safe function of `[[Set]]`/assign that prevents invoking the legacy
 * `__proto__` setter. This is used to avoid prototype pollution attacks.
 */
export function assignProperty<K extends string | number | symbol, T>(
  object: Record<K, T>,
  key: K,
  value: T,
): void {
  if (key === '__proto__') {
    // Shadow the `__proto__` property on the object to prevent prototype
    // pollution.
    Object.defineProperty(object, key, {
      value,
      writable: true,
      enumerable: true,
      configurable: true,
    });
    // fall through to the normal assignment to ensure that potentional Proxy
    // traps are invoked.
  }

  object[key] = value;
}

/**
 * Reads an own property of `object`, or `undefined` when `object` has no own
 * property named `key`. A plain `object[key]` also finds inherited
 * properties, so `object['__proto__']` is `Object.prototype` on an object
 * with no such property of its own.
 */
export function getOwnProperty<T>(
  object: Readonly<Record<string, T>>,
  key: string,
): T | undefined {
  return Object.hasOwn(object, key) ? object[key] : undefined;
}

export function mapEntries<T, U>(
  input: Record<string, T>,
  mapper: (key: string, val: T) => [key: string, val: U],
): Record<string, U> {
  const output: Record<string, U> = {};

  for (const entry of Object.entries(input)) {
    const mapped = mapper(entry[0], entry[1]);
    assignProperty(output, mapped[0], mapped[1]);
  }

  return output;
}

export function mapAllEntries<T, U>(
  input: Record<string, T>,
  mapper: (entries: [key: string, val: T][]) => [key: string, val: U][],
): Record<string, U> {
  const output: Record<string, U> = {};

  for (const [key, val] of mapper(Object.entries(input))) {
    assignProperty(output, key, val);
  }

  return output;
}
