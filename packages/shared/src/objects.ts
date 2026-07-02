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
