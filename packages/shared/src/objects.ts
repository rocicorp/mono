/**
 * Sets a single property on `target`, guarding against prototype pollution: a
 * `__proto__` key sets a literal own property (via `Object.defineProperty`)
 * instead of invoking the inherited setter that would mutate the target's
 * prototype. `__proto__` is the only key with this hazard (e.g. `constructor`
 * assigns a normal own property) and a symbol key can never trigger it, so
 * every other key is assigned directly.
 *
 * Use this instead of `target[key] = value` when `key` may come from untrusted
 * data. See {@link safeAssign} to copy an entire source object.
 */
export function safeSet<T extends object>(
  target: T,
  key: PropertyKey,
  value: unknown,
): T {
  if (key === '__proto__') {
    Object.defineProperty(target, key, {
      value,
      writable: true,
      enumerable: true,
      configurable: true,
    });
  } else {
    (target as Record<PropertyKey, unknown>)[key] = value;
  }
  return target;
}

/**
 * Behaves like {@link Object.assign}, but guards against prototype pollution by
 * setting each property via {@link safeSet}, so a `__proto__` source key sets a
 * literal own property instead of mutating the target's prototype.
 *
 * Like `Object.assign`, this copies own enumerable properties (both string- and
 * symbol-keyed).
 */
export function safeAssign<T extends object, U>(target: T, source: U): T & U;
export function safeAssign<T extends object, U, V>(
  target: T,
  source1: U,
  source2: V,
): T & U & V;
export function safeAssign<T extends object>(
  target: T,
  ...sources: object[]
): T;
export function safeAssign(target: object, ...sources: object[]): object {
  for (const source of sources) {
    for (const key of Reflect.ownKeys(source)) {
      if (Object.prototype.propertyIsEnumerable.call(source, key)) {
        safeSet(target, key, (source as Record<PropertyKey, unknown>)[key]);
      }
    }
  }
  return target;
}

export function mapValues<T extends Record<string, unknown>, U>(
  input: T,
  mapper: (value: T[keyof T]) => U,
): {[K in keyof T]: U} {
  return mapEntries(input, (k, v) => [k, mapper(v as T[keyof T])]) as {
    [K in keyof T]: U;
  };
}

export function mapEntries<T, U>(
  input: Record<string, T>,
  mapper: (key: string, val: T) => [key: string, val: U],
): Record<string, U> {
  // Direct assignment is faster than Object.fromEntries()
  // https://github.com/rocicorp/mono/pull/3927#issuecomment-2706059475
  const output: Record<string, U> = {};

  // In chrome Object.entries is faster than for-in (13x) or Object.keys (15x)
  // https://gist.github.com/arv/1b4e113724f6a14e2d4742bcc760d1fa
  for (const entry of Object.entries(input)) {
    const mapped = mapper(entry[0], entry[1]);
    safeSet(output, mapped[0], mapped[1]);
  }
  return output;
}

export function mapAllEntries<T, U>(
  input: Record<string, T>,
  mapper: (entries: [key: string, val: T][]) => [key: string, val: U][],
): Record<string, U> {
  // Direct assignment is faster than Object.fromEntries()
  // https://github.com/rocicorp/mono/pull/3927#issuecomment-2706059475
  const output: Record<string, U> = {};
  for (const mapped of mapper(Object.entries(input))) {
    safeSet(output, mapped[0], mapped[1]);
  }
  return output;
}
