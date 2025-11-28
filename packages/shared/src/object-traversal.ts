type Split<
  S extends string,
  Sep extends string,
> = S extends `${infer Head}${Sep}${infer Tail}`
  ? [Head, ...Split<Tail, Sep>]
  : S extends ''
    ? []
    : [S];

type GetAtPath<T, Parts extends readonly string[]> = Parts extends readonly [
  infer Head extends string,
  ...infer Tail extends readonly string[],
]
  ? Head extends keyof T
    ? GetAtPath<T[Head], Tail>
    : undefined
  : T;

type ValueAtPath<Path extends string, T, Sep extends string> = GetAtPath<
  T,
  Split<Path, Sep>
>;

export function getValueAtPath(obj: object, path: string, sep: RegExp): unknown;
export function getValueAtPath<
  const Path extends string,
  const T extends object,
  const Sep extends string,
>(obj: T, path: Path, sep: Sep): ValueAtPath<Path, T, Sep>;
export function getValueAtPath(
  obj: object,
  path: string,
  sep: string | RegExp,
): unknown {
  const parts = path.split(sep);
  let current: unknown = obj;
  for (const part of parts) {
    if (current && typeof current === 'object' && part in current) {
      current = (current as Record<string, unknown>)[part];
    } else {
      return undefined;
    }
  }
  return current;
}

/**
 * Recursively iterates over all leaf values in a nested object tree.
 * A value is considered a leaf if `isLeaf(value)` returns true,
 * or if it's not a plain object.
 *
 * @param obj - The object to iterate over
 * @param isLeaf - A function that returns true if a value should be yielded as a leaf
 */
export function* iterateLeaves<T>(
  obj: object,
  isLeaf: (value: unknown) => value is T,
): Iterable<T> {
  for (const key of Object.keys(obj)) {
    const value = (obj as Record<string, unknown>)[key];
    if (isLeaf(value)) {
      yield value;
    } else if (value && typeof value === 'object') {
      yield* iterateLeaves(value, isLeaf);
    }
  }
}
