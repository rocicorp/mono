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

export function getValueAtPath(
  obj: Record<string, unknown>,
  path: string,
  sep: RegExp,
): unknown;
export function getValueAtPath<
  const Path extends string,
  const T extends Record<string, unknown>,
  const Sep extends string,
>(obj: T, path: Path, sep: Sep): ValueAtPath<Path, T, Sep>;
export function getValueAtPath(
  obj: Record<string, unknown>,
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
