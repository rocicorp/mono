// Force TypeScript to evaluate/flatten a type
type Simplify<T> = {[K in keyof T]: T[K]} & {};

// Helper to check if T is a plain object (not array or function)
type IsPlainObject<T> = T extends object
  ? T extends Function | readonly unknown[]
    ? false
    : true
  : false;

type IsLeaf<T, Leaf> = [T] extends [Leaf] ? true : false;

type MergeValue<A, B, Leaf> =
  IsLeaf<A, Leaf> extends true
    ? B
    : IsLeaf<B, Leaf> extends true
      ? B
      : IsPlainObject<A> extends true
        ? IsPlainObject<B> extends true
          ? DeepMerge<A & {}, B & {}, Leaf>
          : B
        : B;

/**
 * Type-level deep merge of two object types. Properties from B override
 * properties from A. When both A[K] and B[K] are objects (not arrays or
 * functions), they are recursively merged.
 */
export type DeepMerge<A, B, Leaf = never> = Simplify<
  Omit<A, keyof B> & {
    [K in keyof B]: K extends keyof A ? MergeValue<A[K], B[K], Leaf> : B[K];
  }
>;

export function isPlainObject(
  value: unknown,
): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

/**
 * Deep merges two objects. Properties from `b` override properties from `a`.
 * Nested objects are recursively merged.
 *
 * @param a - The base object.
 * @param b - The object to merge into `a`.
 * @param isLeaf - Optional predicate to determine if a value should be treated
 *   as a leaf (not recursed into). Defaults to checking if the value is not a
 *   plain object.
 */
export function deepMerge<
  A extends Record<string, unknown>,
  B extends Record<string, unknown>,
  Leaf = never,
>(
  a: A,
  b: B,
  isLeaf: (value: unknown) => boolean = v => !isPlainObject(v),
): DeepMerge<A, B, Leaf> {
  const result: Record<string, unknown> = {};

  // Copy all keys from a
  for (const key of Object.keys(a)) {
    result[key] = a[key];
  }

  // Merge/override with keys from b
  for (const key of Object.keys(b)) {
    const aVal = a[key];
    const bVal = b[key];

    if (key in a && !isLeaf(aVal) && !isLeaf(bVal)) {
      result[key] = deepMerge<
        Record<string, unknown>,
        Record<string, unknown>,
        Leaf
      >(
        aVal as Record<string, unknown>,
        bVal as Record<string, unknown>,
        isLeaf,
      );
    } else {
      result[key] = bVal;
    }
  }

  return result as DeepMerge<A, B, Leaf>;
}
