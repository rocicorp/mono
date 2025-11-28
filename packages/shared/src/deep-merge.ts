// Force TypeScript to evaluate/flatten a type
type Simplify<T> = {[K in keyof T]: T[K]} & {};

// Helper to check if T is a plain object (not array or function)
type IsPlainObject<T> = T extends object
  ? T extends Function | readonly unknown[]
    ? false
    : true
  : false;

// Merge when both are plain objects, otherwise B wins
type MergeValue<A, B> =
  IsPlainObject<A> extends true
    ? IsPlainObject<B> extends true
      ? DeepMerge<A & {}, B & {}>
      : B
    : B;

/**
 * Type-level deep merge of two object types. Properties from B override
 * properties from A. When both A[K] and B[K] are objects (not arrays or
 * functions), they are recursively merged.
 */
export type DeepMerge<A, B> = Simplify<
  Omit<A, keyof B> & {
    [K in keyof B]: K extends keyof A ? MergeValue<A[K], B[K]> : B[K];
  }
>;

function isPlainObject(value: unknown): value is Record<string, unknown> {
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
>(
  a: A,
  b: B,
  isLeaf: (value: unknown) => boolean = v => !isPlainObject(v),
): DeepMerge<A, B> {
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
      result[key] = deepMerge(
        aVal as Record<string, unknown>,
        bVal as Record<string, unknown>,
        isLeaf,
      );
    } else {
      result[key] = bVal;
    }
  }

  return result as DeepMerge<A, B>;
}
