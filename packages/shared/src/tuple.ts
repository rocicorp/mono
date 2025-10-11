/**
 * Generic tuple utility types
 */

/**
 * Get the last element type from a tuple type.
 * Works with tuples of length 1, 2, or 3.
 */
export type LastInTuple<T extends readonly unknown[]> = T extends readonly [
  infer L,
]
  ? L
  : T extends readonly [unknown, infer L]
    ? L
    : T extends readonly [unknown, unknown, infer L]
      ? L
      : never;

/**
 * A tuple type that requires at least one element.
 */
export type AtLeastOne<T> = readonly [T, ...T[]];

/**
 * Type guard to ensure an array has at least one element.
 */
export function atLeastOne<T>(arr: readonly T[]): AtLeastOne<T> {
  if (arr.length === 0) {
    throw new Error('Expected at least one element');
  }
  return arr as AtLeastOne<T>;
}
