export function mapValues<T, U>(
  input: Record<string, T>,
  mapper: (value: T, index: number) => U,
): Record<string, U> {
  const output: Record<string, U> = {};
  let i = 0;
  for (const key in input) {
    output[key] = mapper(input[key], i++);
  }
  return output;
}

/**
 * Note: Use {@link mapValues()} if you do not need to change the keys, as it
 *       is more efficient.
 */
export function mapEntries<T, U>(
  input: Record<string, T>,
  mapper: (key: string, val: T, index: number) => [key: string, val: U],
): Record<string, U> {
  const output: Record<string, U> = {};
  let i = 0;
  for (const key in input) {
    const [k, v] = mapper(key, input[key], i++);
    output[k] = v;
  }
  return output;
}

/**
 * Note: Use {@link mapValues()} (preferred) or {@link mapEntries()} if possible,
 *       as they are more efficient.
 */
export function mapAllEntries<T, U>(
  input: Record<string, T>,
  mapper: (entries: [key: string, val: T][]) => [key: string, val: U][],
): Record<string, U> {
  return Object.fromEntries(mapper(Object.entries(input)));
}
