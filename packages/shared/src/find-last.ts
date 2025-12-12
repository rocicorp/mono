// findLast was added in ES2023

export function findLast<T>(
  array: readonly T[],
  predicate: (value: T, index: number) => unknown,
): T | undefined {
  let index = array.length;
  while (index--) {
    if (predicate(array[index], index)) {
      return array[index];
    }
  }
  return undefined;
}
