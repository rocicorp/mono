// findLastIndex was added in ES2023

export function findLastIndex<T>(
  array: readonly T[],
  predicate: (value: T, index: number) => unknown,
): number {
  let index = array.length;
  while (index--) {
    if (predicate(array[index], index)) {
      return index;
    }
  }
  return -1;
}
