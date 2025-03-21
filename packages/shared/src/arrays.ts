import {assert} from './asserts.ts';

/**
 * Returns `arr` as is if none of the elements are `undefined`.
 * Otherwise returns a new array with only defined elements in `arr`.
 */
export function defined<T>(arr: (T | undefined)[]): T[] {
  // avoid an array copy if possible
  let i = arr.findIndex(x => x === undefined);
  if (i < 0) {
    return arr as T[];
  }
  const defined: T[] = arr.slice(0, i) as T[];
  for (i++; i < arr.length; i++) {
    const x = arr[i];
    if (x !== undefined) {
      defined.push(x);
    }
  }
  return defined;
}

export function areEqual<T>(arr1: readonly T[], arr2: readonly T[]): boolean {
  return arr1.length === arr2.length && arr1.every((e, i) => e === arr2[i]);
}

export function zip<T1, T2>(a1: readonly T1[], a2: readonly T2[]): [T1, T2][] {
  assert(a1.length === a2.length);
  const result: [T1, T2][] = [];
  for (let i = 0; i < a1.length; i++) {
    result.push([a1[i], a2[i]]);
  }
  return result;
}

export function groupBy<T, K>(
  arr: readonly T[],
  keyFn: (el: T) => K,
): Map<K, T[]> {
  const groups = new Map<K, T[]>();
  for (const el of arr) {
    const key = keyFn(el);
    let group = groups.get(key);
    if (group === undefined) {
      group = [];
      groups.set(key, group);
    }
    group.push(el);
  }
  return groups;
}
