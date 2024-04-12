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
