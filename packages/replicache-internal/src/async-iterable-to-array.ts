export async function asyncIterableToArray<T>(
  it: AsyncIterable<T>,
): Promise<T[]> {
  const arr: T[] = [];
  for await (const v of it) {
    arr.push(v);
  }
  return arr;
}
