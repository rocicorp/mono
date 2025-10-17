type TupleIndices<T extends readonly unknown[]> = number extends T['length']
  ? number
  : _TupleIndices<T>;

type _TupleIndices<
  T extends readonly unknown[],
  Acc extends readonly unknown[] = [],
> = T extends readonly [unknown, ...infer Rest extends readonly unknown[]]
  ? Acc['length'] | _TupleIndices<Rest, [...Acc, unknown]>
  : never;

/**
 * Race a collection of promises. If the first promise to settle resolves, this
 * resolves with its index/key (preserving tuple/index inference). If the first
 * promise rejects, the rejection is propagated unchanged. Empty collections
 * reject with an explicit error.
 */
export async function promiseRace<
  const T extends readonly PromiseLike<unknown>[],
>(promises: T): Promise<TupleIndices<T>>;
export async function promiseRace<
  T extends Record<string, PromiseLike<unknown>>,
>(promises: T): Promise<keyof T>;
export async function promiseRace(
  promises:
    | readonly PromiseLike<unknown>[]
    | Record<string, PromiseLike<unknown>>,
): Promise<number | string> {
  const entries: Array<Promise<{key: string | number}>> = Array.isArray(
    promises,
  )
    ? (promises as readonly PromiseLike<unknown>[]).map((promise, index) =>
        Promise.resolve(promise).then(() => ({key: index})),
      )
    : (Object.keys(promises) as Array<keyof typeof promises>).map(key =>
        Promise.resolve(promises[key]).then(() => ({key})),
      );

  if (entries.length === 0) {
    return Promise.reject(new Error('No promises to race'));
  }

  const {key} = await Promise.race(entries);

  return key;
}
