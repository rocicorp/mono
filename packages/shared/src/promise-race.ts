type PromiseRaceResult<T extends Record<string, PromiseLike<unknown>>> = {
  [K in Extract<keyof T, string>]: {
    key: K;
    status: 'fulfilled';
    result: Awaited<T[K]>;
  };
}[Extract<keyof T, string>];

const NO_PROMISES_MESSAGE = 'No promises to race';

const wrapPromise = <K extends string, V>(
  key: K,
  promise: PromiseLike<V>,
): Promise<{key: K; status: 'fulfilled'; result: V}> =>
  Promise.resolve(promise).then(result => ({
    key,
    status: 'fulfilled' as const,
    result,
  }));

/**
 * Race a record of promises and resolve with the first resolved entry.
 *
 * @param promises Record of promises to race.
 * @returns Promise resolving to a discriminated union of key/result pairs.
 * @throws An error if the record is empty or if a promise is rejected.
 */
export async function promiseRace<
  T extends Record<string, PromiseLike<unknown>>,
>(promises: T): Promise<PromiseRaceResult<T> & {}> {
  const keys = Object.keys(promises) as Array<Extract<keyof T, string>>;

  if (keys.length === 0) {
    throw new Error(NO_PROMISES_MESSAGE);
  }

  const wrapped = keys.map(key =>
    wrapPromise(key, promises[key] as PromiseLike<Awaited<T[typeof key]>>),
  );

  return await Promise.race(wrapped);
}
