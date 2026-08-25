import {resolver} from '@rocicorp/resolver';

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

/**
 * A memory-safe way to race a short-lived `Promise` with one or more
 * long-leved `AbortSignal`s. This works around the unsafe practice
 * of racing a short-lived promise (e.g. in a loop) with a long-lived one,
 * which results in accumulating memory via `then` callbacks on the latter:
 *
 * https://github.com/nodejs/node/issues/17469
 *
 * The safe way to race a short-lived, expected-to-complete Promise against a
 * long-lived termination signal is to control the latter via an
 * {@link AbortController} and race the former against the signal of the
 * latter.
 */
export async function promiseOrAbort<T>(
  shortLived: Promise<T>,
  ...longLived: AbortSignal[]
): Promise<T> {
  const promises: Promise<T>[] = [];
  const cleanup: (() => void)[] = [];
  for (const signal of longLived) {
    if (signal.aborted) {
      promises.push(Promise.reject(signal.reason));
    } else {
      const {promise, reject} = resolver<T>();
      const handler = () => reject(signal.reason);
      signal.addEventListener('abort', handler);
      cleanup.push(() => signal.removeEventListener('abort', handler));
      promises.push(promise);
    }
  }

  try {
    return await Promise.race([shortLived, ...promises]);
  } finally {
    cleanup.forEach(fn => fn());
  }
}
