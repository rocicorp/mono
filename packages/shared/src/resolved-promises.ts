export const promiseTrue = Promise.resolve(true as const);
export const promiseFalse = Promise.resolve(false as const);
export const promiseUndefined = Promise.resolve(undefined);
export const promiseVoid = Promise.resolve();

/**
 * A promise that never resolves.
 */
export const promiseNever = new Promise<never>(() => {});
