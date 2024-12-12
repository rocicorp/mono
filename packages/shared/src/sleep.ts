import {resolver} from '@rocicorp/resolver';
import {AbortError} from './abort-error.js';

const promiseVoid = Promise.resolve();
const promiseNever = new Promise<void>(() => undefined);

/**
 * Creates a promise that resolves after `ms` milliseconds. Note that if you
 * pass in `0` no `setTimeout` is used and the promise resolves immediately. In
 * other words no macro task is used in that case.
 *
 * Pass in an AbortSignal to clear the timeout.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  const newAbortError = () => new AbortError('Aborted');

  if (signal?.aborted) {
    return Promise.reject(newAbortError());
  }

  if (ms === 0) {
    return promiseVoid;
  }

  return new Promise((resolve, reject) => {
    let handleAbort: () => void;
    if (signal) {
      handleAbort = () => {
        clearTimeout(id);
        reject(newAbortError());
      };
      signal.addEventListener('abort', handleAbort, {once: true});
    }

    const id = setTimeout(() => {
      resolve();
      signal?.removeEventListener('abort', handleAbort);
    }, ms);
  });
}

/**
 * Returns a pair of promises. The first promise resolves after `ms` milliseconds
 * unless the AbortSignal is aborted. The second promise resolves when the AbortSignal
 * is aborted.
 */
export function sleepWithAbort(
  ms: number,
  signal: AbortSignal,
): [ok: Promise<void>, aborted: Promise<void>] {
  if (ms === 0) {
    return [promiseVoid, promiseNever];
  }

  const {promise: abortedPromise, resolve: abortedResolve} = resolver<void>();

  const sleepPromise = new Promise<void>(resolve => {
    const handleAbort = () => {
      clearTimeout(id);
      abortedResolve();
    };

    const id = setTimeout(() => {
      resolve();
      signal.removeEventListener('abort', handleAbort);
    }, ms);

    signal.addEventListener('abort', handleAbort, {once: true});
  });

  return [sleepPromise, abortedPromise];
}
