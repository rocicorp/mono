import {AbortError} from './abort-error.js';

/**
 * Creates a promise that resolves after `ms` milliseconds. Note that if you
 * pass in `0` no `setTimeout` is used and the promise resolves immediately. In
 * other words no macro task is used in that case.
 *
 * Pass in an AbortSignal to clear the timeout.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms === 0) {
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    const id = setTimeout(() => {
      resolve();
    }, ms);

    if (signal) {
      signal.addEventListener('abort', () => {
        if (id) {
          clearTimeout(id);
        }
        reject(new AbortError('Aborted'));
      });
    }
  });
}
