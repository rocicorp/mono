import {sleepWithAbort} from '../../../shared/src/sleep.js';

/**
 * Resolves to the the string `"timed-out"` if `timeoutMs` elapses before
 * the specified `promise` resolves.
 */
export function orTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<T | 'timed-out'> {
  return orTimeoutWith(promise, timeoutMs, 'timed-out');
}

/**
 * Resolves to the specified `timeoutValue` if `timeoutMs` elapses before
 * the specified `promise` resolves.
 */
export async function orTimeoutWith<T, U>(
  promise: Promise<T>,
  timeoutMs: number,
  timeoutValue: U,
): Promise<T | U> {
  const ac = new AbortController();
  const [timeout] = sleepWithAbort(timeoutMs, ac.signal);
  try {
    return await Promise.race([promise, timeout.then(() => timeoutValue)]);
  } finally {
    ac.abort();
  }
}
