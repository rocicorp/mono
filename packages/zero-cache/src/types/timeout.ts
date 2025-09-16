/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {sleepWithAbort} from '../../../shared/src/sleep.ts';

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
