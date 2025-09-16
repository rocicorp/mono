/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {orTimeout, orTimeoutWith} from './timeout.ts';

describe('timeout', () => {
  test('resolved', async () => {
    const {promise, resolve} = resolver<string>();
    resolve('foo');

    expect(await orTimeout(promise, 1)).toBe('foo');
  });

  test('times out', async () => {
    const {promise} = resolver<string>();

    expect(await orTimeout(promise, 1)).toBe('timed-out');
  });

  test('times out with value', async () => {
    const {promise} = resolver<string>();

    expect(await orTimeoutWith(promise, 1, 123.456)).toBe(123.456);
  });
});
