import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {orTimeout, orTimeoutWith} from './timeout.js';

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
