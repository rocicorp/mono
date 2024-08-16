import {describe, expect, test} from 'vitest';
import {liteValues} from './lite.js';

describe('types/lite', () => {
  test('values', () => {
    expect(
      liteValues({
        a: 1,
        b: 'two',
        c: true,
        d: false,
        e: null,
        f: 12313214123432n,
      }),
    ).toEqual([1, 'two', 1, 0, null, 12313214123432n]);
  });
});
