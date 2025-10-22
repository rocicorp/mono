import {expect, test} from 'vitest';
import {arrayCompare} from './array-compare.ts';

test('array compare', () => {
  const t = <T>(a: ArrayLike<T>, b: ArrayLike<T>, expected: number) => {
    expect(arrayCompare(a, b)).toBe(expected);
    // -expected || 0 converts -0 to 0 to avoid Object.is(-0, 0) === false
    expect(arrayCompare(b, a)).toBe(-expected || 0);
  };

  t([], [], 0);
  t([1], [1], 0);
  t([1], [2], -1);
  t([1, 2], [1, 2], 0);
  t([1, 2], [1, 3], -1);
  t([1, 2], [2, 1], -1);
  t([1, 2, 3], [1, 2, 3], 0);
  t([1, 2, 3], [2, 1, 3], -1);
  t([1, 2, 3], [2, 3, 1], -1);
  t([1, 2, 3], [3, 1, 2], -1);
  t([1, 2, 3], [3, 2, 1], -1);

  t([], [1], -1);
  t([1], [1, 2], -1);
  t([2], [1, 2], 1);
});
