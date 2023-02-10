import {expect} from '@esm-bundle/chai';
import {arrayCompare} from './array-compare';

test('array compare', () => {
  const t = <T>(a: ArrayLike<T>, b: ArrayLike<T>, expected: number) => {
    expect(arrayCompare(a, b)).to.equal(expected);
    expect(arrayCompare(b, a)).to.equal(-expected);
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
