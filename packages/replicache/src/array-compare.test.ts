/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test} from 'vitest';
import {arrayCompare} from './array-compare.ts';

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
