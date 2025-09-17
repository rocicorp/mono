/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {expect, test} from 'vitest';
import {asyncIterableToArray} from './async-iterable-to-array.ts';
import {filterAsyncIterable} from './filter-async-iterable.ts';
import {makeAsyncIterable} from './make-async-iterable.ts';

test('filterAsyncIterable', async () => {
  const t = async <V>(
    elements: Iterable<V>,
    predicate: (v: V) => boolean,
    expected: V[],
  ) => {
    const iter = makeAsyncIterable(elements);
    const filtered = filterAsyncIterable(iter, predicate);
    expect(await asyncIterableToArray(filtered)).to.deep.equal(expected);
  };

  await t([1, 2, 3], () => false, []);
  await t([1, 2, 3], () => true, [1, 2, 3]);
  await t([1, 2, 3], v => v % 2 === 0, [2]);
  await t([1, 2, 3], v => v % 2 === 1, [1, 3]);
});
