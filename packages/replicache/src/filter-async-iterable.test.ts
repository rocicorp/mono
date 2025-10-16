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
    expect(await asyncIterableToArray(filtered)).toEqual(expected);
  };

  await t([1, 2, 3], () => false, []);
  await t([1, 2, 3], () => true, [1, 2, 3]);
  await t([1, 2, 3], v => v % 2 === 0, [2]);
  await t([1, 2, 3], v => v % 2 === 1, [1, 3]);
});
