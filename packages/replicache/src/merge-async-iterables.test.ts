import {expect} from 'chai';
import {asyncIterableToArray} from './async-iterable-to-array.js';
import {mergeAsyncIterables} from './merge-async-iterables.js';
import {stringCompare} from './string-compare.js';

export async function* makeAsyncIterable<V>(
  values: Iterable<V>,
): AsyncIterable<V> {
  for (const value of values) {
    yield value;
  }
}

test('mergeAsyncIterables', async () => {
  const numCompare = (a: number, b: number) => a - b;

  const t = async <A, B>(
    a: A[],
    b: B[],
    expected: (A | B)[],
    compare: (a: A, b: B) => number,
  ) => {
    {
      const iter = makeAsyncIterable(a);
      const iter2 = makeAsyncIterable(b);
      const merged = mergeAsyncIterables(iter, iter2, compare);
      expect(await asyncIterableToArray(merged)).to.deep.equal(expected);
    }
    {
      const iter = a;
      const iter2 = makeAsyncIterable(b);
      const merged = mergeAsyncIterables(iter, iter2, compare);
      expect(await asyncIterableToArray(merged)).to.deep.equal(expected);
    }
    {
      const iter = makeAsyncIterable(a);
      const iter2 = b;
      const merged = mergeAsyncIterables(iter, iter2, compare);
      expect(await asyncIterableToArray(merged)).to.deep.equal(expected);
    }
    {
      const iter = a;
      const iter2 = b;
      const merged = mergeAsyncIterables(iter, iter2, compare);
      expect(await asyncIterableToArray(merged)).to.deep.equal(expected);
    }
  };

  await t([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6], numCompare);
  await t([4, 5, 6], [1, 2, 3], [1, 2, 3, 4, 5, 6], numCompare);
  await t([2, 3, 4], [1, 2, 3], [1, 2, 3, 4], numCompare);

  // Pick second when equal.
  await t(
    [['a', 0] as const, ['b', 1] as const, ['c', 2] as const],
    [['b', 3] as const],
    [
      ['a', 0],
      ['b', 3],
      ['c', 2],
    ],
    (a, b) => stringCompare(a[0], b[0]),
  );

  // Use undefined as delete sentinel.
  await t(
    [['a', 0] as const, ['b', 1] as const, ['c', 2] as const],
    [['b', undefined] as const],
    [
      ['a', 0],
      ['b', undefined],
      ['c', 2],
    ],
    (a, b) => stringCompare(a[0], b[0]),
  );

  // deleted in pending
  await t(
    [['a', 0] as const, ['b', 1] as const, ['c', 2] as const],
    [['d', undefined] as const],
    [
      ['a', 0],
      ['b', 1],
      ['c', 2],
      ['d', undefined],
    ],
    (a, b) => stringCompare(a[0], b[0]),
  );
});
