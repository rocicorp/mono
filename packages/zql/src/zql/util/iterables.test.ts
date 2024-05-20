import {expect, test} from 'vitest';
import {
  genCached,
  genFilter,
  genFlatMap,
  genMap,
  iterInOrder,
  mapIter,
} from './iterables.js';
import fc from 'fast-check';

test('mapIter', () => {
  const iterable = [1, 2, 3];
  const result = mapIter(iterable, (x, i) => x + i);
  expect([...result]).toEqual([1, 3, 5]);
});

test('genFlatMap', () => {
  const iterable = [[1], [2, 3], [4, 5, 6]];
  const flatMapper = genFlatMap(iterable, x => x);

  expect([...flatMapper]).toEqual([1, 2, 3, 4, 5, 6]);
  // can iterate it a second time
  expect([...flatMapper]).toEqual([1, 2, 3, 4, 5, 6]);
});

test('iterInOrder', () => {
  fc.assert(
    fc.property(
      fc.array(fc.array(fc.integer()), {minLength: 1, maxLength: 3}),
      arrays => {
        const sorted = arrays
          .reduce((acc, cur) => acc.concat(cur), [])
          .sort((l, r) => l - r);
        // iterInOrder assumes inputs are ordered
        arrays.forEach(a => a.sort((l, r) => l - r));
        const result = [...iterInOrder(arrays, (l, r) => l - r)];
        expect(result).toEqual(sorted);
      },
    ),
  );
});

test('multiple iterations over a genMapCached will return the exact same results', () => {
  check<number>(genMap, _ => Math.random());
});

test('multiple iterations over a genFilterCached will return the exact same results', () => {
  check<boolean>(genFilter, _ => Math.random() > 0.5);
});

test('multiple iterations over a genFlatMapCached will return the exact same results', () => {
  check<number[]>(genFlatMap, x => (Math.random() > 0.5 ? [x, x, x] : []));
});

function check<R>(
  gen: (p: Iterable<number>, f: (p: number) => R) => Iterable<number>,
  fn: (p: number) => R,
) {
  const iterable = Array.from({length: 100}, (_, i) => i);
  const filter = genCached(gen(iterable, fn));

  const first = filter[Symbol.iterator]();
  const second = filter[Symbol.iterator]();
  let third: Iterator<number>;

  const firstResult: number[] = [];
  const secondResult: number[] = [];
  const thirdResult: number[] = [];

  let firstValue = first.next();
  let secondValue = second.next();
  // iterate both at random intervals to test iterators getting ahead of one another
  while (firstValue.done === false || secondValue.done === false) {
    if (Math.random() > 0.5 && firstValue.done === false) {
      firstResult.push(firstValue.value);
      firstValue = first.next();
    } else if (secondValue.done === false) {
      secondResult.push(secondValue.value);
      secondValue = second.next();
    }

    if (firstResult.length === 1) {
      third = filter[Symbol.iterator]();
    }
  }

  let thirdValue: IteratorResult<number>;
  while ((thirdValue = third!.next()).done === false) {
    thirdResult.push(thirdValue.value);
  }

  expect(firstResult).toEqual(secondResult);
  expect(secondResult).toEqual(thirdResult);
}
