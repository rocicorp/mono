import {expect, test} from 'vitest';
import {
  genFilter,
  genFilterCached,
  genFlatMap,
  genFlatMapCached,
  genMap,
  genMapCached,
  mapIter,
} from './iterables.js';

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

test('genMap finally handling', () => {
  // iterate manually
  // iterate with a loop construct

  const iterable = [1, 2, 3];
  let finallyCalled = false;
  const mappedIterable = genMap(
    iterable,
    x => x + 1,
    () => {
      finallyCalled = true;
    },
  );

  expect(finallyCalled).toBe(false);

  for (const _ of mappedIterable) {
    // do nothing
  }

  expect(finallyCalled).toBe(true);

  finallyCalled = false;
  const manualIter = mappedIterable[Symbol.iterator]();
  manualIter.next();
  manualIter.return();
  expect(finallyCalled).toBe(true);
});

test('genFilter finally handling', () => {
  const iterable = [1, 2, 3];
  let finallyCalled = false;
  const filteredIterable = genFilter(
    iterable,
    x => x > 1,
    () => {
      finallyCalled = true;
    },
  );

  expect(finallyCalled).toBe(false);

  for (const _ of filteredIterable) {
    // do nothing
  }

  expect(finallyCalled).toBe(true);

  finallyCalled = false;
  const manualIter = filteredIterable[Symbol.iterator]();
  manualIter.next();
  manualIter.return();
  expect(finallyCalled).toBe(true);
});

test('genFlatMap finally handling', () => {
  const iterable = [[1], [2, 3], [4, 5, 6]];
  let finallyCalled = false;
  const flatMapper = genFlatMap(
    iterable,
    x => x,
    () => {
      finallyCalled = true;
    },
  );

  expect(finallyCalled).toBe(false);

  for (const _ of flatMapper) {
    // do nothing
  }

  expect(finallyCalled).toBe(true);

  finallyCalled = false;
  const manualIter = flatMapper[Symbol.iterator]();
  manualIter.next();
  manualIter.return();
  expect(finallyCalled).toBe(true);
});

test('multiple iterations over a genMapCached will return the exact same results', () => {
  const iterable = [1, 2, 3];
  const mapper = genMapCached(iterable, _ => Math.random());

  const first = [...mapper];
  const second = [...mapper];

  expect(first).toEqual(second);
});

test('multiple iterations over a genFilterCached will return the exact same results', () => {
  const iterable = [1, 2, 3];
  const filter = genFilterCached(
    iterable,
    (x): x is number => Math.random() > 0.5 || x === 2,
  );

  const first = [...filter];
  const second = [...filter];

  expect(first).toEqual(second);
});

test('multiple iterations over a genFlatMapCached will return the exact same results', () => {
  const iterable = [[1], [2, 3], [4, 5, 6]];
  const flatMapper = genFlatMapCached(iterable, x =>
    Math.random() > 0.5 ? x : [],
  );

  const first = [...flatMapper];
  const second = [...flatMapper];

  expect(first).toEqual(second);
});
