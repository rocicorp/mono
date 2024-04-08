import {expect, test} from 'vitest';
import {flatMapIter, mapIter} from './iterables.js';

test('mapIter', () => {
  const iterable = [1, 2, 3];
  const result = mapIter(iterable, (x, i) => x + i);
  expect([...result]).toEqual([1, 3, 5]);
});

test('flatMapIter', () => {
  const iterable = [[1], [2, 3], [4, 5, 6]];
  const flatMapper = flatMapIter(
    () => iterable,
    x => x,
  );

  expect([...flatMapper]).toEqual([1, 2, 3, 4, 5, 6]);
  // can iterate it a second time
  expect([...flatMapper]).toEqual([1, 2, 3, 4, 5, 6]);
});
