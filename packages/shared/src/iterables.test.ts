import {describe, expect, test} from 'vitest';
import {mergeIterables, wrapIterable} from './iterables.js';
import fc from 'fast-check';

function* range(start = 0, end = Infinity, step = 1) {
  for (let i = start; i < end; i += step) {
    yield i;
  }
}

test('wrapper should be iterable', () => {
  const result = [];
  for (const item of wrapIterable(range(0, 3))) {
    result.push(item);
  }
  expect(result).toEqual([0, 1, 2]);
});

test('wrapper should wrap be iterable', () => {
  const result = [];
  for (const item of wrapIterable([0, 1, 2])) {
    result.push(item);
  }
  expect(result).toEqual([0, 1, 2]);
});

test('wrapper should wrap be iterable 2', () => {
  const result = [];
  for (const item of wrapIterable('abc💩')) {
    result.push(item);
  }
  expect(result).toEqual(['a', 'b', 'c', '💩']);
});

test('filter', () => {
  const result = wrapIterable(range(0, 10)).filter(x => x % 2 === 0);
  expect([...result]).toEqual([0, 2, 4, 6, 8]);
});

test('filter index', () => {
  const result = wrapIterable(range(0, 10)).filter((_, i) => i % 2 === 0);
  expect([...result]).toEqual([0, 2, 4, 6, 8]);
});

test('map', () => {
  const result = wrapIterable(range(0, 10)).map(x => x * 2);
  expect([...result]).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
});

test('map index', () => {
  const result = wrapIterable('abc').map((c, i) => [c, i * 2]);
  expect([...result]).toEqual([
    ['a', 0],
    ['b', 2],
    ['c', 4],
  ]);
});

test('chaining filter and map', () => {
  const result = wrapIterable(range(0, 10))
    .filter(x => x % 2 === 0)
    .map(x => x * 2);
  expect([...result]).toEqual([0, 4, 8, 12, 16]);
});

describe('mergeIterables', () => {
  test('no dupes, interleaved items', () => {
    const iterables = [
      [1, 3, 5],
      [2, 4, 6],
    ];
    const result = mergeIterables(iterables, (l, r) => l - r);
    expect([...result]).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('dupes', () => {
    const iterables = [
      [1, 2, 3],
      [1, 2, 3],
    ];
    let result = mergeIterables(iterables, (l, r) => l - r);
    expect([...result]).toEqual([1, 1, 2, 2, 3, 3]);

    result = mergeIterables(iterables, (l, r) => l - r, true);
    expect([...result]).toEqual([1, 2, 3]);
  });

  test('fuzz', () => {
    fc.assert(
      fc.property(
        fc.array(fc.array(fc.integer())),
        fc.boolean(),
        (arrays, noDupes) => {
          const sorted = arrays.map(a => a.slice().sort((l, r) => l - r));
          const result = mergeIterables(sorted, (l, r) => l - r, noDupes);
          const expected = sorted.flat().sort((l, r) => l - r);
          expect([...result]).toEqual(
            noDupes ? [...new Set(expected)] : expected,
          );
        },
      ),
    );
  });
});
