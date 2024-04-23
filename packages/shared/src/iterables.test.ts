import {expect, test} from 'vitest';
import {wrapIterable} from './iterables.js';

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
