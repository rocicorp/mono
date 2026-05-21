import {expect, test} from 'vitest';
import {getESLibVersion} from './get-es-lib-version.ts';
import {wrapIterable} from './iterables.ts';

function* range(start = 0, end = Infinity, step = 1) {
  for (let i = start; i < end; i += step) {
    yield i;
  }
}

test('lib < ES2024', () => {
  // Iterator.from was added in ES2024

  // sanity check that we are using not yet using es2024. If this starts failing
  // then we can remove the wrapIterable and use the builtins.
  expect(getESLibVersion()).toBeLessThan(2024);
});

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

test('some returns true when predicate matches', () => {
  expect(wrapIterable(range(0, 5)).some(x => x === 3)).toBe(true);
});

test('some returns false when predicate never matches', () => {
  expect(wrapIterable(range(0, 5)).some(x => x === 10)).toBe(false);
});

test('some returns false for empty iterable', () => {
  expect(wrapIterable([]).some(() => true)).toBe(false);
});

test('some short-circuits on first match', () => {
  let count = 0;
  wrapIterable(range(0, 100)).some(x => {
    count++;
    return x === 2;
  });
  expect(count).toBe(3);
});

test('some index', () => {
  expect(wrapIterable(['a', 'b', 'c']).some((_, i) => i === 1)).toBe(true);
});

test('some index short-circuits', () => {
  let count = 0;
  wrapIterable(range(0, 100)).some((_, i) => {
    count++;
    return i === 2;
  });
  expect(count).toBe(3);
});

test('some works after filter', () => {
  expect(
    wrapIterable(range(0, 10))
      .filter(x => x % 2 === 0)
      .some(x => x === 6),
  ).toBe(true);
});
