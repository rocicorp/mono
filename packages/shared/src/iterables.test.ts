import {describe, expect, test} from 'vitest';
import {getESLibVersion} from './get-es-lib-version.ts';
import {
  emptyIterator,
  makeEmptyIteratorWithReturn,
  wrapIterable,
} from './iterables.ts';

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

describe('makeEmptyIteratorWithReturn', () => {
  // The reference behavior: a generator that yields nothing and returns value.
  function makeGenerator<T>(value: T): Generator<unknown, T, unknown> {
    // oxlint-disable-next-line require-yield
    return (function* () {
      return value;
    })();
  }

  function* delegate<T>(inner: IterableIterator<unknown, T, unknown>) {
    return yield* inner;
  }

  describe.each([1, false, undefined, {a: 1}])('value %o', value => {
    test('first next() matches function* () { return v }', () => {
      expect(makeEmptyIteratorWithReturn(value).next()).toEqual(
        makeGenerator(value).next(),
      );
    });

    test('Symbol.iterator returns itself', () => {
      const it = makeEmptyIteratorWithReturn(value);
      expect(it[Symbol.iterator]()).toBe(it);
    });

    test('yield* matches function* () { return v }', () => {
      expect([...delegate(makeEmptyIteratorWithReturn(value))]).toEqual([
        ...delegate(makeGenerator(value)),
      ]);
      expect(delegate(makeEmptyIteratorWithReturn(value)).next()).toEqual(
        delegate(makeGenerator(value)).next(),
      );
    });

    // The remaining tests cover where it intentionally differs from a real
    // generator: it is stateless so a single instance can be shared.

    test('next() keeps returning value', () => {
      // A real generator returns {done: true, value: undefined} once done.
      const it = makeEmptyIteratorWithReturn(value);
      expect(it.next()).toEqual({done: true, value});
      expect(it.next()).toEqual({done: true, value});
    });

    test('can be reused with yield*', () => {
      const it = makeEmptyIteratorWithReturn(value);
      expect(delegate(it).next()).toEqual({done: true, value});
      expect(delegate(it).next()).toEqual({done: true, value});
    });
  });

  test('emptyIterator yields nothing and returns undefined', () => {
    expect([...emptyIterator]).toEqual([]);
    expect(emptyIterator.next()).toEqual({done: true, value: undefined});
    expect(delegate(emptyIterator).next()).toEqual({
      done: true,
      value: undefined,
    });
  });
});
