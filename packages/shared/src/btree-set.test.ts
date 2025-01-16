import {compareUTF8} from 'compare-utf8';
import fc, {assert, property} from 'fast-check';
import {expect, suite, test} from 'vitest';
import {BTreeSet} from './btree-set.js';

test('delete', () => {
  const t = new BTreeSet<number>((a, b) => a - b);
  t.add(0);
  t.add(0);
  t.delete(0);
  expect(t.size).toBe(0);
});

suite('iterators', () => {
  const t = new BTreeSet<number>((a, b) => a - b);
  t.add(10);
  t.add(5);
  t.add(15);
  t.add(2);

  test('values', () => {
    expect([...t.values()]).toEqual([2, 5, 10, 15]);
  });

  test('valuesReversed', () => {
    expect([...t.valuesReversed()]).toEqual([15, 10, 5, 2]);
  });

  test('valuesFrom 5', () => {
    expect([...t.valuesFrom(5)]).toEqual([5, 10, 15]);
    expect([...t.valuesFrom(5, false)]).toEqual([10, 15]);
  });

  test('valuesFrom 6', () => {
    expect([...t.valuesFrom(6)]).toEqual([10, 15]);
    expect([...t.valuesFrom(6, false)]).toEqual([10, 15]);
  });

  test('valuesFrom 4', () => {
    expect([...t.valuesFrom(4)]).toEqual([5, 10, 15]);
    expect([...t.valuesFrom(4, false)]).toEqual([5, 10, 15]);
  });

  test('valuesFromReversed 10', () => {
    expect([...t.valuesFromReversed(10)]).toEqual([10, 5, 2]);
    expect([...t.valuesFromReversed(10, false)]).toEqual([5, 2]);
  });

  test('valuesFromReversed 6', () => {
    expect([...t.valuesFromReversed(6)]).toEqual([5, 2]);
    expect([...t.valuesFromReversed(6, false)]).toEqual([5, 2]);
  });

  test('valuesFromReversed 11', () => {
    expect([...t.valuesFromReversed(11)]).toEqual([10, 5, 2]);
    expect([...t.valuesFromReversed(11, false)]).toEqual([10, 5, 2]);
  });
});

suite('get', () => {
  test('number', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    t.add(10);
    t.add(5);
    t.add(15);
    t.add(2);

    expect(t.get(5)).toBe(5);
    expect(t.get(10)).toBe(10);
    expect(t.get(15)).toBe(15);
    expect(t.get(2)).toBe(2);
    expect(t.get(0)).toBe(undefined);
  });

  test('entry', () => {
    const t = new BTreeSet<[string, number]>((a, b) => compareUTF8(a[0], b[0]));
    t.add(['a', 1]);
    t.add(['b', 2]);
    t.add(['c', 3]);

    expect(t.get(['a', 1])).toEqual(['a', 1]);
    expect(t.get(['b', 2])).toEqual(['b', 2]);
    expect(t.get(['c', 3])).toEqual(['c', 3]);
    expect(t.get(['d', 4])).toBe(undefined);

    expect(t.get(['a', 2])).toEqual(['a', 1]);
  });
});

suite('has', () => {
  test('number', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    t.add(10);
    t.add(5);
    t.add(15);
    t.add(2);

    expect(t.has(5)).toBe(true);
    expect(t.has(10)).toBe(true);
    expect(t.has(15)).toBe(true);
    expect(t.has(2)).toBe(true);
    expect(t.has(0)).toBe(false);
  });

  test('entry', () => {
    const t = new BTreeSet<[string, number]>((a, b) => compareUTF8(a[0], b[0]));
    t.add(['a', 1]);
    t.add(['b', 2]);
    t.add(['c', 3]);

    expect(t.has(['a', 1])).toBe(true);
    expect(t.has(['b', 2])).toBe(true);
    expect(t.has(['c', 3])).toBe(true);
    expect(t.has(['d', 4])).toBe(false);

    expect(t.has(['a', 2])).toBe(true);
  });
});

test('add should allow replacing equal entry', () => {
  const t = new BTreeSet<[string, number]>((a, b) => compareUTF8(a[0], b[0]));
  t.add(['a', 1]);
  t.add(['b', 2]);
  t.add(['c', 3]);

  expect([...t]).toEqual([
    ['a', 1],
    ['b', 2],
    ['c', 3],
  ]);

  t.add(['b', 4]);
  expect([...t]).toEqual([
    ['a', 1],
    ['b', 4],
    ['c', 3],
  ]);
});

suite('fast-check', () => {
  test('OrderedSet Property-based Tests (large)', () => {
    assert(
      property(
        // Arbitrarily generate a list of operations
        script(100),
        checkMutableOrderedSet,
      ),
    );
  });

  test('OrderedSet Property-based Tests (small)', () => {
    assert(property(script(0), checkMutableOrderedSet));
  });

  function script(size: number) {
    return fc.array(
      fc.tuple(
        fc.oneof(
          fc.constant('insert'),
          fc.constant('delete'),
          fc.constant('reinsert'),
        ),
        fc.integer(),
      ),
      {
        minLength: size,
      },
    );
  }

  function checkMutableOrderedSet(operations: [string, number][]) {
    checkOrderedSet(
      operations,
      () => new BTreeSet<number>((l, r) => l - r),
      true,
    );
  }

  function checkOrderedSet(
    operations: [string, number][],
    ctor: () => BTreeSet<number>,
    mutable = false,
  ) {
    const orderedSet = ctor();
    const set = new Set<number>();

    for (const [operation, value] of operations) {
      const oldOrderedSet = orderedSet;
      const oldOrderedSetValues = [...oldOrderedSet];
      switch (operation) {
        case 'insert':
          orderedSet.add(value);
          set.add(value);
          break;
        case 'delete': {
          if (set.size === 0) {
            continue;
          }
          const vs = [...set.values()];
          const v = vs[Math.floor(Math.random() * vs.length)]!;
          orderedSet.delete(v);
          set.delete(v);
          break;
        }
        case 'reinsert':
          orderedSet.add(value);
          orderedSet.add(value);
          set.add(value);
          break;
      }
      const oldOrderedSetValuesPostModification = [...oldOrderedSet];

      // immutable OrderedSet should not be modified in place.
      if (!mutable) {
        expect(oldOrderedSetValues).toEqual(
          oldOrderedSetValuesPostModification,
        );
      }
    }

    // 1. The OrderedSet has all items that are in the set.
    for (const item of set) {
      expect(orderedSet.has(item)).toBe(true);
    }

    // 2. The OrderedSet returns items in sorted order when iterating.
    let lastValue = Number.NEGATIVE_INFINITY;
    for (const value of orderedSet) {
      expect(value).toBeGreaterThan(lastValue);
      lastValue = value;
    }

    // 3. The OrderedSet's size matches the set.
    expect(orderedSet.size).toBe(set.size);

    return true;
  }
});
