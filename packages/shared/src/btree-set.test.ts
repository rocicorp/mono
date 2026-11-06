import {compareUTF8} from 'compare-utf8';
import fc, {assert, property} from 'fast-check';
import {describe, expect, test} from 'vitest';
import {BTreeSet} from './btree-set.ts';

test('delete', () => {
  const t = new BTreeSet<number>((a, b) => a - b);
  t.add(0);
  t.add(0);
  t.delete(0);
  expect(t.size).toBe(0);
});

test('clone', () => {
  // Test that cloned set has same elements as original
  const t1 = new BTreeSet<number>((a, b) => a - b);
  for (let i = 0; i < 100; i++) {
    t1.add(i);
  }
  const t2 = t1.clone();

  // Verify t2 has all elements from t1
  for (let i = 0; i < 100; i++) {
    expect(t2.has(i)).toBe(true);
  }
  expect(t2.size).toBe(t1.size);

  // Mutations to t2 don't affect t1
  for (let i = 0; i < 50; i++) {
    t2.delete(i);
  }
  expect(t2.size).toBe(50);
  expect(t1.size).toBe(100);
  for (let i = 0; i < 50; i++) {
    expect(t2.has(i)).toBe(false);
    expect(t1.has(i)).toBe(true);
  }
  for (let i = 50; i < 100; i++) {
    expect(t2.has(i)).toBe(true);
    expect(t1.has(i)).toBe(true);
  }

  // Mutations to t1 don't affect t2
  for (let i = 50; i < 100; i++) {
    t1.delete(i);
  }
  expect(t2.size).toBe(50);
  expect(t1.size).toBe(50);
  for (let i = 0; i < 50; i++) {
    expect(t1.has(i)).toBe(true);
    expect(t2.has(i)).toBe(false);
  }
  for (let i = 50; i < 100; i++) {
    expect(t1.has(i)).toBe(false);
    expect(t2.has(i)).toBe(true);
  }
});

describe('iterators', () => {
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

describe('get', () => {
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

describe('has', () => {
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

describe('class-based iterators', () => {
  test('forward iterator produces same results as spread', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    for (let i = 0; i < 100; i++) {
      t.add(i);
    }
    const forward = [...t.valuesFrom()];
    expect(forward).toEqual(Array.from({length: 100}, (_, i) => i));
  });

  test('reverse iterator produces same results as spread', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    for (let i = 0; i < 100; i++) {
      t.add(i);
    }
    const reversed = [...t.valuesReversed()];
    expect(reversed).toEqual(Array.from({length: 100}, (_, i) => 99 - i));
  });

  test('empty tree iterator returns done immediately', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    const fwd = t.values();
    expect(fwd.next()).toEqual({done: true, value: undefined});
    const rev = t.valuesReversed();
    expect(rev.next()).toEqual({done: true, value: undefined});
  });

  test('single element tree works', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    t.add(42);
    expect([...t.values()]).toEqual([42]);
    expect([...t.valuesReversed()]).toEqual([42]);
  });

  test('iterator protocol: [Symbol.iterator] returns self', () => {
    const t = new BTreeSet<number>((a, b) => a - b);
    t.add(1);
    t.add(2);
    const fwd = t.values();
    expect(fwd[Symbol.iterator]()).toBe(fwd);
    const rev = t.valuesReversed();
    expect(rev[Symbol.iterator]()).toBe(rev);
  });
});

describe('fast-check', () => {
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
          const v = vs[Math.floor(Math.random() * vs.length)];
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

describe('toArray', () => {
  // Local copy of the module-local constant in btree-set.ts, so the sizes below
  // sit either side of a node boundary.
  const MAX_NODE_SIZE = 32;

  const numeric = (a: number, b: number) => a - b;

  /** The one assertion that matters: `toArray` is `[...tree]`. */
  function expectSameAsIterator<K>(tree: BTreeSet<K>) {
    const array = tree.toArray();
    expect(array).toEqual([...tree]);
    expect(array.length).toBe(tree.size);
    return array;
  }

  test('empty', () => {
    const t = new BTreeSet<number>(numeric);
    expect(t.toArray()).toEqual([]);
    expectSameAsIterator(t);
  });

  test('one element', () => {
    const t = new BTreeSet<number>(numeric);
    t.add(42);
    expect(t.toArray()).toEqual([42]);
    expectSameAsIterator(t);
  });

  test('sizes around the node boundary', () => {
    // Exhaustive from empty through a few levels of splits, which covers the
    // single-leaf case, the first internal node, and nodes either side of
    // MAX_NODE_SIZE.
    for (const n of [
      0,
      1,
      2,
      MAX_NODE_SIZE - 1,
      MAX_NODE_SIZE,
      MAX_NODE_SIZE + 1,
      2 * MAX_NODE_SIZE - 1,
      2 * MAX_NODE_SIZE,
      2 * MAX_NODE_SIZE + 1,
      MAX_NODE_SIZE * MAX_NODE_SIZE - 1,
      MAX_NODE_SIZE * MAX_NODE_SIZE,
      MAX_NODE_SIZE * MAX_NODE_SIZE + 1,
    ]) {
      const t = new BTreeSet<number>(numeric);
      for (let i = 0; i < n; i++) {
        t.add(i);
      }
      expect(t.toArray(), `size ${n}`).toEqual([...t]);
      expect(t.toArray(), `size ${n}`).toEqual(
        Array.from({length: n}, (_, i) => i),
      );
    }
  });

  test('a few thousand elements, inserted out of order', () => {
    const t = new BTreeSet<number>(numeric);
    // A stride coprime with the size, so insertion order is nothing like sorted
    // order and the tree splits and shifts between siblings on the way.
    const n = 5000;
    for (let i = 0; i < n; i++) {
      t.add((i * 1237) % n);
    }
    const array = expectSameAsIterator(t);
    expect(array.length).toBe(n);
  });

  test('after deletes', () => {
    // Deterministic pseudo-random deletes: an LCG, so a failure reproduces.
    let seed = 0x2545f491;
    const rand = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);

    for (const n of [50, 200, 2000]) {
      const t = new BTreeSet<number>(numeric);
      for (let i = 0; i < n; i++) {
        t.add(i);
      }
      const deleted = new Set<number>();
      // Enough deletes to force merges and empty-child removal.
      for (let i = 0; i < n * 0.8; i++) {
        const v = rand() % n;
        t.delete(v);
        deleted.add(v);
        expect(t.toArray(), `size ${n} after ${i + 1} deletes`).toEqual([...t]);
      }
      expect(t.toArray()).toEqual(
        Array.from({length: n}, (_, i) => i).filter(i => !deleted.has(i)),
      );
    }
  });

  test('delete everything', () => {
    const t = new BTreeSet<number>(numeric);
    const n = 500;
    for (let i = 0; i < n; i++) {
      t.add(i);
    }
    for (let i = 0; i < n; i++) {
      t.delete(i);
      expectSameAsIterator(t);
    }
    expect(t.toArray()).toEqual([]);
  });

  test('clone', () => {
    const t = new BTreeSet<number>(numeric);
    const n = 1000;
    for (let i = 0; i < n; i++) {
      t.add(i);
    }
    // Clone marks the root shared; walking must not mutate or unshare anything.
    const c = t.clone();
    expect(c.toArray()).toEqual([...t]);
    expectSameAsIterator(c);
    expectSameAsIterator(t);

    // Diverge them and check both still agree with their own iterator.
    for (let i = 0; i < n; i += 3) {
      c.delete(i);
    }
    c.add(-1);
    expectSameAsIterator(c);
    expectSameAsIterator(t);
    expect(t.toArray()).toEqual(Array.from({length: n}, (_, i) => i));
  });

  test('the result is the caller’s to mutate', () => {
    const t = new BTreeSet<number>(numeric);
    // Single leaf: the one case that could hand back the node's own array.
    t.add(2);
    t.add(1);
    const array = t.toArray();
    array.sort((a, b) => b - a);
    expect(array).toEqual([2, 1]);
    expect([...t]).toEqual([1, 2]);

    // And with an internal node.
    for (let i = 0; i < 2 * MAX_NODE_SIZE; i++) {
      t.add(i);
    }
    const array2 = t.toArray();
    array2.length = 0;
    expect(t.size).toBe(2 * MAX_NODE_SIZE);
    expect(t.toArray().length).toBe(2 * MAX_NODE_SIZE);
  });

  test('fromSorted', () => {
    // The production path in memory-source builds indexes this way.
    for (const n of [0, 1, MAX_NODE_SIZE, MAX_NODE_SIZE + 1, 1025, 5000]) {
      const sorted = Array.from({length: n}, (_, i) => i * 2);
      const t = BTreeSet.fromSorted(numeric, sorted);
      expect(t.toArray(), `size ${n}`).toEqual([...t]);
      expect(t.toArray(), `size ${n}`).toEqual(sorted);
    }
  });

  test('more leaves than one concat chunk', () => {
    // toArray joins the leaves in chunks of 1024 (a local copy of the
    // module-local CONCAT_CHUNK), so cross that boundary: at 32 keys per leaf
    // it takes about 32k rows for the join to need a second round.
    const CONCAT_CHUNK = 1024;
    for (const leaves of [CONCAT_CHUNK - 1, CONCAT_CHUNK, CONCAT_CHUNK + 1]) {
      const n = leaves * MAX_NODE_SIZE;
      const sorted = Array.from({length: n}, (_, i) => i);
      const t = BTreeSet.fromSorted(numeric, sorted);
      expect(t.toArray(), `${leaves} leaves`).toEqual(sorted);
      expect(t.toArray(), `${leaves} leaves`).toEqual([...t]);
    }
  });

  test('strings', () => {
    const t = new BTreeSet<string>(compareUTF8);
    for (let i = 0; i < 1000; i++) {
      t.add(`key-${i}`);
    }
    expectSameAsIterator(t);
  });

  test('randomized: equals the iterator over many shapes', () => {
    assert(
      property(
        fc.array(fc.integer({min: -500, max: 500}), {maxLength: 2000}),
        fc.array(fc.integer({min: -500, max: 500}), {maxLength: 2000}),
        (added, removed) => {
          const t = new BTreeSet<number>(numeric);
          for (const v of added) {
            t.add(v);
          }
          for (const v of removed) {
            t.delete(v);
          }
          expect(t.toArray()).toEqual([...t]);
          expect(t.toArray().length).toBe(t.size);
          return true;
        },
      ),
      {numRuns: 300},
    );
  });
});
