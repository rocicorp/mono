/* eslint-disable */
import BTree, {
  IMap,
  EmptyBTree,
  defaultComparator,
  simpleComparator,
} from './b+tree.js';
import SortedArray from './sorted-array.js';
import MersenneTwister from 'mersenne-twister';
import {expect, test, describe, beforeEach} from 'vitest';

var rand: any = new MersenneTwister(1234);
function randInt(max: number) {
  return rand.random_int() % max;
}
function expectTreeEqualTo(a: BTree, b: SortedArray) {
  a.checkValid();
  expect(a.toArray()).toEqual(b.getArray());
}
function addToBoth<K, V>(a: IMap<K, V>, b: IMap<K, V>, k: K, v: V) {
  expect(a.set(k, v)).toEqual(b.set(k, v));
}

describe('defaultComparator', () => {
  const dateA = new Date(Date.UTC(96, 1, 2, 3, 4, 5));
  const dateA2 = new Date(Date.UTC(96, 1, 2, 3, 4, 5));
  const dateB = new Date(Date.UTC(96, 1, 2, 3, 4, 6));
  const values = [
    dateA,
    dateA2,
    dateB,
    dateA.valueOf(),
    '24x',
    '0',
    '1',
    '3',
    'String',
    '10',
    0,
    'NaN',
    NaN,
    Infinity,
    -0,
    -Infinity,
    1,
    10,
    2,
    [],
    '[]',
    [1],
    ['1'],
  ];
  const sorted = [-Infinity, -10, -1, -0, 0, 1, 2, 10, Infinity];
  testComparison(defaultComparator, sorted, values, [
    [dateA, dateA2],
    [0, -0],
    [[1], ['1']],
  ]);
});

describe('simpleComparator with non-NaN numbers and null', () => {
  const sorted = [-Infinity, -10, -1, -0, 0, null, 1, 2, 10, Infinity];
  testComparison<number | null>(simpleComparator, sorted, sorted, [
    [-0, 0],
    [-0, null],
    [0, null],
  ]);
});

describe('simpleComparator with strings', () => {
  const values = [
    '24x',
    '+0',
    '0.0',
    '0',
    '-0',
    '1',
    '3',
    'String',
    '10',
    'NaN',
  ];
  testComparison<string>(simpleComparator, [], values, []);
});

describe('simpleComparator with Date', () => {
  const dateA = new Date(Date.UTC(96, 1, 2, 3, 4, 5));
  const dateA2 = new Date(Date.UTC(96, 1, 2, 3, 4, 5));
  const dateB = new Date(Date.UTC(96, 1, 2, 3, 4, 6));
  const values = [dateA, dateA2, dateB, null];
  testComparison<Date | null>(simpleComparator, [], values, [[dateA, dateA2]]);
});

describe('simpleComparator arrays', () => {
  const values = [[], [1], ['1'], [2]];
  testComparison<(number | string)[]>(simpleComparator, [], values, [
    [[1], ['1']],
  ]);
});

/**
 * Tests a comparison function, ensuring it produces a strict partial order over the provided values.
 * Additionally confirms that the comparison function has the correct definition of equality via expectedDuplicates.
 */
function testComparison<T>(
  comparison: (a: T, b: T) => number,
  inOrder: T[],
  values: T[],
  expectedDuplicates: [T, T][] = [],
) {
  function compare(a: T, b: T): number {
    const v = comparison(a, b);
    expect(typeof v).toEqual('number');
    if (v !== v) console.log('!!!', a, b);
    expect(v === v).toEqual(true); // Not NaN
    return Math.sign(v);
  }

  test('comparison has correct order', () => {
    expect([...inOrder].sort(comparison)).toMatchObject(inOrder);
  });

  test('comparison deffierantes values', () => {
    let duplicates: [T, T][] = [];
    for (let i = 0; i < values.length; i++) {
      for (let j = i + 1; j < values.length; j++) {
        if (compare(values[i], values[j]) === 0) {
          duplicates.push([values[i], values[j]]);
        }
      }
    }
    expect(duplicates).toMatchObject(expectedDuplicates);
  });

  test('comparison forms a strict partial ordering', () => {
    // To be a strict partial order, the function must be:
    // irreflexive: not a < a
    // transitive: if a < b and b < c then a < c
    // asymmetric: if a < b then not b < a

    // Since our comparison has three outputs, we adjust that to, we need to tighten the rules that involve 'not a < b' (where we have two possible outputs) as follows:
    // irreflexive: compare(a, a) === 0
    // transitive: if compare(a, b) < 0 and compare(b, c) < 0 then compare(a, c) < 0
    // asymmetric: sign(compare(a, b)) === -sign(compare(b, a))

    // This can is brute forced in O(n^3) time below:
    // Violations
    const irreflexive: T[] = [];
    const transitive: [T, T, T][] = [];
    const asymmetric: [T, T][] = [];
    for (const a of values) {
      // irreflexive: compare(a, a) === 0
      if (compare(a, a) !== 0) irreflexive.push(a);
      for (const b of values) {
        for (const c of values) {
          // transitive: if compare(a, b) < 0 and compare(b, c) < 0 then compare(a, c) < 0
          if (compare(a, b) < 0 && compare(b, c) < 0) {
            if (compare(a, c) !== -1) transitive.push([a, b, c]);
          }
        }
        // sign(compare(a, b)) === -sign(compare(b, a))
        if (compare(a, b) !== -compare(b, a)) asymmetric.push([a, b]);
      }
    }
    expect(irreflexive).toEqual([]);
    expect(transitive).toEqual([]);
    expect(asymmetric).toEqual([]);
  });
}

describe('height calculation', () => {
  test('Empty tree', () => {
    const tree = new BTree<number>();
    expect(tree.height).toEqual(0);
  });
  test('Single node', () => {
    const tree = new BTree<number>([[0, 0]]);
    expect(tree.height).toEqual(0);
  });
  test('Multiple node, no internal nodes', () => {
    const tree = new BTree<number>(
      [
        [0, 0],
        [1, 1],
      ],
      undefined,
      32,
    );
    expect(tree.height).toEqual(0);
  });
  test('Multiple internal nodes', () => {
    for (let expectedHeight = 1; expectedHeight < 5; expectedHeight++) {
      for (let nodeSize = 4; nodeSize < 10; nodeSize++) {
        const numEntries = nodeSize ** expectedHeight;
        const entries: [number, number][] = [];
        for (let i = 0; i < numEntries; i++) {
          entries.push([i, i]);
        }
        const tree = new BTree<number>(entries, undefined, nodeSize);
        expect(tree.height).toEqual(expectedHeight - 1);
      }
    }
  });
});

describe('Simple tests on leaf nodes', () => {
  test('A few insertions (fanout 8)', insert8.bind(null, 8));
  test('A few insertions (fanout 4)', insert8.bind(null, 4));
  function insert8(maxNodeSize: number) {
    var items: [number, any][] = [
      [6, 'six'],
      [7, 7],
      [5, 5],
      [2, 'two'],
      [4, 4],
      [1, 'one'],
      [3, 3],
      [8, 8],
    ];
    var tree = new BTree<number>(items, undefined, maxNodeSize);
    var list = new SortedArray(items, undefined);
    tree.checkValid();
    expect(tree.keysArray()).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expectTreeEqualTo(tree, list);
  }

  function forExpector(
    k: number,
    v: string,
    counter: number,
    i: number,
    first: number = 0,
  ) {
    expect(k).toEqual(v.length);
    expect(k - first).toEqual(counter);
    expect(k - first).toEqual(i);
  }
  {
    let tree = new BTree<number, string>([
      [0, ''],
      [1, '1'],
      [2, 'to'],
      [3, 'tri'],
      [4, 'four'],
      [5, 'five!'],
    ]);
    test('forEach', () => {
      let i = 0;
      expect(
        tree.forEach(
          function (this: any, v, k, tree_) {
            expect(tree_).toBe(tree);
            expect((this as any).self).toBe('me');
            forExpector(k, v, i, i++);
          },
          {self: 'me'},
        ),
      ).toBe(6);
    });
    test('forEachPair', () => {
      let i = 0;
      expect(
        tree.forEachPair(function (k, v, counter) {
          forExpector(k, v, counter - 10, i++);
        }, 10),
      ).toBe(16);
    });
    test('forRange', () => {
      let i = 0;
      expect(
        tree.forRange(
          2,
          4,
          false,
          function (k, v, counter) {
            forExpector(k, v, counter - 10, i++, 2);
          },
          10,
        ),
      ).toBe(12);
      i = 0;
      expect(
        tree.forRange(
          2,
          4,
          true,
          function (k, v, counter) {
            forExpector(k, v, counter - 10, i++, 2);
          },
          10,
        ),
      ).toBe(13);
      i = 0;
      expect(
        tree.forRange(
          0,
          4.5,
          true,
          function (k, v, counter) {
            forExpector(k, v, counter - 10, i++);
          },
          10,
        ),
      ).toBe(15);
    });
    test('editRange', () => {
      let i = 0;
      expect(
        tree.editRange(
          1,
          4,
          true,
          function (k, v, counter) {
            forExpector(k, v, counter - 10, i++, 1);
          },
          10,
        ),
      ).toBe(14);
      i = 0;
      expect(
        tree.editRange(
          1,
          9,
          true,
          function (k, v, counter) {
            forExpector(k, v, counter - 10, i++, 1);
            if (k & 1) return {delete: true};
            if (k == 2) return {value: 'TWO!'};
            if (k >= 4) return {break: 'STOP'};
          },
          10,
        ),
      ).toBe('STOP');
      expect(tree.toArray()).toEqual([
        [0, ''],
        [2, 'TWO!'],
        [4, 'four'],
        [5, 'five!'],
      ]);
    });
  }
  {
    let items: [string, any][] = [
      ['A', 1],
      ['B', 2],
      ['C', 3],
      ['D', 4],
      ['E', 5],
      ['F', 6],
      ['G', 7],
      ['H', 8],
    ];
    let tree = new BTree<string>(items);
    tree.checkValid();

    test('has() in a leaf node of strings', () => {
      expect(tree.has('!')).toBe(false);
      expect(tree.has('A')).toBe(true);
      expect(tree.has('H')).toBe(true);
      expect(tree.has('Z')).toBe(false);
    });
    test('get() in a leaf node of strings', () => {
      expect(tree.get('!', 7)).toBe(7);
      expect(tree.get('A', 7)).toBe(1);
      expect(tree.get('H', 7)).toBe(8);
      expect(tree.get('Z', 7)).toBe(7);
    });
    test('getRange() in a leaf node', () => {
      expect(tree.getRange('#', 'B', false)).toEqual([['A', 1]]);
      expect(tree.getRange('#', 'B', true)).toEqual([
        ['A', 1],
        ['B', 2],
      ]);
      expect(tree.getRange('G', 'S', true)).toEqual([
        ['G', 7],
        ['H', 8],
      ]);
    });
    test('iterators work on leaf nodes', () => {
      expect(Array.from(tree.entries())).toEqual(items);
      expect(Array.from(tree.keys())).toEqual(items.map(p => p[0]));
      expect(Array.from(tree.values())).toEqual(items.map(p => p[1]));
    });
    test('try out the reverse iterator', () => {
      expect(Array.from(tree.entriesReversed())).toEqual(
        items.slice(0).reverse(),
      );
    });
    test('minKey() and maxKey()', () => {
      expect(tree.minKey()).toEqual('A');
      expect(tree.maxKey()).toEqual('H');
    });
    test('delete() in a leaf node', () => {
      expect(tree.delete('C')).toBe(true);
      expect(tree.delete('C')).toBe(false);
      expect(tree.delete('H')).toBe(true);
      expect(tree.delete('H')).toBe(false);
      expect(tree.deleteRange(' ', 'A', false)).toBe(0);
      expect(tree.deleteRange(' ', 'A', true)).toBe(1);
      expectTreeEqualTo(
        tree,
        new SortedArray([
          ['B', 2],
          ['D', 4],
          ['E', 5],
          ['F', 6],
          ['G', 7],
        ]),
      );
    });
    test('editRange() - again', () => {
      expect(
        tree.editRange(tree.minKey()!, 'F', true, (k, v, counter) => {
          if (k == 'D') return {value: 44};
          if (k == 'E' || k == 'G') return {delete: true};
          if (k >= 'F') return {stop: counter + 1};
        }),
      ).toBe(4);
      expectTreeEqualTo(
        tree,
        new SortedArray([
          ['B', 2],
          ['D', 44],
          ['F', 6],
          ['G', 7],
        ]),
      );
    });
    test('A clone is independent', () => {
      var tree2 = tree.clone();
      expect(tree.delete('G')).toBe(true);
      expect(tree2.deleteRange('A', 'F', false)).toBe(2);
      expect(tree2.deleteRange('A', 'F', true)).toBe(1);
      expectTreeEqualTo(
        tree,
        new SortedArray([
          ['B', 2],
          ['D', 44],
          ['F', 6],
        ]),
      );
      expectTreeEqualTo(tree2, new SortedArray([['G', 7]]));
    });
  }

  test('Can be frozen and unfrozen', () => {
    var tree = new BTree([[1, 'one']]);
    expect(tree.isFrozen).toBe(false);
    tree.freeze();
    expect(tree.isFrozen).toBe(true);
    expect(() => tree.set(2, 'two')).toThrowError(/frozen/);
    expect(() => tree.setPairs([[2, 'two']])).toThrowError(/frozen/);
    expect(() => tree.clear()).toThrowError(/frozen/);
    expect(() => tree.delete(1)).toThrowError(/frozen/);
    expect(() =>
      tree.editRange(0, 10, true, () => {
        return {delete: true};
      }),
    ).toThrowError(/frozen/);
    expect(tree.toArray()).toEqual([[1, 'one']]);

    tree.unfreeze();
    tree.set(2, 'two');
    tree.delete(1);
    expect(tree.toArray()).toEqual([[2, 'two']]);
    tree.clear();
    expect(tree.keysArray()).toEqual([]);
  });

  test('Custom comparator', () => {
    var tree = new BTree(undefined, (a, b) => {
      if (a.name > b.name) return 1; // Return a number >0 when a > b
      else if (a.name < b.name) return -1; // Return a number <0 when a < b
      // names are equal (or incomparable)
      else return a.age - b.age; // Return >0 when a.age > b.age
    });
    tree.set({name: 'Bill', age: 17}, 'happy');
    tree.set({name: 'Rose', age: 40}, 'busy & stressed');
    tree.set({name: 'Bill', age: 55}, 'recently laid off');
    tree.set({name: 'Rose', age: 10}, 'rambunctious');
    tree.set({name: 'Chad', age: 18}, 'smooth');

    // Try editing a key
    tree.set({name: 'Bill', age: 17, planet: 'Earth'}, 'happy');

    var list: any[] = [];
    expect(
      tree.forEachPair((k, v) => {
        list.push(Object.assign({value: v}, k));
      }, 10),
    ).toBe(15);

    expect(list).toEqual([
      {name: 'Bill', age: 17, planet: 'Earth', value: 'happy'},
      {name: 'Bill', age: 55, value: 'recently laid off'},
      {name: 'Chad', age: 18, value: 'smooth'},
      {name: 'Rose', age: 10, value: 'rambunctious'},
      {name: 'Rose', age: 40, value: 'busy & stressed'},
    ]);
  });
});

// Tests relating to `isShared` and cloning.
// Tests on this subject that do not care about the specific interior structure of the tree
// (and are thus maxNodeSize agnostic) can be added to testBTree to be testing on different branching factors instead.
describe('cloning and sharing tests', () => {
  test('Regression test for failing to propagate shared when removing top two layers', () => {
    // This tests make a full 3 layer tree (height = 2), so use a small branching factor.
    const maxNodeSize = 4;
    const tree = new BTree<number, number>(
      undefined,
      simpleComparator,
      maxNodeSize,
    );
    // Build a 3 layer complete tree, all values 0.
    for (
      let index = 0;
      index < maxNodeSize * maxNodeSize * maxNodeSize;
      index++
    ) {
      tree.set(index, 0);
    }
    // Leaf nodes don't count, so this is depth 2
    expect(tree.height).toBe(2);

    // Edit the tree so it has a node in the second layer with exactly one child (key 0).
    tree.deleteRange(1, maxNodeSize * maxNodeSize, false);
    expect(tree.height).toBe(2);

    // Make a clone that should never be mutated.
    const clone = tree.clone();

    // Mutate the original tree in such a way that clone gets mutated due to incorrect is shared tracking.
    // Delete everything outside of the internal node with only one child, so its child becomes the new root.
    tree.deleteRange(maxNodeSize, Number.POSITIVE_INFINITY, false);
    expect(tree.height).toBe(0);

    // Modify original
    tree.set(0, 1);

    // Check that clone is not modified as well:
    expect(clone.get(0)).toBe(0);
  });

  test('Regression test for greedyClone(true) not copying all nodes', () => {
    const maxNodeSize = 4;
    const tree = new BTree<number, number>(
      undefined,
      simpleComparator,
      maxNodeSize,
    );
    // Build a 3 layer tree.
    for (let index = 0; index < maxNodeSize * maxNodeSize + 1; index++) {
      tree.set(index, 0);
    }
    // Leaf nodes don't count, so this is depth 2
    expect(tree.height).toBe(2);

    // To trigger the bug, mark children of the root node as shared (not just the root)
    tree.clone().set(1, 1);

    const clone = tree.greedyClone(true);

    // The bug was that `force` was not passed down. This meant that non-shared nodes below the second layer would not be cloned.
    // Thus we check that the third layer of this tree did get cloned.
    // Since this depends on private APIs and types,
    // and this package currently has no way to expose them to tests without exporting them from the package,
    // do some private field access and any casts to make it work.
    expect((clone['_root'] as any).children[0].children[0]).not.toBe(
      (tree['_root'] as any).children[0].children[0],
    );
  });

  test('Regression test for mergeSibling setting isShared', () => {
    // This tests make a 3 layer tree (height = 2), so use a small branching factor.
    const maxNodeSize = 4;
    const tree = new BTree<number, number>(
      undefined,
      simpleComparator,
      maxNodeSize,
    );
    // Build a 3 layer tree
    const count = maxNodeSize * maxNodeSize * maxNodeSize;
    for (let index = 0; index < count; index++) {
      tree.set(index, 0);
    }
    // Leaf nodes don't count, so this is depth 2
    expect(tree.height).toBe(2);

    // Delete most of the keys so merging interior nodes is possible, marking all nodes as shared.
    for (let index = 0; index < count; index++) {
      if (index % 4 !== 0) {
        tree.delete(index);
      }
    }

    const deepClone = tree.greedyClone(true);
    const cheapClone = tree.clone();

    // These two clones should remain unchanged forever.
    // The bug this is testing for resulted in the cheap clone getting modified:
    // we will compare it against the deep clone to confirm it does not.

    // Delete a bunch more nodes, causing merging.
    for (let index = 0; index < count; index++) {
      if (index % 16 !== 0) {
        tree.delete(index);
      }
    }

    const different: number[] = [];
    const onDiff = (k: number) => {
      different.push(k);
    };
    deepClone.diffAgainst(cheapClone, onDiff, onDiff, onDiff);
    expect(different).toEqual([]);
  });
});

describe('B+ tree with fanout 32', testBTree.bind(null, 32));
describe('B+ tree with fanout 10', testBTree.bind(null, 10));
describe('B+ tree with fanout 4', testBTree.bind(null, 4));

function testBTree(maxNodeSize: number) {
  for (let size of [8, 64, 512]) {
    let tree = new BTree<number, number>(undefined, undefined, maxNodeSize);
    let list = new SortedArray<number, number>();
    test(`Insert randomly & toArray [size ${size}]`, () => {
      while (tree.size < size) {
        var key = randInt(size * 2);
        addToBoth(tree, list, key, key);
        expect(tree.size).toEqual(list.size);
      }
      expectTreeEqualTo(tree, list);
    });

    test(`Iteration [size ${size}]`, () => {
      expect(tree.size).toBe(size);
      var it = tree.entries();
      var array = list.getArray(),
        i = 0;
      for (let next = it.next(); !next.done; next = it.next(), i++) {
        expect(next.value).toEqual(array[i]);
      }
      expect(i).toBe(array.length);
    });

    test(`Reverse iteration [size ${size}]`, () => {
      expect(tree.size).toBe(size);
      var it = tree.entriesReversed();
      var array = list.getArray(),
        i = array.length - 1;
      for (let next = it.next(); !next.done; next = it.next(), i--) {
        expect(next.value).toEqual(array[i]);
      }
      expect(i).toBe(-1);
    });

    test(`Insert with few values [size ${size}]`, () => {
      let list = new SortedArray<number, string | undefined>();
      for (var i = 0; i < size; i++) {
        var key = randInt(size * 2);
        // Use a value only occasionally to stress out the no-values optimization
        list.set(key, key % 10 == 0 ? key.toString() : undefined);
      }
      let tree = new BTree<number, string | undefined>(
        list.getArray(),
        undefined,
        maxNodeSize,
      );
      expectTreeEqualTo(tree, list);
    });
  }

  describe(`Next higher/lower methods`, () => {
    test(`nextLower/nextHigher methods return undefined in an empty tree`, () => {
      const tree = new BTree<number, number>(undefined, undefined, maxNodeSize);
      expect(tree.nextLowerPair(undefined)).toEqual(undefined);
      expect(tree.nextHigherPair(undefined)).toEqual(undefined);
      expect(tree.getPairOrNextLower(1)).toEqual(undefined);
      expect(tree.getPairOrNextHigher(2)).toEqual(undefined);

      // This shouldn't make a difference
      tree.set(5, 55);
      tree.delete(5);

      expect(tree.nextLowerPair(undefined)).toEqual(undefined);
      expect(tree.nextHigherPair(undefined)).toEqual(undefined);
      expect(tree.nextLowerPair(3)).toEqual(undefined);
      expect(tree.nextHigherPair(4)).toEqual(undefined);
      expect(tree.getPairOrNextLower(5)).toEqual(undefined);
      expect(tree.getPairOrNextHigher(6)).toEqual(undefined);
    });

    for (let size of [5, 10, 300]) {
      // Build a tree and list with pairs whose keys are even numbers: 0, 2, 4, 6, 8, 10...
      const tree = new BTree<number, number>(undefined, undefined, maxNodeSize);
      const pairs: [number, number][] = [];
      for (let i = 0; i < size; i++) {
        const value = i;
        tree.set(i * 2, value);
        pairs.push([i * 2, value]);
      }

      test(`nextLowerPair/nextHigherPair for tree of size ${size}`, () => {
        expect(tree.nextHigherPair(undefined)).toEqual([
          tree.minKey()!,
          tree.get(tree.minKey()!),
        ]);
        expect(tree.nextHigherPair(tree.maxKey())).toEqual(undefined);
        for (let i = 0; i < size * 2; i++) {
          if (i > 0) {
            expect(tree.nextLowerPair(i)).toEqual(pairs[((i + 1) >> 1) - 1]);
          }
          if (i < size - 1) {
            expect(tree.nextHigherPair(i)).toEqual(pairs[(i >> 1) + 1]);
          }
        }
        expect(tree.nextLowerPair(undefined)).toEqual([
          tree.maxKey()!,
          tree.get(tree.maxKey()!),
        ]);
        expect(tree.nextLowerPair(tree.minKey())).toEqual(undefined);
      });

      test(`getPairOrNextLower/getPairOrNextHigher for tree of size ${size}`, () => {
        for (let i = 0; i < size * 2; i++) {
          if (i > 0) {
            expect(tree.getPairOrNextLower(i)).toEqual(pairs[i >> 1]);
          }
          if (i < size - 1) {
            expect(tree.getPairOrNextHigher(i)).toEqual(pairs[(i + 1) >> 1]);
          }
        }
      });
    }
  });

  for (let size of [6, 36, 216]) {
    test(`setPairs & deleteRange [size ${size}]`, () => {
      // Store numbers in descending order
      var reverseComparator = (a: number, b: number) => b - a;

      // Prepare reference list
      var list = new SortedArray<number, string>([], reverseComparator);
      for (var i = size - 1; i >= 0; i--) list.set(i, i.toString());

      // Add all to tree in the "wrong" order (ascending)
      var tree = new BTree<number, string>(
        undefined,
        reverseComparator,
        maxNodeSize,
      );
      tree.setPairs(list.getArray().slice(0).reverse());
      expectTreeEqualTo(tree, list);

      // Remove most of the items
      expect(tree.deleteRange(size - 2, 5, true)).toEqual(size - 6);
      expectTreeEqualTo(
        tree,
        new SortedArray<number, string>(
          [
            [size - 1, (size - 1).toString()],
            [4, '4'],
            [3, '3'],
            [2, '2'],
            [1, '1'],
            [0, '0'],
          ],
          reverseComparator,
        ),
      );
      expect(tree.deleteRange(size, 0, true)).toEqual(6);
      expect(tree.toArray()).toEqual([]);
    });
  }

  for (let size of [5, 25, 125]) {
    // Ensure standard operations work for various list sizes
    test(`Various operations [starting size ${size}]`, () => {
      var tree = new BTree<number, number | undefined>(
        undefined,
        undefined,
        maxNodeSize,
      );
      var list = new SortedArray<number, number | undefined>();

      var i = 0,
        key;
      for (var i = 0; tree.size < size; i++) {
        addToBoth(tree, list, i, undefined);
        expect(list.size).toEqual(tree.size);
      }
      expectTreeEqualTo(tree, list);

      // Add some in the middle and try get()
      for (var i = size; i <= size + size / 8; i += 0.5) {
        expect(tree.get(i)).toEqual(list.get(i));
        addToBoth(tree, list, i, i);
      }
      expectTreeEqualTo(tree, list);
      expect(tree.get(-15, 12345)).toBe(12345);
      expect(tree.get(0.5, 12345)).toBe(12345);

      // Try all the iterators...
      expect(Array.from(tree.entries())).toEqual(list.getArray());
      expect(Array.from(tree.keys())).toEqual(list.getArray().map(p => p[0]));
      expect(Array.from(tree.values())).toEqual(list.getArray().map(p => p[1]));

      // Try iterating from past the end...
      expect(Array.from(tree.entries(tree.maxKey()! + 1))).toEqual([]);
      expect(Array.from(tree.keys(tree.maxKey()! + 1))).toEqual([]);
      expect(Array.from(tree.values(tree.maxKey()! + 1))).toEqual([]);
      expect(Array.from(tree.entriesReversed(tree.minKey()! - 1))).toEqual([]);

      // Try some changes that should have no effect
      for (var i = size; i < size + size / 8; i += 0.5) {
        expect(tree.setIfNotPresent(i, -i)).toBe(false);
        expect(tree.changeIfPresent(-i, -i)).toBe(false);
      }
      expectTreeEqualTo(tree, list);

      // Remove a few items and check against has()
      for (var i = 0; i < 10; i++) {
        key = randInt(size * 2) / 2;
        var has = tree.has(key);
        expect(has).toEqual(list.has(key));
        expect(has).toEqual(tree.delete(key));
        expect(has).toEqual(list.delete(key));
        expectTreeEqualTo(tree, list);
      }

      expectTreeEqualTo(tree, list);
    });
  }

  test('persistent and functional operations', () => {
    var tree = new BTree<number, number | undefined>(
      undefined,
      undefined,
      maxNodeSize,
    );
    var list = new SortedArray<number, number | undefined>();

    // Add keys 10 to 5000, step 10
    for (var i = 1; i <= 500; i++) addToBoth(tree, list, i * 10, i);

    // Test reduce()
    expect(tree.reduce((sum, pair) => sum + pair[1]!, 0)).toBe(501 * 250);

    // Test mapValues()
    tree
      .mapValues(v => v! * 10)
      .forEachPair((k, v) => {
        expect(v).toBe(k);
      });

    // Perform various kinds of no-ops
    var t1 = tree;
    expect(t1.withKeys([10, 20, 30], true)).toBe(tree);
    expect(t1.withKeys([10, 20, 30], false)).not.toBe(tree);
    expect(t1.withoutKeys([5, 105, 205], true)).toBe(tree);
    expect(t1.without(666, true)).toBe(tree);
    expect(t1.withoutRange(1001, 1010, false, true)).toBe(tree);
    expect(t1.filter(() => true, true)).toBe(tree);

    // Make a series of modifications in persistent mode
    var t2 = t1.with(5, 5).with(999, 999);
    var t3 = t2.without(777).without(7);
    var t4 = t3.withPairs(
      [
        [60, 66],
        [6, 6.6],
      ],
      false,
    );
    var t5 = t4.withKeys([199, 299, 399], true);
    var t6 = t4.without(200).without(300).without(400);
    var t7 = t6.withoutKeys([10, 20, 30], true);
    var t8 = t7.withoutRange(100, 200, false, true);

    // Check that it all worked as expected
    expectTreeEqualTo(t1, list);
    list.set(5, 5);
    list.set(999, 999);
    expectTreeEqualTo(t2, list);
    list.delete(777);
    list.delete(7);
    expectTreeEqualTo(t3, list);
    list.set(6, 6.6);
    expectTreeEqualTo(t4, list);
    list.set(199, undefined);
    list.set(299, undefined);
    list.set(399, undefined);
    expectTreeEqualTo(t5, list);
    for (var k of [199, 299, 399, 200, 300, 400]) list.delete(k);
    expectTreeEqualTo(t6, list);
    for (var k of [10, 20, 30]) list.delete(k);
    expectTreeEqualTo(t7, list);
    for (var i = 100; i < 200; i++) list.delete(i);
    expectTreeEqualTo(t8, list);

    // Filter out all hundreds
    var t9 = t8.filter(k => k % 100 !== 0, true);
    for (let k = 0; k <= tree.maxKey()!; k += 100) list.delete(k);
    expectTreeEqualTo(t9, list);
  });

  describe('Diff computation', () => {
    let onlyThis: Map<number, number>;
    let onlyOther: Map<number, number>;
    let different: Map<number, string>;
    function reset(): void {
      onlyOther = new Map();
      onlyThis = new Map();
      different = new Map();
    }

    beforeEach(() => reset());

    const OnlyThis = (k: number, v: number) => {
      onlyThis.set(k, v);
    };
    const OnlyOther = (k: number, v: number) => {
      onlyOther.set(k, v);
    };
    const Different = (k: number, vThis: number, vOther: number) => {
      different.set(k, `vThis: ${vThis}, vOther: ${vOther}`);
    };
    const compare = (a: number, b: number) => a - b;

    function expectMapsEquals<K, V>(mapA: Map<K, V>, mapB: Map<K, V>) {
      const onlyA: [K, V][] = [];
      const onlyB: [K, V][] = [];
      const different: [K, V, V][] = [];
      mapA.forEach((valueA, keyA) => {
        const valueB = mapB.get(keyA);
        if (valueB === undefined) {
          onlyA.push([keyA, valueA]);
        } else if (!Object.is(valueB, valueB)) {
          different.push([keyA, valueA, valueB]);
        }
      });
      mapB.forEach((valueB, keyB) => {
        const valueA = mapA.get(keyB);
        if (valueA === undefined) {
          onlyA.push([keyB, valueB]);
        }
      });
      expect(onlyA.length).toEqual(0);
      expect(onlyB.length).toEqual(0);
      expect(different.length).toEqual(0);
    }

    function expectDiffCorrect(
      treeThis: BTree<number, number>,
      treeOther: BTree<number, number>,
    ): void {
      reset();
      treeThis.diffAgainst(treeOther, OnlyThis, OnlyOther, Different);
      let onlyThisT: Map<number, number> = new Map();
      let onlyOtherT: Map<number, number> = new Map();
      let differentT: Map<number, string> = new Map();
      treeThis.forEachPair((kThis, vThis) => {
        if (!treeOther.has(kThis)) {
          onlyThisT.set(kThis, vThis);
        } else {
          const vOther = treeOther.get(kThis);
          if (!Object.is(vThis, vOther))
            differentT.set(kThis, `vThis: ${vThis}, vOther: ${vOther}`);
        }
      });
      treeOther.forEachPair((kOther, vOther) => {
        if (!treeThis.has(kOther)) {
          onlyOtherT.set(kOther, vOther);
        }
      });
      expectMapsEquals(onlyThis, onlyThisT);
      expectMapsEquals(onlyOther, onlyOtherT);
      expectMapsEquals(different, differentT);
    }

    test(`Diff of trees with different comparators is an error`, () => {
      const treeA = new BTree<number, number>([], compare);
      const treeB = new BTree<number, number>([], (a, b) => b - a);
      expect(() =>
        treeA.diffAgainst(treeB, OnlyThis, OnlyOther, Different),
      ).toThrow('comparators');
    });

    const entriesGroup: [number, number][][] = [
      [],
      [
        [1, 1],
        [2, 2],
        [3, 3],
        [4, 4],
        [5, 5],
      ],
    ];
    entriesGroup.forEach(entries => {
      test(`Diff of the same tree ${
        entries.length > 0 ? '(non-empty)' : '(empty)'
      }`, () => {
        const tree = new BTree<number, number>(entries, compare, maxNodeSize);
        expectDiffCorrect(tree, tree);
        expect(onlyOther.size).toEqual(0);
        expect(onlyThis.size).toEqual(0);
        expect(different.size).toEqual(0);
      });
    });

    test(`Diff of identical trees`, () => {
      const treeA = new BTree<number, number>(
        entriesGroup[1],
        compare,
        maxNodeSize,
      );
      const treeB = new BTree<number, number>(
        entriesGroup[1],
        compare,
        maxNodeSize,
      );
      expectDiffCorrect(treeA, treeB);
    });

    [entriesGroup, [...entriesGroup].reverse()].forEach(doubleEntries => {
      test(`Diff of an ${
        doubleEntries[0].length === 0 ? 'empty' : 'non-empty'
      } tree and a ${
        doubleEntries[1].length === 0 ? 'empty' : 'non-empty'
      } one`, () => {
        const treeA = new BTree<number, number>(
          doubleEntries[0],
          compare,
          maxNodeSize,
        );
        const treeB = new BTree<number, number>(
          doubleEntries[1],
          compare,
          maxNodeSize,
        );
        expectDiffCorrect(treeA, treeB);
      });
    });

    test(`Diff of different trees`, () => {
      const treeA = new BTree<number, number>(
        entriesGroup[1],
        compare,
        maxNodeSize,
      );
      const treeB = new BTree<number, number>(
        entriesGroup[1],
        compare,
        maxNodeSize,
      );
      treeB.set(-1, -1);
      treeB.delete(2);
      treeB.set(3, 4);
      treeB.set(10, 10);
      expectDiffCorrect(treeA, treeB);
    });

    test(`Diff of odds and evens`, () => {
      const treeA = new BTree<number, number>(
        [
          [1, 1],
          [3, 3],
          [5, 5],
          [7, 7],
        ],
        compare,
        maxNodeSize,
      );
      const treeB = new BTree<number, number>(
        [
          [2, 2],
          [4, 4],
          [6, 6],
          [8, 8],
        ],
        compare,
        maxNodeSize,
      );
      expectDiffCorrect(treeA, treeB);
      expectDiffCorrect(treeB, treeA);
    });

    function applyChanges(
      treeA: BTree<number, number>,
      duplicate: (tree: BTree<number, number>) => BTree<number, number>,
    ): void {
      const treeB = duplicate(treeA);
      const maxKey: number = treeA.maxKey()!;
      const onlyInA = -10;
      treeA.set(onlyInA, onlyInA);
      const onlyInBSmall = -1;
      treeB.set(onlyInBSmall, onlyInBSmall);
      const onlyInBLarge = maxKey + 1;
      treeB.set(onlyInBLarge, onlyInBLarge);
      const onlyInAFromDelete = 10;
      treeB.delete(onlyInAFromDelete);
      const differingValue = -100;
      const modifiedInB1 = 3,
        modifiedInB2 = maxKey - 2;
      treeB.set(modifiedInB1, differingValue);
      treeB.set(modifiedInB2, differingValue);
      treeA.diffAgainst(treeB, OnlyThis, OnlyOther, Different);
      expectDiffCorrect(treeA, treeB);
    }

    function makeLargeTree(size?: number): BTree<number, number> {
      size = size ?? Math.pow(maxNodeSize, 3);
      const tree = new BTree<number, number>([], compare, maxNodeSize);
      for (let i = 0; i < size; i++) {
        tree.set(i, i);
      }
      return tree;
    }

    test(`Diff of large trees`, () => {
      const tree = makeLargeTree();
      applyChanges(tree, tree => tree.greedyClone());
    });

    test(`Diff of cloned trees`, () => {
      const tree = makeLargeTree();
      applyChanges(tree, tree => tree.clone());
    });

    test(`Diff can early exit`, () => {
      const tree = makeLargeTree(100);
      const tree2 = tree.clone();
      tree2.set(-1, -1);
      tree2.delete(10);
      tree2.set(20, -1);
      tree2.set(110, -1);
      const ReturnKey = (key: number) => {
        return {break: key};
      };

      let val = tree.diffAgainst(tree2, OnlyThis, OnlyOther, ReturnKey);
      expect(onlyOther.size).toEqual(1);
      expect(onlyThis.size).toEqual(0);
      expect(val).toEqual(20);
      reset();

      val = tree.diffAgainst(tree2, OnlyThis, ReturnKey, Different);
      expect(different.size).toEqual(0);
      expect(onlyThis.size).toEqual(0);
      expect(val).toEqual(110);
      reset();

      val = tree.diffAgainst(tree2, ReturnKey, OnlyOther, Different);
      expect(different.size).toEqual(1);
      expect(onlyOther.size).toEqual(1);
      expect(val).toEqual(10);
      reset();

      expectDiffCorrect(tree, tree2);
    });
  });

  test('Issue #2 reproduction', () => {
    const tree = new BTree<number>([], (a, b) => a - b, maxNodeSize);
    for (let i = 0; i <= 1999; i++) {
      tree.set(i, i);
      if (tree.size > 100 && i % 2 == 0) {
        const key = i / 2;
        tree.delete(key);
        tree.checkValid();
        expect(tree.size).toBe(i / 2 + 50);
      }
    }
  });

  test('entriesReversed when highest key does not exist', () => {
    const entries: [{key: number}, number][] = [
      [{key: 10}, 0],
      [{key: 20}, 0],
      [{key: 30}, 0],
    ];
    const tree = new BTree<{key: number}, number>(
      entries,
      (a, b) => a.key - b.key,
    );
    expect(Array.from(tree.entriesReversed({key: 40}))).toEqual(
      entries.reverse(),
    );
  });

  test('nextLowerPair/nextHigherPair and issue #9: nextLowerPair returns highest pair if key is 0', () => {
    const tree = new BTree<number, number>(undefined, undefined, maxNodeSize);
    tree.set(-2, 123);
    tree.set(0, 1234);
    tree.set(2, 12345);

    expect(tree.nextLowerPair(-2)).toEqual(undefined);
    expect(tree.nextLowerPair(-1)).toEqual([-2, 123]);
    expect(tree.nextLowerPair(0)).toEqual([-2, 123]);
    expect(tree.nextLowerKey(0)).toBe(-2);
    expect(tree.nextHigherPair(-1)).toEqual([0, 1234]);
    expect(tree.nextHigherPair(0)).toEqual([2, 12345]);
    expect(tree.nextHigherKey(0)).toBe(2);
    expect(tree.nextHigherPair(1)).toEqual([2, 12345]);
    expect(tree.nextHigherPair(2)).toEqual(undefined);
    expect(tree.nextLowerPair(undefined)).toEqual([2, 12345]);
    expect(tree.nextHigherPair(undefined)).toEqual([-2, 123]);

    for (
      let i = -10;
      i <= 300;
      i++ // embiggen the tree
    )
      tree.set(i, i * 2);
    expect(tree.nextLowerPair(-1)).toEqual([-2, -4]);
    expect(tree.nextLowerPair(0)).toEqual([-1, -2]);
    expect(tree.nextHigherPair(-1)).toEqual([0, 0]);
    expect(tree.nextHigherPair(0)).toEqual([1, 2]);

    expect(tree.nextLowerPair(undefined)).toEqual([300, 600]);
    expect(tree.nextHigherPair(undefined)).toEqual([-10, -20]);
  });

  test('Regression test for invalid default comparator causing malformed trees', () => {
    const key = '24e26f0b-3c1a-47f8-a7a1-e8461ddb69ce6';
    const tree = new BTree<string, {}>(undefined, undefined, maxNodeSize);
    // The defaultComparator was not transitive for these inputs due to comparing numeric strings to each other numerically,
    // but lexically when compared to non-numeric strings. This resulted in keys not being orderable, and the tree behaving incorrectly.
    const inputs: [string, {}][] = [
      [key, {}],
      ['0', {}],
      ['1', {}],
      ['2', {}],
      ['3', {}],
      ['4', {}],
      ['Cheese', {}],
      ['10', {}],
      ['11', {}],
      ['12', {}],
      ['13', {}],
      ['15', {}],
      ['16', {}],
    ];

    for (const [id, node] of inputs) {
      expect(tree.set(id, node)).toBeTruthy();
      tree.checkValid();
      expect(tree.get(key)).not.toBeUndefined();
    }
    expect(tree.get(key)).not.toBeUndefined();
  });
}
