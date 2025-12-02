import {bench, describe} from 'vitest';
import {BTreeSet} from './btree-set.ts';

// Generate test data
function generateSortedNumbers(n: number): number[] {
  const result: number[] = [];
  for (let i = 0; i < n; i++) {
    result.push(i);
  }
  return result;
}

function generateRandomNumbers(n: number, seed: number = 42): number[] {
  // Simple LCG for reproducible random numbers
  let state = seed;
  const random = () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state;
  };

  const result: number[] = [];
  for (let i = 0; i < n; i++) {
    result.push(random());
  }
  return result;
}

function generateSortedRows(n: number): {id: number; name: string}[] {
  const result: {id: number; name: string}[] = [];
  for (let i = 0; i < n; i++) {
    result.push({id: i, name: `name-${i}`});
  }
  return result;
}

const comparator = (a: number, b: number) => a - b;
const rowComparator = (
  a: {id: number; name: string},
  b: {id: number; name: string},
) => a.id - b.id;

describe('BTreeSet bulk add benchmarks', () => {
  const sizes = [1_000, 10_000, 100_000];

  for (const size of sizes) {
    const sortedData = generateSortedNumbers(size);
    const randomData = generateRandomNumbers(size);
    const sortedRandomData = [...randomData].sort(comparator);

    describe(`${size.toLocaleString()} items`, () => {
      bench('constructor with sorted iterator (pre-sorted)', () => {
        new BTreeSet(comparator, sortedData.values());
      });

      bench('add one-by-one (pre-sorted input)', () => {
        const tree = new BTreeSet<number>(comparator);
        for (const item of sortedData) {
          tree.add(item);
        }
      });

      bench(
        'constructor with sorted iterator (from random, pre-sorted)',
        () => {
          new BTreeSet(comparator, sortedRandomData.values());
        },
      );

      bench('add one-by-one (random order)', () => {
        const tree = new BTreeSet<number>(comparator);
        for (const item of randomData) {
          tree.add(item);
        }
      });
    });
  }

  describe('Row objects (100,000 items)', () => {
    const sortedRows = generateSortedRows(100_000);

    bench('constructor with sorted iterator (rows)', () => {
      new BTreeSet(rowComparator, sortedRows.values());
    });

    bench('add one-by-one rows', () => {
      const tree = new BTreeSet<{id: number; name: string}>(rowComparator);
      for (const row of sortedRows) {
        tree.add(row);
      }
    });
  });

  describe('Building secondary index from existing tree (simulates MemorySource index creation)', () => {
    // This simulates what MemorySource does: it has a primary index and needs
    // to create a secondary index sorted differently.

    const primaryData = generateSortedRows(50_000);
    const primaryTree = new BTreeSet(rowComparator, primaryData.values());

    // Secondary index sorted by name
    const nameComparator = (
      a: {id: number; name: string},
      b: {id: number; name: string},
    ) => a.name.localeCompare(b.name);

    bench('old approach: iterate + add one-by-one', () => {
      const secondaryTree = new BTreeSet<{id: number; name: string}>(
        nameComparator,
      );
      for (const row of primaryTree) {
        secondaryTree.add(row);
      }
    });

    bench(
      'new approach: collect + sort + constructor with sorted iterator',
      () => {
        const rows = [...primaryTree];
        rows.sort(nameComparator);
        new BTreeSet(nameComparator, rows.values());
      },
    );
  });
});
