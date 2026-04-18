import {bench, describe, use} from './bench.ts';
import {BTreeSet} from './btree-set.ts';

const NUM_ENTRIES = 1000;

const numericComparator = (a: number, b: number) => a - b;

function makeNumericTree(size: number): BTreeSet<number> {
  const tree = new BTreeSet<number>(numericComparator);
  for (let i = 0; i < size; i++) {
    tree.add(i);
  }
  return tree;
}

const tree = makeNumericTree(NUM_ENTRIES);
const midKey = NUM_ENTRIES / 2;

describe('BTreeSet iterators', () => {
  bench('values() full scan', () => {
    let sum = 0;
    for (const v of tree.values()) {
      sum += v;
    }
    use(sum);
  });

  bench('valuesFrom() from mid', () => {
    let sum = 0;
    for (const v of tree.valuesFrom(midKey)) {
      sum += v;
    }
    use(sum);
  });

  bench('valuesReversed() full scan', () => {
    let sum = 0;
    for (const v of tree.valuesReversed()) {
      sum += v;
    }
    use(sum);
  });

  bench('valuesFromReversed() from mid', () => {
    let sum = 0;
    for (const v of tree.valuesFromReversed(midKey)) {
      sum += v;
    }
    use(sum);
  });

  bench('[Symbol.iterator]() full scan', () => {
    let sum = 0;
    for (const v of tree) {
      sum += v;
    }
    use(sum);
  });
});

// Isolate just the iterator step cost by calling next() directly,
// with no work in the "loop body" beyond consuming the value.
describe('BTreeSet iterator next() in isolation', () => {
  bench('forward iterator next()', () => {
    const iter = tree.values();
    let result = iter.next();
    let sum = 0;
    while (!result.done) {
      sum += result.value;
      result = iter.next();
    }
    use(sum);
  });

  bench('forward iterator next() from mid', () => {
    const iter = tree.valuesFrom(midKey);
    let result = iter.next();
    let sum = 0;
    while (!result.done) {
      sum += result.value;
      result = iter.next();
    }
    use(sum);
  });

  bench('reverse iterator next()', () => {
    const iter = tree.valuesReversed();
    let result = iter.next();
    let sum = 0;
    while (!result.done) {
      sum += result.value;
      result = iter.next();
    }
    use(sum);
  });

  bench('reverse iterator next() from mid', () => {
    const iter = tree.valuesFromReversed(midKey);
    let result = iter.next();
    let sum = 0;
    while (!result.done) {
      sum += result.value;
      result = iter.next();
    }
    use(sum);
  });
});

// Lookup and mutation benchmarks exercise internal node traversal,
// triggering the BNode vs BNodeInternal shape polymorphism.
describe('BTreeSet lookups', () => {
  bench('has() hit', () => {
    // Spread across the tree to exercise multiple internal nodes
    use(tree.has(100));
    use(tree.has(500));
    use(tree.has(900));
  });

  bench('has() miss', () => {
    use(tree.has(NUM_ENTRIES + 1));
    use(tree.has(NUM_ENTRIES + 2));
    use(tree.has(NUM_ENTRIES + 3));
  });

  bench('get() hit', () => {
    use(tree.get(100));
    use(tree.get(500));
    use(tree.get(900));
  });
});

describe('BTreeSet mutations', () => {
  bench('add() then delete() single key', function* () {
    // Setup: clone the tree so we don't corrupt the shared tree
    const t = tree.clone();
    yield () => {
      t.add(NUM_ENTRIES + 1);
      t.delete(NUM_ENTRIES + 1);
    };
  });

  bench('add() 100 sequential keys', function* () {
    yield () => {
      const t = new BTreeSet<number>(numericComparator);
      for (let i = 0; i < 100; i++) {
        t.add(i);
      }
      use(t.size);
    };
  });

  bench('fromSorted() 100 sequential keys', function* () {
    const keys = Array.from({length: 100}, (_, i) => i);
    yield () => {
      const t = BTreeSet.fromSorted(numericComparator, keys);
      use(t.size);
    };
  });

  bench('add() 1000 sequential keys', function* () {
    yield () => {
      const t = new BTreeSet<number>(numericComparator);
      for (let i = 0; i < NUM_ENTRIES; i++) {
        t.add(i);
      }
      use(t.size);
    };
  });

  bench('fromSorted() 1000 sequential keys', function* () {
    const keys = Array.from({length: NUM_ENTRIES}, (_, i) => i);
    yield () => {
      const t = BTreeSet.fromSorted(numericComparator, keys);
      use(t.size);
    };
  });

  // Simulates #getOrCreateIndex: source data is sorted by a different comparator,
  // so we must sort first then build. Compare against the prior add()-loop approach.
  bench('getOrCreateIndex pattern (old): add() loop after sort', function* () {
    const sourceKeys = Array.from({length: NUM_ENTRIES}, (_, i) => i);
    // Simulate source data arriving in reverse order (different comparator)
    const reverseComparator = (a: number, b: number) => b - a;
    yield () => {
      const sorted = sourceKeys.toSorted(reverseComparator);
      const t = new BTreeSet<number>(reverseComparator);
      for (const k of sorted) {
        t.add(k);
      }
      use(t.size);
    };
  });

  bench('getOrCreateIndex pattern (new): sort + fromSorted()', function* () {
    const sourceKeys = Array.from({length: NUM_ENTRIES}, (_, i) => i);
    const reverseComparator = (a: number, b: number) => b - a;
    yield () => {
      const sorted = sourceKeys.toSorted(reverseComparator);
      const t = BTreeSet.fromSorted(reverseComparator, sorted);
      use(t.size);
    };
  });
});
