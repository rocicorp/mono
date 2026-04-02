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
