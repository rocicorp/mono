import {afterEach, bench, suite} from 'vitest';
import BTree from '../../btree/b+tree.js';
import {BTreeSet} from './btree-set.js';

suite('iteration', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const btreeSet = new BTreeSet<number>(comparator);

  for (let i = 0; i < 100_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    btreeSet.add(v);
  }

  let result = 0;

  bench('BTree', () => {
    result = 0;
    for (const v of btree.keys()) {
      result += v;
    }
  });

  bench('BTreeSet', () => {
    result = 0;
    for (const v of btreeSet.keys()) {
      result += v;
    }
  });

  afterEach(() => {
    console.log(result);
  });
});

suite('insertion', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const sortedSet = new BTreeSet<number>(comparator);

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    sortedSet.add(v);
  }

  let result = 0;

  bench('BTree', () => {
    for (let i = 0; i < 1_000; i++) {
      const v = Math.random();
      btree.set(v, undefined);
    }
    result = btree.size;
  });

  bench('BTreeSet', () => {
    result = 0;
    for (let i = 0; i < 1_000; i++) {
      const v = Math.random();
      sortedSet.add(v);
    }
    result = sortedSet.size;
  });

  afterEach(() => {
    console.log(result);
  });
});

suite('get', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const btreeSet = new BTreeSet<number>(comparator);
  const values: number[] = [];

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    btreeSet.add(v);
    // 90% should hit, 10 might fail
    values.push(Math.random() > 0.9 ? v : Math.random());
  }

  let result = 0;

  bench('BTree', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btree.get(values[(Math.random() * values.length) | 0]) ? 1 : 0;
    }
  });

  bench('BTreeSet', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btreeSet.get(values[(Math.random() * values.length) | 0])
        ? 1
        : 0;
    }
  });

  afterEach(() => {
    console.log(result);
  });
});

suite('delete', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const btreeSet = new BTreeSet<number>(comparator);
  const values: number[] = [];

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    btreeSet.add(v);
    // 90% should hit, 10 might fail
    values.push(Math.random() > 0.9 ? v : Math.random());
  }

  let result = 0;

  bench('BTree', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btree.delete(values[(Math.random() * values.length) | 0])
        ? 1
        : 0;
    }
  });

  bench('BTreeSet', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btreeSet.delete(
        values[Math.floor(Math.random() * values.length)],
      )
        ? 1
        : 0;
    }
  });

  afterEach(() => {
    console.log(result);
  });
});
