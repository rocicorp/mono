import {afterEach, bench, suite} from 'vitest';
import BTree from '../../btree/b+tree.js';
import {Treap} from './treap.js';

suite('iteration', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const treap = new Treap<number>(comparator);

  for (let i = 0; i < 100_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    treap.add(v);
  }

  let result = 0;

  bench('btree', () => {
    result = 0;
    for (const v of btree.keys()) {
      result += v;
    }
    // console.log(result);
  });

  bench('treap', () => {
    result = 0;
    for (const v of treap.keys()) {
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
  const treap = new Treap<number>(comparator);

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    treap.add(v);
  }

  let result = 0;

  bench('btree', () => {
    for (let i = 0; i < 1_000; i++) {
      const v = Math.random();
      btree.set(v, undefined);
    }
    result = btree.size;
  });

  bench('treap', () => {
    result = 0;
    for (let i = 0; i < 1_000; i++) {
      const v = Math.random();
      treap.add(v);
    }
    result = treap.size;
  });

  afterEach(() => {
    console.log(result);
  });
});

suite('get', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const treap = new Treap<number>(comparator);

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    treap.add(v);
  }

  let result = 0;

  bench('btree', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btree.get(Math.random()) ? 1 : 0;
    }
  });

  bench('treap', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += treap.get(Math.random()) ? 1 : 0;
    }
  });

  afterEach(() => {
    console.log(result);
  });
});

suite('delete', () => {
  const comparator = (a: number, b: number) => a - b;
  const btree = new BTree<number, undefined>([], comparator);
  const treap = new Treap<number>(comparator);
  const values: number[] = [];

  for (let i = 0; i < 1_000; i++) {
    const v = Math.random();
    btree.set(v, undefined);
    treap.add(v);
    values.push(v);
  }

  let result = 0;

  bench('btree', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += btree.delete(values[Math.floor(Math.random() * values.length)])
        ? 1
        : 0;
    }
  });

  bench('treap', () => {
    result = 0;
    for (let i = 0; i < 500; i++) {
      result += treap.delete(values[Math.floor(Math.random() * values.length)])
        ? 1
        : 0;
    }
  });

  afterEach(() => {
    console.log(result);
  });
});
