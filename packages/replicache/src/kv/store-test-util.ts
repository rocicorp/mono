import {expect} from '@esm-bundle/chai';
import type {FrozenJSONValue, ReadonlyJSONValue} from '../json.js';
import {withRead, withWrite} from '../with-transactions.js';
import type {Read, Store, Write} from './store.js';

class TestStore implements Store {
  private readonly _store: Store;

  constructor(store: Store) {
    this._store = store;
  }

  get closed(): boolean {
    return this._store.closed;
  }

  read(): Promise<Read> {
    return this._store.read();
  }

  write(): Promise<Write> {
    return this._store.write();
  }

  close(): Promise<void> {
    return this._store.close();
  }

  async put(key: string, value: FrozenJSONValue): Promise<void> {
    await withWrite(this, async write => {
      await write.put(key, value);
      await write.commit();
    });
  }

  async del(key: string): Promise<void> {
    await withWrite(this, async write => {
      await write.del(key);
      await write.commit();
    });
  }

  has(key: string): Promise<boolean> {
    return withRead(this, read => read.has(key));
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    return withRead(this, read => read.get(key));
  }
}

export function runAll(
  name: string,
  newStore: () => Promise<Store> | Store,
): void {
  const funcs = [
    store,
    simpleCommit,
    del,
    readOnlyCommit,
    readOnlyRollback,
    simpleRollback,
    readTransaction,
    writeTransaction,
    isolation,
  ];

  for (const f of funcs) {
    test(`store ${f.name} (${name})`, async () => {
      const s = new TestStore(await newStore());
      await f(s);
    });
  }
}

async function simpleCommit(store: TestStore): Promise<void> {
  // Start a write transaction, and put a value on it.
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.false;
    await wt.put('bar', 'baz');
    expect(await wt.get('bar')).to.deep.equal('baz');
    await wt.put('other', 'abc');
    expect(await wt.get('other')).to.deep.equal('abc');
    await wt.commit();
  });

  // Verify that the write was effective.
  await withRead(store, async rt => {
    expect(await rt.has('bar')).to.be.true;
    expect(await rt.get('bar')).to.deep.equal('baz');
    expect(await rt.has('other')).to.be.true;
    expect(await rt.get('other')).to.deep.equal('abc');
  });
}

async function del(store: TestStore): Promise<void> {
  // Start a write transaction, and put a value on it.
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.false;
    await wt.put('bar', 'baz');
    await wt.put('other', 'abc');
    await wt.commit();
  });

  // Delete
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.true;
    await wt.del('bar');
    expect(await wt.has('bar')).to.be.false;
    await wt.del('other');
    expect(await wt.has('other')).to.be.false;
    await wt.commit();
  });

  // Verify that the delete was effective.
  await withRead(store, async rt => {
    expect(await rt.has('bar')).to.be.false;
    expect(await rt.get('bar')).to.be.undefined;
    expect(await rt.has('other')).to.be.false;
    expect(await rt.get('other')).to.be.undefined;
  });
}

async function readOnlyCommit(store: TestStore): Promise<void> {
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.false;
    await wt.commit();
  });
}

async function readOnlyRollback(store: TestStore): Promise<void> {
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.false;
  });
}

async function simpleRollback(store: TestStore): Promise<void> {
  // Start a write transaction and put a value, then abort.
  await withWrite(store, async wt => {
    await wt.put('bar', 'baz');
    await wt.put('other', 'abc');
    // no commit, implicit rollback
  });

  await withRead(store, async rt => {
    expect(await rt.has('bar')).to.be.false;
    expect(await rt.has('other')).to.be.false;
  });
}

async function store(store: TestStore): Promise<void> {
  // Test put/has/get, which use read() and write() for one-shot txs.
  expect(await store.has('foo')).to.be.false;
  expect(await store.get('foo')).to.be.undefined;

  await store.put('foo', 'bar');
  expect(await store.has('foo')).to.be.true;
  expect(await store.get('foo')).to.deep.equal('bar');

  await store.put('foo', 'baz');
  expect(await store.has('foo')).to.be.true;
  expect(await store.get('foo')).to.deep.equal('baz');

  expect(await store.has('baz')).to.be.false;
  expect(await store.get('baz')).to.be.undefined;
  await store.put('baz', 'bat');
  expect(await store.has('baz')).to.be.true;
  expect(await store.get('baz')).to.deep.equal('bat');
}

async function readTransaction(store: TestStore): Promise<void> {
  await store.put('k1', 'v1');

  await withRead(store, async rt => {
    expect(await rt.has('k1')).to.be.true;
    expect('v1').to.deep.equal(await rt.get('k1'));
  });
}

async function writeTransaction(store: TestStore): Promise<void> {
  await store.put('k1', 'v1');
  await store.put('k2', 'v2');

  // Test put then commit.
  await withWrite(store, async wt => {
    expect(await wt.has('k1')).to.be.true;
    expect(await wt.has('k2')).to.be.true;
    await wt.put('k1', 'overwrite');
    await wt.commit();
  });
  expect(await store.get('k1')).to.deep.equal('overwrite');
  expect(await store.get('k2')).to.deep.equal('v2');

  // Test put then rollback.
  await withWrite(store, async wt => {
    await wt.put('k1', 'should be rolled back');
  });
  expect(await store.get('k1')).to.deep.equal('overwrite');

  // Test del then commit.
  await withWrite(store, async wt => {
    await wt.del('k1');
    expect(await wt.has('k1')).to.be.false;
    await wt.commit();
  });
  expect(await store.has('k1')).to.be.false;

  // Test del then rollback.
  expect(await store.has('k2')).to.be.true;
  await withWrite(store, async wt => {
    await wt.del('k2');
    expect(await wt.has('k2')).to.be.false;
  });
  expect(await store.has('k2')).to.be.true;

  // Test overwrite multiple times then commit.
  await withWrite(store, async wt => {
    await wt.put('k2', 'overwrite');
    await wt.del('k2');
    await wt.put('k2', 'final');
    await wt.commit();
  });
  expect(await store.get('k2')).to.deep.equal('final');

  // Test Read interface on Write.
  await withWrite(store, async wt => {
    await wt.put('k2', 'new value');
    expect(await wt.has('k2')).to.be.true;
    expect(await wt.get('k2')).to.deep.equal('new value');
    await wt.commit();
  });
}

async function isolation(store: Store): Promise<void> {
  // Things we want to test:
  // - Multiple readers can read at the same time.
  // - A write transaction must wait until all readers are done.
  // - Only one writer can write at a time.
  // - If a writer is waiting no new readers or writers can access the store.

  const log: string[] = [];

  const r1 = store.read().then(async r => {
    log.push('r1 start');
    expect(await r.has('k1')).to.be.false;
    log.push('r1 touched store');
    expect(await r.get('k1')).to.be.undefined;
    log.push('r1 end');
    r.release();
  });
  const r2 = store.read().then(async r => {
    log.push('r2 start');
    expect(await r.has('k1')).to.be.false;
    log.push('r2 touched store');
    expect(await r.get('k1')).to.be.undefined;
    log.push('r2 end');
    r.release();
  });
  const w1 = store.write().then(async w => {
    log.push('w1 start');
    expect(await w.has('k1')).to.be.false;
    log.push('w1 touched store');
    expect(await w.get('k1')).to.be.undefined;

    log.push('w1 mutated store');
    await w.put('k1', 'w1');

    log.push('w1 end');
    await w.commit();
    w.release();
  });
  const w2 = store.write().then(async w => {
    log.push('w2 start');
    expect(await w.has('k1')).to.be.true;
    log.push('w2 touched store');
    expect(await w.get('k1')).to.deep.equal('w1');

    log.push('w2 mutated store');
    await w.put('k1', 'w2');

    log.push('w2 end');
    await w.commit();
    w.release();
  });
  const r3 = store.read().then(async r => {
    log.push('r3 start');
    expect(await r.has('k1')).to.be.true;
    log.push('r3 touched store');
    expect(await r.get('k1')).to.deep.equal('w2');
    log.push('r3 end');
    r.release();
  });

  await Promise.all([r1, r2, w1, w2, r3]);

  // We allow some lee-way for the ordering as long as the actual reads
  // (get/has) and writes (put/del) are ordered correctly. This is to allow
  // IndexedDB which does not queue on transaction creation but on the actual
  // requests done in the transaction.

  function assertOrder(...items: string[]) {
    let last = -1;
    for (const item of items) {
      expect(log.indexOf(item)).to.be.greaterThan(last);
      last = log.indexOf(item);
    }
  }

  assertOrder('r1 start', 'r1 touched store', 'r1 end');
  assertOrder('r2 start', 'r2 touched store', 'r2 end');
  assertOrder('w1 start', 'w1 touched store', 'w1 mutated store', 'w1 end');
  assertOrder('w2 start', 'w2 touched store', 'w2 mutated store', 'w2 end');
  assertOrder('r3 start', 'r3 touched store', 'r3 end');

  assertOrder('r1 start', 'r2 start', 'w1 start', 'w2 start', 'r3 start');

  assertOrder('r1 end', 'w1 end', 'w2 end', 'r3 end');
  assertOrder('r2 end', 'w1 end', 'w2 end', 'r3 end');

  assertOrder(
    'r1 touched store',
    'r2 touched store',
    'w1 touched store',
    'w2 touched store',
    'r3 touched store',
  );

  assertOrder(
    'r1 touched store',
    'r2 touched store',
    'w1 mutated store',
    'w2 mutated store',
    'r3 touched store',
  );

  assertOrder('r1 end', 'w1 touched store');
  assertOrder('r2 end', 'w1 touched store');

  assertOrder('w1 end', 'w2 touched store');

  assertOrder('w2 end', 'r3 touched store');
}
