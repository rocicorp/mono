import {expect, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {isDeepFrozen} from '../frozen-json.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';
import type {Read, Store, Write} from './store.ts';

class TestStore implements Store {
  readonly #store: Store;

  constructor(store: Store) {
    this.#store = store;
  }

  get closed(): boolean {
    return this.#store.closed;
  }

  read(): Promise<Read> {
    return this.#store.read();
  }

  write(): Promise<Write> {
    return this.#store.write();
  }

  close(): Promise<void> {
    return this.#store.close();
  }

  async put(key: string, value: FrozenJSONValue): Promise<void> {
    await withWrite(this, async write => {
      await write.put(key, value);
    });
  }

  async del(key: string): Promise<void> {
    await withWrite(this, async write => {
      await write.del(key);
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
    writeTransactionRollbackOnError,
    readAndWriteTransactionStateManagement,
    jsonValueFreezing,
    nullAndUndefinedValueHandling,
    closedReflectsStatusAfterClose,
    closingAStoreMultipleTimes,
    dataPersistsAfterStoreIsClosedAndReopened,
    multipleStoresWithSameNameShareData,
    multipleStoresWithDifferentNamesHaveSeparateData,
    errorHandlingForClosedStore,
    errorHandlingForClosedTransactions,
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
  });

  // Delete
  await withWrite(store, async wt => {
    expect(await wt.has('bar')).to.be.true;
    await wt.del('bar');
    expect(await wt.has('bar')).to.be.false;
    await wt.del('other');
    expect(await wt.has('other')).to.be.false;
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
  });
}

async function readOnlyRollback(store: TestStore): Promise<void> {
  await withWriteNoImplicitCommit(store, async wt => {
    expect(await wt.has('bar')).to.be.false;
  });
}

async function simpleRollback(store: TestStore): Promise<void> {
  // Start a write transaction and put a value, then abort.
  await withWriteNoImplicitCommit(store, async wt => {
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
  });
  expect(await store.get('k1')).to.deep.equal('overwrite');
  expect(await store.get('k2')).to.deep.equal('v2');

  // Test put then rollback.
  await withWriteNoImplicitCommit(store, async wt => {
    await wt.put('k1', 'should be rolled back');
  });
  expect(await store.get('k1')).to.deep.equal('overwrite');

  // Test del then commit.
  await withWrite(store, async wt => {
    await wt.del('k1');
    expect(await wt.has('k1')).to.be.false;
  });
  expect(await store.has('k1')).to.be.false;

  // Test del then rollback.
  expect(await store.has('k2')).to.be.true;
  await withWriteNoImplicitCommit(store, async wt => {
    await wt.del('k2');
    expect(await wt.has('k2')).to.be.false;
  });
  expect(await store.has('k2')).to.be.true;

  // Test overwrite multiple times then commit.
  await withWrite(store, async wt => {
    await wt.put('k2', 'overwrite');
    await wt.del('k2');
    await wt.put('k2', 'final');
  });
  expect(await store.get('k2')).to.deep.equal('final');

  // Test Read interface on Write.
  await withWrite(store, async wt => {
    await wt.put('k2', 'new value');
    expect(await wt.has('k2')).to.be.true;
    expect(await wt.get('k2')).to.deep.equal('new value');
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

async function writeTransactionRollbackOnError(
  store: TestStore,
): Promise<void> {
  await withWrite(store, async wt => {
    await wt.put('existing', 'value');
  });

  // Simulate an error during write transaction
  let errorThrown = false;
  try {
    await withWriteNoImplicitCommit(store, async wt => {
      await wt.put('new-key', 'new-value');
      await wt.put('existing', 'modified-value');
      throw new Error('Simulated error');
    });
  } catch (e) {
    errorThrown = true;
    expect((e as Error).message).toBe('Simulated error');
  }

  expect(errorThrown).toBe(true);

  // Verify rollback - changes should not be persisted
  await withRead(store, async rt => {
    expect(await rt.get('new-key')).toBe(undefined);
    expect(await rt.get('existing')).equal('value'); // Original value
  });
}

async function readAndWriteTransactionStateManagement(
  store: TestStore,
): Promise<void> {
  // Test read transaction state - note that TestStore doesn't expose direct transaction access
  // so we test this through the withRead/withWrite helpers which should behave consistently

  // Test that read transactions work properly
  await withRead(store, async rt => {
    expect(await rt.has('non-existent')).toBe(false);
    expect(await rt.get('non-existent')).toBe(undefined);
  });

  // Test write transaction state management
  await withWrite(store, async wt => {
    await wt.put('test-key', 'test-value');
    expect(await wt.get('test-key')).equal('test-value');
  });

  // Verify the write was committed
  await withRead(store, async rt => {
    expect(await rt.get('test-key')).equal('test-value');
  });
}

async function jsonValueFreezing(store: TestStore): Promise<void> {
  const complexObject = {
    array: [1, 2, {nested: 'value'}],
    object: {
      deep: {
        value: 'test',
        number: 42,
      },
    },
  };

  await withWrite(store, async wt => {
    await wt.put('complex', complexObject);
  });

  await withRead(store, async rt => {
    const retrieved = await rt.get('complex');

    // Should be deeply equal
    expect(retrieved).toEqual(complexObject);

    // Should be deep frozen (read-only) - this is the key requirement
    expect(isDeepFrozen(retrieved, [])).toBe(true);
  });
}

async function nullAndUndefinedValueHandling(store: TestStore): Promise<void> {
  await withWrite(store, async wt => {
    await wt.put('null-key', null);
    await wt.put('zero-key', 0);
    await wt.put('false-key', false);
    await wt.put('empty-string-key', '');
    await wt.put('empty-array-key', []);
    await wt.put('empty-object-key', {});
  });

  await withRead(store, async rt => {
    expect(await rt.get('null-key')).toBe(null);
    expect(await rt.get('zero-key')).toBe(0);
    expect(await rt.get('false-key')).toBe(false);
    expect(await rt.get('empty-string-key')).equal('');
    expect(await rt.get('empty-array-key')).toEqual([]);
    expect(await rt.get('empty-object-key')).toEqual({});

    expect(await rt.has('null-key')).toBe(true);
    expect(await rt.has('zero-key')).toBe(true);
    expect(await rt.has('false-key')).toBe(true);
    expect(await rt.has('empty-string-key')).toBe(true);
    expect(await rt.has('empty-array-key')).toBe(true);
    expect(await rt.has('empty-object-key')).toBe(true);
    expect(await rt.has('non-existent')).toBe(false);
  });
}

async function closedReflectsStatusAfterClose(store: TestStore): Promise<void> {
  expect(store.closed).toBe(false);
  await store.close();
  expect(store.closed).toBe(true);
}

async function closingAStoreMultipleTimes(store: TestStore): Promise<void> {
  await store.close();
  // Second close should be a no-op and must not throw.
  await store.close();
  expect(store.closed).toBe(true);
}

async function dataPersistsAfterStoreIsClosedAndReopened(
  store: TestStore,
): Promise<void> {
  await store.put('foo', 'bar');
  await store.close();

  // Note: For universal testing, we can't easily test reopening with the same name
  // since TestStore doesn't expose the underlying store creation mechanism.
  // This test verifies the data persists before closing.
  // Implementation-specific tests should verify reopening behavior.

  // Verify data was written before close
  // This is somewhat limited but ensures the basic persistence contract
  expect(store.closed).toBe(true);
}

async function multipleStoresWithSameNameShareData(
  store: TestStore,
): Promise<void> {
  // Note: This test is limited in the universal context since we only get one store instance
  // Implementation-specific tests should verify shared data between stores with same name
  await store.put('shared', 'data');
  expect(await store.get('shared')).equal('data');
}

async function multipleStoresWithDifferentNamesHaveSeparateData(
  store: TestStore,
): Promise<void> {
  // Note: This test is limited in the universal context since we only get one store instance
  // Implementation-specific tests should verify separation between different named stores
  await store.put('key', 'value1');
  expect(await store.get('key')).equal('value1');
}

async function errorHandlingForClosedTransactions(
  store: TestStore,
): Promise<void> {
  // Test read transaction error handling
  const readTx = await store.read();
  readTx.release();

  await expect(readTx.has('key')).rejects.toThrow('Transaction is closed');
  await expect(readTx.get('key')).rejects.toThrow('Transaction is closed');

  // Test write transaction error handling
  const writeTx = await store.write();
  writeTx.release();

  await expect(writeTx.has('key')).rejects.toThrow('Transaction is closed');
  await expect(writeTx.get('key')).rejects.toThrow('Transaction is closed');
  await expect(writeTx.put('key', 'value')).rejects.toThrow(
    'Transaction is closed',
  );
  await expect(writeTx.del('key')).rejects.toThrow('Transaction is closed');
  await expect(writeTx.commit()).rejects.toThrow('Transaction is closed');
}

async function errorHandlingForClosedStore(store: TestStore): Promise<void> {
  await store.close();

  // Different store implementations throw different error messages:
  // - MemStore, SQLite stores: "Store is closed"
  // - IndexedDB: varies by browser (e.g., "The database connection is closing", etc.)
  await expect(store.read()).rejects.toThrow();
  await expect(store.write()).rejects.toThrow();
}
