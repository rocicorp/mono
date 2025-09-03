import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeEach, expect, test} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {
  SQLiteStore,
  type SQLiteDatabaseManagerOptions,
} from './sqlite-store.ts';
import {runAll} from './store-test-util.ts';

const sqlite3DatabaseManager = getTestSQLiteDatabaseManager();

function createStore(name: string, opts: SQLiteDatabaseManagerOptions) {
  return new SQLiteStore(name, sqlite3DatabaseManager, opts);
}

const getNewStore = (name: string) =>
  createStore(name, {
    readPoolSize: 2,
    journalMode: 'WAL',
    synchronous: 'NORMAL',
    readUncommitted: false,
    busyTimeout: 200,
  });

runAll('SQLiteStore', () => getNewStore('test'));

beforeEach(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

afterAll(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

// Additional comprehensive tests for SQLiteStore implementation

test('read pool round-robin behavior', async () => {
  const store = getNewStore('round-robin-test');

  // Put some data first
  await withWrite(store, async wt => {
    await wt.put('key1', 'value1');
    await wt.put('key2', 'value2');
  });

  const readPromises: Promise<void>[] = [];
  const readOrder: number[] = [];

  // Create multiple concurrent reads to test round-robin
  for (let i = 0; i < 6; i++) {
    const readPromise = withRead(store, async rt => {
      readOrder.push(i);
      expect(await rt.get('key1')).equal('value1');
      // Add small delay to ensure concurrent execution
      await sleep(10);
    });
    readPromises.push(readPromise);
  }

  await Promise.all(readPromises);
  expect(readOrder).toHaveLength(6);

  await store.close();
});

test('read pool size validation', () => {
  expect(() => {
    createStore('invalid-pool', {
      readPoolSize: 1, // Invalid: must be > 1
      journalMode: 'WAL',
      synchronous: 'NORMAL',
      readUncommitted: false,
      busyTimeout: 200,
    });
  }).toThrow('readPoolSize must be greater than 1');
});

test('concurrent reads with write blocking', async () => {
  const store = getNewStore('concurrent-test');

  await withWrite(store, async wt => {
    await wt.put('foo', 'initial');
  });

  const {promise: writeStarted, resolve: resolveWriteStarted} = resolver();
  const {promise: writeCanComplete, resolve: resolveWriteCanComplete} =
    resolver();

  let writeCompleted = false;
  const readResults: string[] = [];

  // Start a long-running write transaction
  const writePromise = withWrite(store, async wt => {
    resolveWriteStarted();
    await wt.put('foo', 'updated-by-write');
    await writeCanComplete;
    writeCompleted = true;
  });

  // Wait for write to start
  await writeStarted;

  // Start multiple reads - these should either see the old value or wait for write to complete
  const readPromises = [1, 2].map(async _i => {
    const result = await withRead(store, rt => rt.get('foo'));
    readResults.push(result as string);
    return result;
  });

  // Give a moment for reads to start
  await sleep(20);

  // Complete the write
  resolveWriteCanComplete();
  await writePromise;

  // Wait for all reads to complete
  await Promise.all(readPromises);

  // After write completes, subsequent reads should see the updated value
  await withRead(store, async rt => {
    expect(await rt.get('foo')).equal('updated-by-write');
  });

  expect(writeCompleted).toBe(true);
  expect(readResults).toHaveLength(2);

  await store.close();
});

test('write exclusivity - only one write at a time', async () => {
  const store = getNewStore('write-exclusivity-test');

  await withWrite(store, async wt => {
    await wt.put('counter', 0);
  });

  const {promise: write1Started, resolve: resolveWrite1Started} = resolver();
  const {promise: write1CanComplete, resolve: resolveWrite1CanComplete} =
    resolver();

  let write1Completed = false;
  let write2Started = false;

  // Start first write transaction
  const write1Promise = withWrite(store, async wt => {
    resolveWrite1Started();
    const current = (await wt.get('counter')) as number;
    await write1CanComplete;
    await wt.put('counter', current + 1);
    write1Completed = true;
  });

  // Wait for first write to start
  await write1Started;

  // Start second write transaction - this should wait for first to complete
  const write2Promise = withWrite(store, async wt => {
    write2Started = true;
    const current = (await wt.get('counter')) as number;
    await wt.put('counter', current + 10);
  });

  // Give second write a chance to start
  await sleep(20);

  // Second write should not have started yet because first write holds the lock
  expect(write2Started).toBe(false);
  expect(write1Completed).toBe(false);

  // Complete first write
  resolveWrite1CanComplete();
  await write1Promise;

  expect(write1Completed).toBe(true);

  // Now second write should complete
  await write2Promise;
  expect(write2Started).toBe(true);

  // Final value should be 11 (0 + 1 + 10)
  await withRead(store, async rt => {
    expect(await rt.get('counter')).equal(11);
  });

  await store.close();
});

test('write transaction rollback on error', async () => {
  const store = getNewStore('rollback-test');

  await withWrite(store, async wt => {
    await wt.put('existing', 'value');
  });

  // Simulate an error during write transaction
  let errorThrown = false;
  try {
    await withWriteNoImplicitCommit(store, async wt => {
      await wt.put('new-key', 'new-value');
      await wt.put('existing', 'updated-value');

      // Verify changes are visible within transaction
      expect(await wt.get('new-key')).equal('new-value');
      expect(await wt.get('existing')).equal('updated-value');

      // Don't commit, simulate error
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

  await store.close();
});

test('safe filename generation', async () => {
  // Test that special characters in store names are handled safely
  const specialNames = [
    'test-with-dashes',
    'test.with.dots',
    'test with spaces',
    'test@with#special$chars%',
    'test/with/slashes',
    'test\\with\\backslashes',
  ];

  const stores = specialNames.map(name => getNewStore(name));

  // Each store should work independently
  for (let i = 0; i < stores.length; i++) {
    const store = stores[i];
    const testValue = `value-${i}`;

    await withWrite(store, async wt => {
      await wt.put('test-key', testValue);
    });

    await withRead(store, async rt => {
      expect(await rt.get('test-key')).equal(testValue);
    });

    await store.close();
  }
});

test('database manager destroy functionality', async () => {
  const storeName = 'destroy-test';
  const store1 = getNewStore(storeName);

  await withWrite(store1, async wt => {
    await wt.put('persistent-key', 'persistent-value');
  });

  await store1.close();

  // Destroy the database
  sqlite3DatabaseManager.destroy(storeName);

  // Create new store with same name - data should be gone
  const store2 = getNewStore(storeName);
  await withRead(store2, async rt => {
    expect(await rt.get('persistent-key')).toBe(undefined);
  });

  await store2.close();
});

test('database manager close functionality', async () => {
  const storeName = 'close-test';
  const store = getNewStore(storeName);

  await withWrite(store, async wt => {
    await wt.put('test-key', 'test-value');
  });

  // Close through database manager
  sqlite3DatabaseManager.close(storeName);

  // Store should still report correct closed status
  await store.close();
  expect(store.closed).toBe(true);
});

test('multiple database instances with shared underlying file', async () => {
  const storeName = 'shared-test';

  // Create first instance
  const store1 = getNewStore(storeName);
  await withWrite(store1, async wt => {
    await wt.put('shared-key', 'value-from-store1');
  });

  // Create second instance with same name (shares file)
  const store2 = getNewStore(storeName);
  await withRead(store2, async rt => {
    expect(await rt.get('shared-key')).equal('value-from-store1');
  });

  // Write from second instance
  await withWrite(store2, async wt => {
    await wt.put('shared-key', 'value-from-store2');
  });

  // Read from first instance should see updated value
  await withRead(store1, async rt => {
    expect(await rt.get('shared-key')).equal('value-from-store2');
  });

  await store1.close();
  await store2.close();
});

test('read and write transaction state management', async () => {
  const store = getNewStore('state-test');

  // Test read transaction state
  const readTx = await store.read();
  expect(readTx.closed).toBe(false);

  await readTx.has('non-existent');
  expect(readTx.closed).toBe(false);

  readTx.release();
  expect(readTx.closed).toBe(true);

  // Test write transaction state
  const writeTx = await store.write();
  expect(writeTx.closed).toBe(false);

  await writeTx.put('test-key', 'test-value');
  expect(writeTx.closed).toBe(false);

  await writeTx.commit();
  expect(writeTx.closed).toBe(false);

  writeTx.release();
  expect(writeTx.closed).toBe(true);

  await store.close();
});

test('json value freezing and deep copying', async () => {
  const store = getNewStore('json-test');

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

    // Should be deeply equal but not the same reference
    expect(retrieved).toEqual(complexObject);
    expect(retrieved).not.toBe(complexObject);

    // Should be frozen (read-only)
    expect(Object.isFrozen(retrieved)).toBe(true);
    if (retrieved && typeof retrieved === 'object' && 'array' in retrieved) {
      expect(Object.isFrozen(retrieved.array)).toBe(true);
      expect(Object.isFrozen((retrieved.array as unknown[])[2])).toBe(true);
    }
  });

  await store.close();
});

test('database pragma configuration', async () => {
  // Test with different configuration options
  const storeWithOptions = createStore('pragma-test', {
    readPoolSize: 3,
    journalMode: 'DELETE', // Different from default WAL
    synchronous: 'FULL', // Different from default NORMAL
    readUncommitted: true, // Different from default false
    busyTimeout: 500, // Different from default 200
  });

  await withWrite(storeWithOptions, async wt => {
    await wt.put('config-test', 'configured-value');
  });

  await withRead(storeWithOptions, async rt => {
    expect(await rt.get('config-test')).equal('configured-value');
  });

  await storeWithOptions.close();
});

test('error handling in database operations', async () => {
  const store = getNewStore('error-test');

  // Test error in read transaction
  const readTx = await store.read();
  let readErrorThrown = false;

  try {
    // This should work normally
    await readTx.get('valid-key');
  } catch (e) {
    readErrorThrown = true;
  }

  expect(readErrorThrown).toBe(false);
  readTx.release();

  // Test error in write transaction
  const writeTx = await store.write();
  let writeErrorThrown = false;

  try {
    // This should work normally
    await writeTx.put('valid-key', 'valid-value');
    await writeTx.commit();
  } catch (e) {
    writeErrorThrown = true;
  }

  expect(writeErrorThrown).toBe(false);
  writeTx.release();

  await store.close();
});

test('large data handling', async () => {
  const store = getNewStore('large-data-test');

  // Create a large object to test serialization/deserialization
  const largeArray = new Array(1000).fill(0).map((_, i) => ({
    id: i,
    data: `item-${i}`,
    nested: {
      value: i * 2,
      description: `Description for item ${i}`.repeat(10),
    },
  }));

  await withWrite(store, async wt => {
    await wt.put('large-data', largeArray);
  });

  await withRead(store, async rt => {
    const retrieved = await rt.get('large-data');
    expect(retrieved).toEqual(largeArray);
    expect(Array.isArray(retrieved)).toBe(true);
    if (Array.isArray(retrieved)) {
      expect(retrieved).toHaveLength(1000);
      expect(retrieved[0]).toEqual(largeArray[0]);
      expect(retrieved[999]).toEqual(largeArray[999]);
    }
  });

  await store.close();
});

test('sequential write operations with different values', async () => {
  const store = getNewStore('sequential-test');

  const operations = [
    {key: 'key1', value: 'value1'},
    {key: 'key2', value: 'value2'},
    {key: 'key1', value: 'updated-value1'}, // Update existing
    {key: 'key3', value: {complex: 'object', array: [1, 2, 3]}},
  ];

  // Perform operations sequentially
  for (const {key, value} of operations) {
    await withWrite(store, async wt => {
      await wt.put(key, value);
    });
  }

  // Verify final state
  await withRead(store, async rt => {
    expect(await rt.get('key1')).equal('updated-value1');
    expect(await rt.get('key2')).equal('value2');
    expect(await rt.get('key3')).toEqual({complex: 'object', array: [1, 2, 3]});
  });

  await store.close();
});

test('delete operations and has checks', async () => {
  const store = getNewStore('delete-test');

  // Setup initial data
  await withWrite(store, async wt => {
    await wt.put('key1', 'value1');
    await wt.put('key2', 'value2');
    await wt.put('key3', 'value3');
  });

  // Verify initial state
  await withRead(store, async rt => {
    expect(await rt.has('key1')).toBe(true);
    expect(await rt.has('key2')).toBe(true);
    expect(await rt.has('key3')).toBe(true);
    expect(await rt.has('non-existent')).toBe(false);
  });

  // Delete one key
  await withWrite(store, async wt => {
    await wt.del('key2');
  });

  // Verify deletion
  await withRead(store, async rt => {
    expect(await rt.has('key1')).toBe(true);
    expect(await rt.has('key2')).toBe(false);
    expect(await rt.has('key3')).toBe(true);

    expect(await rt.get('key1')).equal('value1');
    expect(await rt.get('key2')).toBe(undefined);
    expect(await rt.get('key3')).equal('value3');
  });

  // Delete non-existent key (should not error)
  await withWrite(store, async wt => {
    await wt.del('non-existent');
  });

  await store.close();
});

test('prepared statement lifecycle and connection reuse', async () => {
  const storeName = 'lifecycle-test';
  const store1 = getNewStore(storeName);

  // Perform some operations to initialize prepared statements
  await withWrite(store1, async wt => {
    await wt.put('key1', 'value1');
    await wt.put('key2', 'value2');
  });

  await withRead(store1, async rt => {
    expect(await rt.get('key1')).equal('value1');
    expect(await rt.has('key2')).toBe(true);
  });

  await store1.close();

  // Create new store with same name - should reuse database file
  const store2 = getNewStore(storeName);

  await withRead(store2, async rt => {
    expect(await rt.get('key1')).equal('value1');
    expect(await rt.get('key2')).equal('value2');
  });

  // Add more data
  await withWrite(store2, async wt => {
    await wt.del('key1');
    await wt.put('key3', 'value3');
  });

  await withRead(store2, async rt => {
    expect(await rt.get('key1')).toBe(undefined);
    expect(await rt.get('key2')).equal('value2');
    expect(await rt.get('key3')).equal('value3');
  });

  await store2.close();
});

test('null and undefined value handling', async () => {
  const store = getNewStore('null-test');

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

    expect(await rt.has('non-existent-key')).toBe(false);
    expect(await rt.get('non-existent-key')).toBe(undefined);
  });

  await store.close();
});

test('transaction commit without explicit writes', async () => {
  const store = getNewStore('empty-commit-test');

  // Empty read transaction
  await withRead(store, async rt => {
    expect(await rt.get('non-existent')).toBe(undefined);
    expect(await rt.has('non-existent')).toBe(false);
  });

  // Empty write transaction (commit with no operations)
  await withWrite(store, async _wt => {
    // Do nothing, just commit
  });

  // Verify store is still functional
  await withWrite(store, async wt => {
    await wt.put('test-key', 'test-value');
  });

  await withRead(store, async rt => {
    expect(await rt.get('test-key')).equal('test-value');
  });

  await store.close();
});

test('creating multiple with same name shares data after close', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  await store.close();

  const store2 = getNewStore('test');
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });

  await store2.close();
});

test('creating multiple with different name gets unique data', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const store2 = getNewStore('test2');
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal(undefined);
  });
});

test('multiple reads at the same time', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise, resolve} = resolver();

  let readCounter = 0;
  const p1 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    await promise;
    expect(readCounter).equal(1);
    readCounter++;
  });
  const p2 = withRead(store, async rt => {
    expect(readCounter).equal(0);
    readCounter++;
    expect(await rt.get('foo')).equal('bar');
    resolve();
  });
  expect(readCounter).equal(0);
  await Promise.all([p1, p2]);
  expect(readCounter).equal(2);
});

test('multiple reads at the same time with first committing early', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise: promise1, resolve: resolve1} = resolver();
  const {promise: promise2, resolve: resolve2} = resolver();

  // this runs COMMIT
  const p1 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    await promise1;
    resolve2();
  });
  // runs after the previous tx
  const p2 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    resolve1();
  });
  const p3 = withRead(store, async rt => {
    await promise2;
    expect(await rt.get('foo')).equal('bar');
  });

  await Promise.all([p1, p2, p3]);
});

test('read before write', async () => {
  const store = getNewStore('test');

  const {promise, resolve} = resolver();

  const readP = withRead(store, async rt => {
    await promise;
    expect(await rt.get('foo')).equal(undefined);
  });

  const writeP = withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  resolve();

  await Promise.all([readP, writeP]);
});

test('single write at a time', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise: promise1, resolve: resolve1} = resolver();
  const {promise: promise2, resolve: resolve2} = resolver();

  let writeCounter = 0;
  const p1 = withWrite(store, async wt => {
    await promise1;
    expect(await wt.get('foo')).equal('bar');
    expect(writeCounter).equal(0);
    writeCounter++;
  });
  const p2 = withWrite(store, async wt => {
    await promise2;
    expect(writeCounter).equal(1);
    expect(await wt.get('foo')).equal('bar');
    writeCounter++;
  });

  // Doesn't matter that resolve2 is called first, because p2 is waiting on p1.
  resolve2();
  await sleep(10);
  resolve1();

  await Promise.all([p1, p2]);
  expect(writeCounter).equal(2);
});

test('closed reflects status after close', async () => {
  const store = getNewStore('flag');
  expect(store.closed).to.be.false;
  await store.close();
  expect(store.closed).to.be.true;
});

test('closing a store multiple times', async () => {
  const store = getNewStore('double');
  await store.close();
  // Second close should be a no-op and must not throw.
  await store.close();
  expect(store.closed).to.be.true;
});

test('data persists after store is closed and reopened', async () => {
  const name = 'persist';
  const store1 = getNewStore(name);
  await withWrite(store1, async wt => {
    await wt.put('foo', 'bar');
  });
  await store1.close();

  const store2 = getNewStore(name);
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });
  await store2.close();
});
