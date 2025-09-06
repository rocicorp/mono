import {resolver} from '@rocicorp/resolver';
import sqlite3 from '@rocicorp/zero-sqlite3';
import fs from 'fs';
import path from 'path';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import {runAll} from '../kv/store-test-util.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';

//Mock the expo-sqlite module with Node SQLite implementation
vi.mock('expo-sqlite', () => {
  // Map of database names to their actual sqlite3 instances
  // This ensures that multiple stores with the same name share the same database
  const databases = new Map<string, ReturnType<typeof sqlite3>>();
  const openConnections = new Map<string, number>();

  return {
    openDatabaseSync: (name: string) => {
      const filename = path.resolve(__dirname, `${name}.db`);

      // Get or create the actual database instance
      let db: ReturnType<typeof sqlite3>;
      if (databases.has(name)) {
        db = databases.get(name)!;
      } else {
        db = sqlite3(filename);
        databases.set(name, db);
      }

      // Track connections to this database
      const currentConnections = openConnections.get(name) || 0;
      openConnections.set(name, currentConnections + 1);

      const dbWrapper = {
        execSync: (sql: string) => db.exec(sql),
        prepareSync: (sql: string) => {
          const stmt = db.prepare(sql);
          return {
            executeAsync: (params: unknown[] = []) => {
              try {
                let result: unknown[];
                const isSelectQuery = /^\s*select/i.test(sql);
                if (isSelectQuery) {
                  result = params.length ? stmt.all(...params) : stmt.all();
                } else {
                  stmt.run(...params);
                  result = [];
                }
                return Promise.resolve({
                  getAllAsync: () => Promise.resolve(result),
                });
              } catch (error) {
                return Promise.reject(error);
              }
            },
            finalizeSync: () => {
              // SQLite3 statements don't need explicit finalization
            },
          };
        },
        closeSync: () => {
          const connections = openConnections.get(name) || 1;
          if (connections <= 1) {
            // Last connection - actually close the database
            db.close();
            databases.delete(name);
            openConnections.delete(name);
          } else {
            // Still has other connections
            openConnections.set(name, connections - 1);
          }
        },
      };

      return dbWrapper;
    },
    deleteDatabaseSync: (name: string) => {
      // Close any open connections first
      if (databases.has(name)) {
        const db = databases.get(name)!;
        db.close();
        databases.delete(name);
        openConnections.delete(name);
      }

      const filename = path.resolve(__dirname, `${name}.db`);
      if (fs.existsSync(filename)) {
        fs.unlinkSync(filename);
      }
    },
  };
});

// Import the store after mocking
import {
  clearAllNamedExpoSQLiteStoresForTesting,
  ExpoSQLiteStore,
  expoSQLiteStoreProvider,
} from './store.ts';

function createStore(
  name: string,
  opts?: Parameters<typeof expoSQLiteStoreProvider>[0],
) {
  return new ExpoSQLiteStore(name, opts);
}

const getNewStore = (name: string) =>
  createStore(name, {
    busyTimeout: 200,
    journalMode: 'WAL',
    synchronous: 'NORMAL',
    readUncommitted: false,
  });

// Cleanup function to remove test databases
function cleanupTestDatabases() {
  // Clear shared store instances
  clearAllNamedExpoSQLiteStoresForTesting();

  const testDir = __dirname;
  if (fs.existsSync(testDir)) {
    const files = fs.readdirSync(testDir);
    for (const file of files) {
      if (
        file.endsWith('.db') ||
        file.endsWith('.db-wal') ||
        file.endsWith('.db-shm')
      ) {
        try {
          fs.unlinkSync(path.join(testDir, file));
        } catch (error) {
          // Ignore cleanup errors
        }
      }
    }
  }
}

beforeEach(() => {
  cleanupTestDatabases();
});

afterEach(() => {
  cleanupTestDatabases();
});

// Run all standard store tests with isolated databases
runAll('ExpoSQLiteStore', () => getNewStore('test'));

// Additional tests specific to ExpoSQLiteStore

test('shared read transaction behavior', async () => {
  const store = getNewStore('shared-read-test');

  // Put some data first
  await withWrite(store, async wt => {
    await wt.put('key1', 'value1');
    await wt.put('key2', 'value2');
  });

  const readPromises: Promise<void>[] = [];
  const readOrder: number[] = [];

  // Create multiple concurrent reads
  for (let i = 0; i < 4; i++) {
    const readPromise = withRead(store, async rt => {
      readOrder.push(i);
      expect(await rt.get('key1')).equal('value1');
      expect(await rt.get('key2')).equal('value2');
      await sleep(10); // Small delay to test concurrency
    });
    readPromises.push(readPromise);
  }

  await Promise.all(readPromises);
  expect(readOrder).toHaveLength(4);

  await store.close();
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

  // Start multiple reads - these should wait for write to complete
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

test('store provider drop functionality', async () => {
  const provider = expoSQLiteStoreProvider();
  const storeName = 'drop-test';

  const store1 = provider.create(storeName);

  await withWrite(store1, async wt => {
    await wt.put('persistent-key', 'persistent-value');
  });

  await store1.close();

  // Drop the database
  await provider.drop(storeName);

  // Create new store with same name - data should be gone
  const store2 = provider.create(storeName);
  await withRead(store2, async rt => {
    expect(await rt.get('persistent-key')).toBe(undefined);
  });

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

test('json value freezing', async () => {
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
    }
  });

  await store.close();
});

test('different configuration options', async () => {
  // Test with different configuration options
  const storeWithOptions = createStore('pragma-test', {
    busyTimeout: 500,
    journalMode: 'DELETE',
    synchronous: 'FULL',
    readUncommitted: true,
  });

  await withWrite(storeWithOptions, async wt => {
    await wt.put('config-test', 'configured-value');
  });

  await withRead(storeWithOptions, async rt => {
    expect(await rt.get('config-test')).equal('configured-value');
  });

  await storeWithOptions.close();
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
    }
  });

  await store.close();
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
    expect(await rt.has('non-existent')).toBe(false);
  });

  await store.close();
});

test('closed reflects status after close', async () => {
  const store = getNewStore('flag');
  expect(store.closed).toBe(false);
  await store.close();
  expect(store.closed).toBe(true);
});

test('closing a store multiple times', async () => {
  const store = getNewStore('double');
  await store.close();
  // Second close should be a no-op and must not throw.
  await store.close();
  expect(store.closed).toBe(true);
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

test('multiple stores with same name share data', async () => {
  const store1 = getNewStore('test');
  await withWrite(store1, async wt => {
    await wt.put('shared', 'data');
  });

  const store2 = getNewStore('test');
  await withRead(store2, async rt => {
    expect(await rt.get('shared')).equal('data');
  });

  await store1.close();
  await store2.close();
});

test('multiple stores with different names have separate data', async () => {
  const store1 = getNewStore('test1');
  await withWrite(store1, async wt => {
    await wt.put('key', 'value1');
  });

  const store2 = getNewStore('test2');
  await withRead(store2, async rt => {
    expect(await rt.get('key')).toBe(undefined);
  });

  await store1.close();
  await store2.close();
});

test('error handling for closed transactions', async () => {
  const store = getNewStore('error-test');

  // Test read transaction error handling
  const readTx = await store.read();
  readTx.release();

  await expect(readTx.has('key')).rejects.toThrow('Read transaction is closed');
  await expect(readTx.get('key')).rejects.toThrow('Read transaction is closed');

  // Test write transaction error handling
  const writeTx = await store.write();
  writeTx.release();

  await expect(writeTx.has('key')).rejects.toThrow(
    'Write transaction is closed',
  );
  await expect(writeTx.get('key')).rejects.toThrow(
    'Write transaction is closed',
  );
  await expect(writeTx.put('key', 'value')).rejects.toThrow(
    'Write transaction is closed',
  );
  await expect(writeTx.del('key')).rejects.toThrow(
    'Write transaction is closed',
  );
  await expect(writeTx.commit()).rejects.toThrow('Write transaction is closed');

  await store.close();
});

test('error handling for closed store', async () => {
  const store = getNewStore('closed-store-test');
  await store.close();

  await expect(store.read()).rejects.toThrow('Store is closed');
  await expect(store.write()).rejects.toThrow('Store is closed');
});
