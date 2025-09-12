import {afterEach, beforeEach, expect, test} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {runAll} from './store-test-util.ts';
import type {Store, StoreProvider} from './store.ts';

export interface SQLiteStoreTestConfig<TOptions = unknown> {
  /** Name to identify this SQLite implementation in test descriptions */
  storeName: string;
  /** Function that returns a store provider */
  createStoreProvider: (opts?: TOptions) => StoreProvider;
  /** Function to clear all named stores for testing */
  clearAllNamedStores: () => void;
  /** Function to create a store with default test options */
  createStoreWithDefaults: (name: string) => Store;
  /** Default options for creating stores in tests */
  defaultStoreOptions?: TOptions;
}

// Global set to track all created store names during tests
const createdStoreNames = new Set<string>();

/**
 * Registers a store name that was created during testing so it can be cleaned up later
 */
export function registerCreatedFile(storeName: string): void {
  createdStoreNames.add(storeName);
}

/**
 * Shared cleanup function for SQLite store tests.
 * Uses the store provider's drop method to properly clean up databases.
 */
function createCleanupFunction(
  clearAllNamedStores: () => void,
  storeProvider: StoreProvider,
) {
  return function cleanupTestDatabases() {
    clearAllNamedStores();

    // Clean up tracked store names using the store provider's drop method
    const cleanupPromises = Array.from(createdStoreNames).map(
      async storeName => {
        try {
          await storeProvider.drop(storeName);
        } catch (error) {
          // Ignore cleanup errors
        }
      },
    );

    // Wait for all cleanup operations to complete
    void Promise.all(cleanupPromises).finally(() => {
      createdStoreNames.clear();
    });
  };
}

/**
 * Shared test suite for SQLite store implementations.
 * Runs all common tests that should work across all SQLite stores.
 */
export function runSQLiteStoreTests<TOptions = unknown>(
  config: SQLiteStoreTestConfig<TOptions>,
) {
  const {
    storeName,
    createStoreProvider,
    clearAllNamedStores,
    createStoreWithDefaults,
    defaultStoreOptions,
  } = config;

  // Create cleanup function
  const cleanupTestDatabases = createCleanupFunction(
    clearAllNamedStores,
    createStoreProvider(defaultStoreOptions),
  );

  beforeEach(() => {
    cleanupTestDatabases();
  });

  afterEach(() => {
    cleanupTestDatabases();
  });

  // Counter to ensure unique store names for each test
  let storeCounter = Date.now();

  // Run all standard store tests with unique names
  runAll(storeName, () => createStoreWithDefaults(`test-${++storeCounter}`));

  // SQLite-specific tests
  test('shared read transaction behavior', async () => {
    const store = createStoreWithDefaults(`shared-read-test-${++storeCounter}`);

    // Put some data first
    await withWrite(store, async wt => {
      await wt.put('key1', 'value1');
      await wt.put('key2', 'value2');
    });

    // Start multiple read transactions concurrently
    const readPromises = [];
    for (let i = 0; i < 5; i++) {
      readPromises.push(
        withRead(store, async rt => {
          const value1 = await rt.get('key1');
          const value2 = await rt.get('key2');
          return {value1, value2};
        }),
      );
    }

    const results = await Promise.all(readPromises);

    // All reads should see the same consistent data
    for (const result of results) {
      expect(result.value1).toBe('value1');
      expect(result.value2).toBe('value2');
    }

    await store.close();
  });

  test('concurrent reads with write blocking', async () => {
    const store = createStoreWithDefaults(`concurrent-test-${++storeCounter}`);

    // Put initial data
    await withWrite(store, async wt => {
      await wt.put('key', 'initial');
    });

    const results: string[] = [];

    // Start multiple concurrent operations
    const operations = [
      // Long-running read transaction
      withRead(store, async rt => {
        const value = await rt.get('key');
        await sleep(50); // Simulate slow read
        results.push(`read: ${value}`);
        return value;
      }),

      // Write transaction that should wait
      withWrite(store, async wt => {
        await wt.put('key', 'updated');
        results.push('write: updated');
      }),

      // Another read that should see initial value
      withRead(store, async rt => {
        const value = await rt.get('key');
        results.push(`read: ${value}`);
        return value;
      }),
    ];

    await Promise.all(operations);

    // The exact order depends on SQLite implementation, but we should see all operations
    expect(results).toHaveLength(3);
    expect(results.filter(r => r.includes('read'))).toHaveLength(2);
    expect(results.filter(r => r.includes('write'))).toHaveLength(1);

    await store.close();
  });

  test('write exclusivity - only one write at a time', async () => {
    const store = createStoreWithDefaults(
      `write-exclusivity-test-${++storeCounter}`,
    );

    const writeOrder: number[] = [];
    const writes = [];

    // Start multiple write transactions
    for (let i = 0; i < 3; i++) {
      writes.push(
        withWrite(store, async wt => {
          writeOrder.push(i);
          await wt.put(`key${i}`, `value${i}`);
          await sleep(10); // Small delay to ensure ordering effects are visible
        }),
      );
    }

    await Promise.all(writes);

    // All writes should have completed
    expect(writeOrder).toHaveLength(3);
    expect(writeOrder).toEqual(expect.arrayContaining([0, 1, 2]));

    // Verify all data was written
    await withRead(store, async rt => {
      expect(await rt.get('key0')).toBe('value0');
      expect(await rt.get('key1')).toBe('value1');
      expect(await rt.get('key2')).toBe('value2');
    });

    await store.close();
  });

  test('safe filename generation', async () => {
    // Test that special characters in store names are handled safely
    const specialNames = [
      `test/with/slashes-${++storeCounter}`,
      `test with spaces-${++storeCounter}`,
      `test-with-dashes-${++storeCounter}`,
      `test_with_underscores-${++storeCounter}`,
      `test.with.dots-${++storeCounter}`,
    ];

    for (const name of specialNames) {
      const store = createStoreWithDefaults(name);
      await withWrite(store, async wt => {
        await wt.put('test-key', 'test-value');
      });

      await withRead(store, async rt => {
        expect(await rt.get('test-key')).toBe('test-value');
      });

      await store.close();
    }
  });

  test('store provider drop functionality', async () => {
    const provider = createStoreProvider(defaultStoreOptions);
    const storeName = `drop-test-${++storeCounter}`;

    const store1 = provider.create(storeName);
    registerCreatedFile(storeName);

    await withWrite(store1, async wt => {
      await wt.put('persistent-key', 'persistent-value');
    });

    await store1.close();

    // Drop the database
    await provider.drop(storeName);

    // Create new store with same name - data should be gone
    const store2 = provider.create(storeName);

    await withRead(store2, async rt => {
      expect(await rt.get('persistent-key')).toBeUndefined();
    });

    await store2.close();
  });

  test('read and write transaction state management', async () => {
    const store = createStoreWithDefaults(`state-test-${++storeCounter}`);

    // Test read transaction state
    const readTx = await store.read();
    expect(readTx.closed).toBe(false);

    await readTx.get('nonexistent');
    expect(readTx.closed).toBe(false);

    readTx.release();
    expect(readTx.closed).toBe(true);

    // Test write transaction state
    const writeTx = await store.write();
    expect(writeTx.closed).toBe(false);

    await writeTx.put('key', 'value');
    expect(writeTx.closed).toBe(false);

    await writeTx.commit();
    expect(writeTx.closed).toBe(false);

    writeTx.release();
    expect(writeTx.closed).toBe(true);

    await store.close();
  });

  test('different configuration options', async () => {
    // Test with different configuration options
    const storeName = `pragma-test-${++storeCounter}`;
    const storeWithOptions =
      createStoreProvider(defaultStoreOptions).create(storeName);
    registerCreatedFile(storeName);

    await withWrite(storeWithOptions, async wt => {
      await wt.put('config-test', 'configured-value');
    });

    await withRead(storeWithOptions, async rt => {
      expect(await rt.get('config-test')).toBe('configured-value');
    });

    await storeWithOptions.close();
  });
}
