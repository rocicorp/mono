import {expect, test} from 'vitest';
import {
  clearBrowserOverrides,
  overrideBrowserGlobal,
} from '../../../shared/src/browser-env.ts';
import {dropMemStore, MemStore} from '../kv/mem-store.ts';
import type {StoreProvider} from '../kv/store.ts';
import {dropAllDatabases} from './collect-idb-databases.ts';
import {IDBDatabasesStore} from './idb-databases-store.ts';

test('dropAllDatabases without kvStore option throws when indexedDB unavailable', async () => {
  // Override indexedDB to throw if accessed - simulates non-browser environment
  overrideBrowserGlobal(
    'indexedDB',
    new Proxy({} as IDBFactory, {
      get() {
        throw new Error('indexedDB not available');
      },
    }),
  );

  try {
    // Call dropAllDatabases WITHOUT passing kvStore - should try to use IDB and fail
    await expect(dropAllDatabases()).rejects.toThrow('indexedDB not available');
  } finally {
    clearBrowserOverrides();
  }
});

test('dropAllDatabases with custom StoreProvider does not access indexedDB', async () => {
  // Override indexedDB to throw if accessed - this catches any accidental IDB usage
  overrideBrowserGlobal(
    'indexedDB',
    new Proxy({} as IDBFactory, {
      get() {
        throw new Error('indexedDB should not be accessed with custom kvStore');
      },
    }),
  );

  try {
    // Create a custom StoreProvider backed by MemStore
    const customProvider: StoreProvider = {
      create: (name: string) => new MemStore(name),
      drop: (name: string) => dropMemStore(name),
    };

    // Set up some databases using the custom provider
    const store = new IDBDatabasesStore(customProvider.create);
    await store.putDatabase({
      name: 'test-db-1',
      replicacheName: 'test1',
      replicacheFormatVersion: 1,
      schemaVersion: '1',
    });
    await store.putDatabase({
      name: 'test-db-2',
      replicacheName: 'test2',
      replicacheFormatVersion: 1,
      schemaVersion: '1',
    });

    // Verify databases were registered
    const dbs = await store.getDatabases();
    expect(Object.keys(dbs)).toHaveLength(2);

    // Now drop all databases - this should NOT access indexedDB
    const result = await dropAllDatabases({kvStore: customProvider});

    expect(result.errors).toHaveLength(0);
    expect(result.dropped).toHaveLength(2);
    expect(result.dropped).toContain('test-db-1');
    expect(result.dropped).toContain('test-db-2');

    // Verify databases were removed from registry
    const dbsAfter = await store.getDatabases();
    expect(Object.keys(dbsAfter)).toHaveLength(0);
  } finally {
    clearBrowserOverrides();
  }
});
