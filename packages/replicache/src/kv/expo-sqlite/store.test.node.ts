import sqlite3 from '@rocicorp/zero-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import {expect, test, vi} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {clearAllNamedStoresForTesting} from '../sqlite-store.ts';
import {expoSQLiteStoreProvider, type ExpoSQLiteStoreOptions} from './store.ts';

//Mock the expo-sqlite module with Node SQLite implementation
vi.mock('expo-sqlite', () => ({
  openDatabaseSync: (name: string) => {
    // Add expo_ prefix to match the actual store implementation
    const prefixedName = `expo_${name}`;
    const filename = path.resolve(__dirname, `${prefixedName}.db`);

    // Register the store name for cleanup (not the filename)
    registerCreatedFile(name);

    // Create a new database connection - SQLite handles file locking and concurrency
    const db = sqlite3(filename);

    return {
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
          executeForRawResultAsync: (params: unknown[] = []) => {
            try {
              const isSelectQuery = /^\s*select/i.test(sql);
              if (isSelectQuery) {
                const result = stmt.all(...params);
                return Promise.resolve({
                  getFirstAsync: () =>
                    Promise.resolve(
                      result.length > 0
                        ? Object.values(result[0] as Record<string, unknown>)
                        : null,
                    ),
                });
              }
              stmt.run(...params);
              return Promise.resolve({
                getFirstAsync: () => Promise.resolve(null),
              });
            } catch (error) {
              return Promise.reject(error);
            }
          },
          executeSync: (params: unknown[] = []) => {
            const isSelectQuery = /^\s*select/i.test(sql);
            if (isSelectQuery) {
              return stmt.all(...params);
            }
            return stmt.run(...params);
          },
          finalizeSync: () => {
            // SQLite3 statements don't need explicit finalization
          },
        };
      },
      closeSync: () => {
        // SQLite handles this properly, just close the connection
        db.close();
      },
    };
  },
  deleteDatabaseSync: (name: string) => {
    // Add expo_ prefix to match the actual store implementation
    const prefixedName = `expo_${name}`;
    const filename = path.resolve(__dirname, `${prefixedName}.db`);

    // Simply delete the file if it exists - SQLite handles any open connections
    if (fs.existsSync(filename)) {
      fs.unlinkSync(filename);
    }
  },
}));

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

function createStore(name: string, opts?: ExpoSQLiteStoreOptions) {
  const provider = expoSQLiteStoreProvider(opts);
  return provider.create(name);
}

// Run all shared SQLite store tests
runSQLiteStoreTests<ExpoSQLiteStoreOptions>({
  storeName: 'ExpoSQLiteStore',
  createStoreProvider: expoSQLiteStoreProvider,
  clearAllNamedStores: clearAllNamedStoresForTesting,
  createStoreWithDefaults: createStore,
  defaultStoreOptions,
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
    expect(await rt.get('config-test')).toBe('configured-value');
  });

  await storeWithOptions.close();
});
