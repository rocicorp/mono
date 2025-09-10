import sqlite3 from '@rocicorp/zero-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import {expect, test, vi} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {runSQLiteStoreTests} from '../sqlite-store-test-util.ts';
import {
  clearAllNamedExpoSQLiteStoresForTesting,
  expoSQLiteStoreProvider,
  type ExpoSQLiteStoreOptions,
} from './store.ts';

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

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

function getNewStore(name: string) {
  const provider = expoSQLiteStoreProvider(defaultStoreOptions);
  return provider.create(name);
}

function createStore(name: string, opts?: ExpoSQLiteStoreOptions) {
  const provider = expoSQLiteStoreProvider(opts);
  return provider.create(name);
}

// Run all shared SQLite store tests
runSQLiteStoreTests<ExpoSQLiteStoreOptions>({
  storeName: 'ExpoSQLiteStore',
  createStoreProvider: expoSQLiteStoreProvider,
  clearAllNamedStores: clearAllNamedExpoSQLiteStoresForTesting,
  createStoreWithDefaults: getNewStore,
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
