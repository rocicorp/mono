import sqlite3 from '@rocicorp/zero-sqlite3';
import fs from 'node:fs';
import {expect, test, vi} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {clearAllNamedStoresForTesting} from '../sqlite-store.ts';
import {bunSQLiteStoreProvider, type BunSQLiteStoreOptions} from './store.ts';

// Mock the bun:sqlite module with Node SQLite implementation
vi.mock('bun:sqlite', () => ({
  Database: {
    open: (name: string) => {
      // Register the store name for cleanup (not the filename)
      registerCreatedFile(name);

      // Create a new database connection - SQLite handles file locking and concurrency
      const db = sqlite3(name);

      // Create the entry table if it doesn't exist (simulating setupDatabase used by expo/op-sqlite)
      db.exec(`
        CREATE TABLE IF NOT EXISTS entry (
          key TEXT PRIMARY KEY, 
          value TEXT NOT NULL
        ) WITHOUT ROWID
      `);

      return {
        run: (sql: string) => db.exec(sql),
        prepare: (sql: string) => {
          const stmt = db.prepare(sql);

          // Determine query type for proper result handling
          const isHasQuery =
            /^\s*SELECT\s+1\s+FROM\s+entry\s+WHERE\s+key\s*=\s*\?/i.test(sql);
          const isGetQuery =
            /^\s*SELECT\s+value\s+FROM\s+entry\s+WHERE\s+key\s*=\s*\?/i.test(
              sql,
            );

          return {
            values: (params: string[] = []) => {
              if (isHasQuery) {
                // For has() queries: SELECT 1 FROM entry WHERE key = ? LIMIT 1
                const result = stmt.all(...params);
                // Return array of arrays format: [[1]] if key exists, [] if not
                return result.length > 0 ? [[1]] : [];
              } else if (isGetQuery) {
                // For get() queries: SELECT value FROM entry WHERE key = ?
                const result = stmt.all(...params);
                // Return array of arrays format: [["json_string"]] if key exists, [] if not
                return result.length > 0
                  ? [[(result[0] as {value: string}).value]]
                  : [];
              }
              // For other queries, just run them
              stmt.run(...params);
              return [];
            },
            run: (params: unknown[] = []) => {
              if (isHasQuery || isGetQuery) {
                // SELECT queries should return results
                return stmt.all(...params);
              }
              // INSERT/UPDATE/DELETE queries
              return stmt.run(...params);
            },
            finalize: () => {
              // SQLite3 statements don't need explicit finalization
            },
          };
        },
        close: () => {
          // SQLite handles this properly, just close the connection
          db.close();
        },
        destroy: () => {
          // Close the database and delete the file
          db.close();
          if (fs.existsSync(name)) {
            fs.unlinkSync(name);
          }
        },
      };
    },
  },
}));

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

function createStore(name: string, opts?: BunSQLiteStoreOptions) {
  const provider = bunSQLiteStoreProvider(opts);
  return provider.create(name);
}

// Run all shared SQLite store tests
runSQLiteStoreTests<BunSQLiteStoreOptions>({
  storeName: 'BunSQLiteStore',
  createStoreProvider: bunSQLiteStoreProvider,
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
