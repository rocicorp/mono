import fs from 'node:fs';
import path from 'node:path';
import sqlite3 from '@rocicorp/zero-sqlite3';
import {expect, test, vi} from 'vitest';
import {withRead, withWrite} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {clearAllNamedStoresForTesting, safeFilename} from '../sqlite-store.ts';
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
        const isSelectQuery = /^\s*select/i.test(sql);

        // Model expo-sqlite's stateful native statement. In expo-sqlite the
        // bindings and cursor position live on the single sqlite3_stmt, and
        // are shared by every result object created from it:
        // - `executeForRawResultAsync` is one native round trip that resets
        //   the statement, binds params and steps the first row (cached in JS).
        // - the result's `getAllAsync` is a second native round trip that
        //   steps the *remaining* rows of whatever the statement is currently
        //   bound to, with no check that the binding still belongs to this
        //   result.
        // The store must therefore not let two callers interleave these two
        // calls on the same statement.
        let rows: unknown[][] = [];
        let pos = 0;
        const bridgeHop = () => new Promise(resolve => setImmediate(resolve));

        const run = (params: unknown[]) => {
          if (isSelectQuery) {
            rows = stmt.raw(true).all(...params) as unknown[][];
          } else {
            stmt.run(...params);
            rows = [];
          }
          pos = 0;
          return rows.length > 0 ? rows[pos++] : null;
        };

        const makeResult = (firstRow: unknown[] | null, async: boolean) => {
          let stepped = false;
          const getAll = () => {
            if (stepped) {
              throw new Error('The SQLite cursor has been shifted');
            }
            stepped = true;
            if (firstRow === null) {
              return [];
            }
            const rest = rows.slice(pos);
            pos = rows.length;
            return [firstRow, ...rest];
          };
          return async
            ? {
                getFirstAsync: () => Promise.resolve(firstRow),
                getAllAsync: async () => {
                  if (firstRow !== null) {
                    await bridgeHop();
                  }
                  return getAll();
                },
              }
            : {
                getFirstSync: () => firstRow,
                getAllSync: getAll,
              };
        };

        return {
          executeAsync: async (params: unknown[] = []) => {
            await bridgeHop();
            return makeResult(run(params), true);
          },
          executeForRawResultAsync: async (params: unknown[] = []) => {
            await bridgeHop();
            return makeResult(run(params), true);
          },
          executeSync: (params: unknown[] = []) =>
            makeResult(run(params), false),
          executeForRawResultSync: (params: unknown[] = []) =>
            makeResult(run(params), false),
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

test('withWrite reports both operation and rollback errors', async () => {
  const storeName = 'auto-rollback-expo';
  const store = createStore(storeName);
  const filename = path.resolve(
    __dirname,
    `expo_${safeFilename(storeName)}.db`,
  );
  const triggerDb = sqlite3(filename);
  triggerDb.exec(`
    DROP TRIGGER IF EXISTS entry_auto_rollback_expo;
    CREATE TRIGGER entry_auto_rollback_expo
    BEFORE INSERT ON entry
    WHEN NEW.key = 'trigger-rollback'
    BEGIN
      SELECT RAISE(ROLLBACK, 'auto rollback put failure');
    END;
  `);
  triggerDb.close();

  const err = await withWrite(store, async write => {
    await write.put('trigger-rollback', 'value');
  }).then(
    () => undefined,
    e => e,
  );

  expect(err).toBeInstanceOf(Error);
  expect(String(err)).toContain('auto rollback put failure');
  expect(String(err)).toContain('cannot rollback');
  expect(String((err as Error).cause)).toContain('auto rollback put failure');

  await store.close();
});
