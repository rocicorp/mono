import fs from 'node:fs';
import path from 'node:path';
import sqlite3 from '@rocicorp/zero-sqlite3';
import {expect, test, vi} from 'vitest';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../../with-transactions.ts';
import {
  registerCreatedFile,
  runSQLiteStoreTests,
} from '../sqlite-store-test-util.ts';
import {clearAllNamedStoresForTesting, safeFilename} from '../sqlite-store.ts';
import {opSQLiteStoreProvider, type OpSQLiteStoreOptions} from './store.ts';

// Mock the @op-engineering/op-sqlite module with Node SQLite implementation
vi.mock('@op-engineering/op-sqlite', () => {
  const mockModule = {
    open: (options: {
      name: string;
      location?: string;
      encryptionKey?: string;
    }) => {
      const {name} = options;
      // Add op_ prefix to match the actual store implementation
      const prefixedName = `op_${name}`;
      const filename = path.resolve(__dirname, `${prefixedName}.db`);

      // Register the store name for cleanup (not the filename)
      registerCreatedFile(name);

      // Create a new database connection - SQLite handles file locking and concurrency
      const db = sqlite3(filename);

      return {
        // oxlint-disable-next-line require-await
        executeRaw: async (sql: string, params: string[] = []) => {
          const stmt = db.prepare(sql);
          const isSelectQuery = /^\s*select/i.test(sql);
          if (isSelectQuery) {
            const result = stmt.all(...params);
            // Convert to raw format (array of arrays)
            return Array.isArray(result)
              ? result.map(row => Object.values(row as Record<string, unknown>))
              : [];
          }
          stmt.run(...params);
          return [];
        },
        executeRawSync: (sql: string, params: string[] = []) => {
          const stmt = db.prepare(sql);
          const isSelectQuery = /^\s*select/i.test(sql);
          if (isSelectQuery) {
            const result = stmt.all(...params);
            // Convert to raw format (array of arrays)
            return Array.isArray(result)
              ? result.map(row => Object.values(row as Record<string, unknown>))
              : [];
          }
          stmt.run(...params);
          return [];
        },
        close: () => {
          // SQLite handles this properly, just close the connection
          db.close();
        },
        delete: () => {
          // Close the database and delete the file
          db.close();
          const filename = path.resolve(__dirname, `${prefixedName}.db`);
          if (fs.existsSync(filename)) {
            fs.unlinkSync(filename);
          }
        },
      };
    },
  };

  return mockModule;
});

const defaultStoreOptions = {
  busyTimeout: 200,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
} as const;

// Run all shared SQLite store tests
runSQLiteStoreTests<OpSQLiteStoreOptions>({
  storeName: 'OpSQLiteStore',
  createStoreProvider: opSQLiteStoreProvider,
  clearAllNamedStores: clearAllNamedStoresForTesting,
  createStoreWithDefaults: createStore,
  defaultStoreOptions,
});

function createStore(name: string, opts?: OpSQLiteStoreOptions) {
  const provider = opSQLiteStoreProvider(opts);
  return provider.create(name);
}

test('different configuration options', async () => {
  // Test with different configuration options
  const storeWithOptions = createStore('pragma-test', {
    busyTimeout: 500,
    journalMode: 'DELETE',
    synchronous: 'FULL',
    readUncommitted: true,
    location: 'default',
    encryptionKey: 'test-key',
  });

  await withWrite(storeWithOptions, async wt => {
    await wt.put('config-test', 'configured-value');
  });

  await withRead(storeWithOptions, async rt => {
    expect(await rt.get('config-test')).toBe('configured-value');
  });

  await storeWithOptions.close();
});

// OpSQLiteStore-specific tests
test('OpSQLite specific configuration options', async () => {
  // Test OpSQLite-specific configuration options
  const storeWithOptions = createStore('op-sqlite-pragma-test', {
    busyTimeout: 500,
    journalMode: 'DELETE',
    synchronous: 'FULL',
    readUncommitted: true,
    location: 'default',
    encryptionKey: 'test-key',
  });

  await withWrite(storeWithOptions, async wt => {
    await wt.put('config-test', 'configured-value');
  });

  await withRead(storeWithOptions, async rt => {
    expect(await rt.get('config-test')).equal('configured-value');
  });

  await storeWithOptions.close();
});

test('withWriteNoImplicitCommit reports both operation and rollback errors', async () => {
  const storeName = 'auto-rollback-op';
  const store = createStore(storeName);
  const filename = path.resolve(__dirname, `op_${safeFilename(storeName)}.db`);
  const triggerDb = sqlite3(filename);
  triggerDb.exec(`
    DROP TRIGGER IF EXISTS entry_auto_rollback_op;
    CREATE TRIGGER entry_auto_rollback_op
    BEFORE INSERT ON entry
    WHEN NEW.key = 'trigger-rollback'
    BEGIN
      SELECT RAISE(ROLLBACK, 'auto rollback put failure');
    END;
  `);
  triggerDb.close();

  const err = await withWriteNoImplicitCommit(store, async write => {
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
