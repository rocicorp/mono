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
import {opSQLiteStoreProvider, type OpSQLiteStoreOptions} from './store.ts';

// Mock the @op-engineering/op-sqlite module with Node SQLite implementation
vi.mock('@op-engineering/op-sqlite', () => {
  // Map of database names to their actual sqlite3 instances
  // This ensures that multiple stores with the same name share the same database
  const databases = new Map<string, ReturnType<typeof sqlite3>>();
  const openConnections = new Map<string, number>();

  const clearAllDatabases = () => {
    for (const db of databases.values()) {
      db.close();
    }
    databases.clear();
    openConnections.clear();
  };

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

      // Get or create the actual database instance
      let db: ReturnType<typeof sqlite3>;
      if (databases.has(prefixedName)) {
        db = databases.get(prefixedName)!;
      } else {
        db = sqlite3(filename);
        databases.set(prefixedName, db);
      }

      // Track connections to this database
      const currentConnections = openConnections.get(prefixedName) || 0;
      openConnections.set(prefixedName, currentConnections + 1);

      return {
        // eslint-disable-next-line require-await
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
          const connections = openConnections.get(prefixedName) || 1;
          if (connections <= 1) {
            // Last connection - actually close the database
            db.close();
            databases.delete(prefixedName);
            openConnections.delete(prefixedName);
          } else {
            // Still has other connections
            openConnections.set(prefixedName, connections - 1);
          }
        },
        delete: () => {
          // Close any open connections first
          if (databases.has(prefixedName)) {
            const db = databases.get(prefixedName)!;
            db.close();
            databases.delete(prefixedName);
            openConnections.delete(prefixedName);
          }

          const filename = path.resolve(__dirname, `${prefixedName}.db`);
          if (fs.existsSync(filename)) {
            fs.unlinkSync(filename);
          }
        },
      };
    },
    // Internal function for testing - clear all databases
    clearAllDatabasesForTesting: clearAllDatabases,
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
