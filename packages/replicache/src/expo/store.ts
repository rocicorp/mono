import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteBindParams,
} from 'expo-sqlite';
import {
  safeFilename,
  SQLiteDatabaseManager,
  SQLiteStore,
  type SQLiteDatabaseManagerOptions,
} from '../kv/sqlite-store.ts';
import type {StoreProvider} from '../kv/store.ts';

export function expoSQLiteStoreProvider(
  opts?: Partial<SQLiteDatabaseManagerOptions>,
): StoreProvider {
  return {
    create: (name: string) => {
      const expoDbManagerInstance = new SQLiteDatabaseManager({
        open: fileName => {
          const db = openDatabaseSync(fileName);
          let closed = false;

          const close = () => {
            if (!closed) {
              db.closeSync();
              closed = true;
            }
          };

          return {
            close,
            destroy() {
              close();
              deleteDatabaseSync(fileName);
            },
            prepare(sql: string) {
              const stmt = db.prepareSync(sql);
              return {
                run: (...params: unknown[]): void => {
                  stmt.executeSync(params as SQLiteBindParams);
                },
                all: <T>(...params: unknown[]): T[] => {
                  const result = stmt.executeSync(params as SQLiteBindParams);
                  return result.getAllSync() as unknown as T[];
                },
                finalize: () => stmt.finalizeSync(),
              };
            },
          };
        },
      });

      return new SQLiteStore(name, expoDbManagerInstance, {
        // we default to 3 read connections for mobile devices
        readPoolSize: 3,
        busyTimeout: 200,
        synchronous: 'NORMAL',
        readUncommitted: false,
        journalMode: 'WAL',
        ...opts,
      });
    },

    drop: (name: string) => {
      // Note that we cannot drop a database if it is open.
      // All connections must be closed before calling drop.
      deleteDatabaseSync(safeFilename(name));

      return Promise.resolve();
    },
  };
}
