import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteBindParams,
} from 'expo-sqlite';
import {
  SQLiteDatabaseManager,
  SQLiteStore,
  type SQLiteDatabaseManagerOptions,
} from '../kv/sqlite-store.ts';
import type {StoreProvider} from '../kv/store.ts';

export const expoDbManagerInstance = new SQLiteDatabaseManager({
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

export function expoSQLiteStoreProvider(
  opts?: Partial<Omit<SQLiteDatabaseManagerOptions, 'journalMode'>>,
): StoreProvider {
  return {
    create: (name: string) =>
      new SQLiteStore(name, expoDbManagerInstance, {
        // we default to 3 read connections for mobile devices
        readPoolSize: 3,
        busyTimeout: 200,
        synchronous: 'NORMAL',
        readUncommitted: false,
        ...opts,
        // we override the journal mode to undefined because
        // setting it to WAL causes hanging COMMITs on Expo
        journalMode: undefined,
      }),
    drop: (name: string) => {
      expoDbManagerInstance.destroy(name);

      return Promise.resolve();
    },
  };
}
