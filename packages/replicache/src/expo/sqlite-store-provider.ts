import {
  createSQLiteStore,
  type SQLiteDatabaseManagerOptions,
} from '../kv/sqlite-store.ts';
import type {StoreProvider} from '../kv/store.ts';
import {expoDbManagerInstance} from './db-manager-instance.ts';

export const expoSQLiteStoreProvider = (
  opts?: Partial<Omit<SQLiteDatabaseManagerOptions, 'journalMode'>>,
): StoreProvider => ({
  create: (name: string) =>
    createSQLiteStore(expoDbManagerInstance)(name, {
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
});
