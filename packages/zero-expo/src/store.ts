import * as SQLite from 'expo-sqlite';
import {
  getCreateReplicacheSQLiteKVStore,
  SQLiteDatabaseManager,
  type GenericSQLiteDatabaseManager,
  type SQLDatabase,
} from '../../replicache/src/kv/sqlite-store.ts';
import type {StoreProvider} from '../../replicache/src/kv/store.ts';
import {ExpoSQLiteTransaction} from './transaction.ts';

const genericDatabase: GenericSQLiteDatabaseManager = {
  open: async (name: string) => {
    const db = await SQLite.openDatabaseAsync(name);

    const genericDb: SQLDatabase = {
      transaction: () => new ExpoSQLiteTransaction(db),
      destroy: async () => {
        await db.closeAsync();
        await SQLite.deleteDatabaseAsync(name);
      },
      close: () => db.closeAsync(),
    };

    return genericDb;
  },
};

const expoDbManagerInstance = new SQLiteDatabaseManager(genericDatabase);

export const createExpoSQLiteStore: StoreProvider = {
  create: getCreateReplicacheSQLiteKVStore(expoDbManagerInstance),
  drop: (name: string) => expoDbManagerInstance.destroy(name),
};
