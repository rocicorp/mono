import {openDatabaseAsync, deleteDatabaseAsync} from 'expo-sqlite';
import {
  getCreateSQLiteStore,
  SQLiteDatabaseManager,
  type GenericSQLiteDatabaseManager,
  type SQLDatabase,
} from '../../replicache/src/kv/sqlite-store.ts';
import type {StoreProvider} from '../../replicache/src/kv/store.ts';
import {ExpoSQLiteTransaction} from './transaction.ts';

const genericDatabase: GenericSQLiteDatabaseManager = {
  open: async (name: string) => {
    const db = await openDatabaseAsync(name);

    const genericDb: SQLDatabase = {
      transaction: () => new ExpoSQLiteTransaction(db),
      destroy: async () => {
        await db.closeAsync();
        await deleteDatabaseAsync(name);
      },
      close: () => db.closeAsync(),
    };

    return genericDb;
  },
};

const expoDbManagerInstance = new SQLiteDatabaseManager(genericDatabase);

export const createExpoSQLiteStore: StoreProvider = {
  create: getCreateSQLiteStore(expoDbManagerInstance),
  drop: (name: string) => expoDbManagerInstance.destroy(name),
};
