import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteBindParams,
} from 'expo-sqlite';
import {
  SQLiteDatabaseManager,
  type SQLiteDatabase,
} from '../kv/sqlite-store.ts';

export const expoDbManagerInstance = new SQLiteDatabaseManager({
  open: fileName => {
    const db = openDatabaseSync(fileName);

    const genericDb: SQLiteDatabase = {
      close: () => db.closeSync(),
      destroy: () => {
        db.closeSync();
        deleteDatabaseSync(fileName);
      },
      prepare: (sql: string) => {
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

    return genericDb;
  },
});
