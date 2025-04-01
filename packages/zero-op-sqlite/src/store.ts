import * as OPSQLite from '@op-engineering/op-sqlite';
import {
  getCreateSQLiteStore,
  SQLiteDatabaseManager,
  type GenericSQLiteDatabaseManager,
} from '../../replicache/src/kv/sqlite-store.ts';
import type {StoreProvider} from '../../replicache/src/kv/store.ts';
import {OPSQLiteTransaction} from './transaction.js';

const genericDatabase: GenericSQLiteDatabaseManager = {
  open: (name: string) => {
    const db = OPSQLite.open({name});
    return Promise.resolve({
      transaction: () => new OPSQLiteTransaction(db),
      destroy: () => Promise.resolve(db.delete()),
      close: () => Promise.resolve(db.close()),
    });
  },
};

const opSqliteDbManagerInstance = new SQLiteDatabaseManager(genericDatabase);

export const createOPSQLiteStore: StoreProvider = {
  create: getCreateSQLiteStore(opSqliteDbManagerInstance),
  drop: (name: string) => opSqliteDbManagerInstance.destroy(name),
};
