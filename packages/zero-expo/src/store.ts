import * as SQLite from 'expo-sqlite';
import {
  getCreateReplicacheSQLiteKVStore,
  ReplicacheGenericSQLiteDatabaseManager,
  ReplicacheGenericSQLiteTransaction,
  type GenericSQLDatabase,
  type GenericDatabaseManager,
} from '../../replicache/src/kv/generic-store.ts';
import type {StoreProvider} from '../../replicache/src/kv/store.ts';

export class ReplicacheExpoSQLiteTransaction extends ReplicacheGenericSQLiteTransaction {
  #tx:
    | Parameters<
        Parameters<SQLite.SQLiteDatabase['withExclusiveTransactionAsync']>[0]
      >[0]
    | null = null;
  #transactionCommittedSubscriptions = new Set<() => void>();
  #txCommitted = false;
  #transactionEndedSubscriptions = new Set<{
    resolve: () => void;
    reject: () => void;
  }>();
  #txEnded = false;

  constructor(private readonly db: SQLite.SQLiteDatabase) {
    super();
  }

  // expo-sqlite doesn't support readonly
  public async start() {
    return await new Promise<void>((resolve, reject) => {
      let didResolve = false;
      try {
        this.db.withExclusiveTransactionAsync(async tx => {
          didResolve = true;
          this.#tx = tx;
          resolve();

          try {
            // expo-sqlite auto-commits our transaction when this callback ends.
            // Lets artificially keep it open until we commit.
            await this.#waitForTransactionCommitted();
            this.#setTransactionEnded(false);
          } catch {
            this.#setTransactionEnded(true);
          }
        });
      } catch {
        if (!didResolve) {
          reject(new Error('Did not resolve'));
        }
      }
    });
  }

  public async execute(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ) {
    const tx = this.#assertTransactionReady();

    const statement = await tx.prepareAsync(sqlStatement);
    let allRows: any;
    let result: any;
    try {
      result = await statement.executeAsync(...(args ?? []));
      allRows = await result.getAllAsync();
    } finally {
      await statement.finalizeAsync();
    }

    return {item: (idx: number) => allRows[idx], length: allRows.length};
  }

  public async commit() {
    // Transaction is committed automatically.
    this.#txCommitted = true;
    for (const resolver of this.#transactionCommittedSubscriptions) {
      resolver();
    }
    this.#transactionCommittedSubscriptions.clear();
  }

  public waitForTransactionEnded() {
    if (this.#txEnded) return;
    return new Promise<void>((resolve, reject) => {
      this.#transactionEndedSubscriptions.add({resolve, reject});
    });
  }

  #assertTransactionReady() {
    if (this.#tx === null) throw new Error('Transaction is not ready.');
    if (this.#txCommitted) throw new Error('Transaction already committed.');
    if (this.#txEnded) throw new Error('Transaction already ended.');
    return this.#tx;
  }

  #waitForTransactionCommitted() {
    if (this.#txCommitted) return;
    return new Promise<void>(resolve => {
      this.#transactionCommittedSubscriptions.add(resolve);
    });
  }

  #setTransactionEnded(errored = false) {
    this.#txEnded = true;
    for (const {resolve, reject} of this.#transactionEndedSubscriptions) {
      if (errored) {
        reject();
      } else {
        resolve();
      }
    }
    this.#transactionEndedSubscriptions.clear();
  }
}

const genericDatabase: GenericDatabaseManager = {
  open: async (name: string) => {
    const db = await SQLite.openDatabaseAsync(name);

    const genericDb: GenericSQLDatabase = {
      transaction: () => new ReplicacheExpoSQLiteTransaction(db),
      destroy: async () => {
        await db.closeAsync();
        await SQLite.deleteDatabaseAsync(name);
      },
      close: async () => await db.closeAsync(),
    };

    return genericDb;
  },
};

const expoDbManagerInstance = new ReplicacheGenericSQLiteDatabaseManager(
  genericDatabase,
);

export const createReplicacheExpoSQLiteKVStore: StoreProvider = {
  create: getCreateReplicacheSQLiteKVStore(expoDbManagerInstance),
  drop: (name: string) => expoDbManagerInstance.destroy(name),
};
