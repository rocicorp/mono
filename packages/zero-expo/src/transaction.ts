import {SQLiteDatabase, type SQLiteExecuteAsyncResult} from 'expo-sqlite';
import {type SQLiteTransaction} from '../../replicache/src/kv/sqlite-store.ts';
import {resolver, type Resolver} from '@rocicorp/resolver';

type Transaction = Parameters<
  Parameters<SQLiteDatabase['withExclusiveTransactionAsync']>[0]
>[0];

export class ExpoSQLiteTransaction implements SQLiteTransaction {
  #tx: Transaction | null = null;
  #transactionCommittedSubscriptions = new Set<Resolver<void>>();
  #txCommitted = false;
  #transactionEndedSubscriptions = new Set<Resolver<void>>();
  #txEnded = false;
  private readonly _db: SQLiteDatabase;

  constructor(db: SQLiteDatabase) {
    this._db = db;
  }

  // expo-sqlite doesn't support readonly
  begin() {
    const beginResolver = resolver<void>();

    let didResolve = false;
    try {
      void this._db.withExclusiveTransactionAsync(async tx => {
        didResolve = true;
        this.#tx = tx;
        beginResolver.resolve();

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
        beginResolver.reject(new Error('Did not resolve'));
      }
    }

    return beginResolver.promise;
  }

  async execute<T>(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ) {
    const tx = this.#assertTransactionReady();

    const statement = await tx.prepareAsync(sqlStatement);
    let allRows: T[];
    let result: SQLiteExecuteAsyncResult<T>;
    try {
      result = await statement.executeAsync(...(args ?? []));
      allRows = await result.getAllAsync();
    } finally {
      await statement.finalizeAsync();
    }

    return {item: (idx: number) => allRows[idx], length: allRows.length};
  }

  commit(): Promise<void> {
    // Transaction is committed automatically.
    this.#txCommitted = true;
    for (const r of this.#transactionCommittedSubscriptions) {
      r.resolve();
    }
    this.#transactionCommittedSubscriptions.clear();
    return Promise.resolve();
  }

  waitForTransactionEnded() {
    if (this.#txEnded) return;
    const endedResolver = resolver<void>();
    this.#transactionEndedSubscriptions.add(endedResolver);
    return endedResolver.promise;
  }

  #assertTransactionReady() {
    if (this.#tx === null) throw new Error('Transaction is not ready.');
    if (this.#txCommitted) throw new Error('Transaction already committed.');
    if (this.#txEnded) throw new Error('Transaction already ended.');
    return this.#tx;
  }

  #waitForTransactionCommitted() {
    if (this.#txCommitted) return;
    const committedResolver = resolver<void>();
    this.#transactionCommittedSubscriptions.add(committedResolver);
    return committedResolver.promise;
  }

  #setTransactionEnded(errored = false) {
    this.#txEnded = true;
    for (const r of this.#transactionEndedSubscriptions) {
      if (errored) {
        r.reject(new Error('Transaction ended with error'));
      } else {
        r.resolve();
      }
    }
    this.#transactionEndedSubscriptions.clear();
  }
}
