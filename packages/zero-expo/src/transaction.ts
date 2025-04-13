import {SQLiteDatabase, type SQLiteExecuteAsyncResult} from 'expo-sqlite';
import {type SQLiteTransaction} from '../../replicache/src/kv/sqlite-store.ts';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../shared/src/asserts.ts';

type Transaction = SQLiteDatabase;

export class ExpoSQLiteTransaction implements SQLiteTransaction {
  private readonly _db: SQLiteDatabase;
  #tx: Transaction | null = null;
  #transactionCommittedSubscriptions = new Set<Resolver<void>>();
  #txCommitted = false;
  #transactionEndedSubscriptions = new Set<Resolver<void>>();
  #txEnded = false;

  constructor(db: SQLiteDatabase) {
    this._db = db;
  }

  // expo-sqlite doesn't support readonly
  begin() {
    const beginResolver = resolver<void>();

    let didResolve = false;
    try {
      void this._db.withExclusiveTransactionAsync(async (tx: Transaction) => {
        didResolve = true;
        this.#tx = tx;
        beginResolver.resolve();

        try {
          // expo-sqlite auto-commits our transaction when this callback ends.
          // Lets artificially keep it open until we commit.
          const waitPromise = this.#waitForTransactionCommitted();
          if (waitPromise) {
            await waitPromise;
            this.#setTransactionEnded(false);
          } else {
            this.#setTransactionEnded(false);
          }
        } catch (error) {
          this.#setTransactionEnded(true, error);
          throw error;
        }
      });
    } catch (error) {
      if (!didResolve) {
        beginResolver.reject(error || new Error('Did not resolve'));
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

    return allRows;
  }

  commit(): Promise<void> {
    this.#assertTransactionReady();
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
    assert(this.#tx !== null, 'Transaction is not ready (tx is null).');
    assert(!this.#txCommitted, 'Transaction already committed.');
    assert(!this.#txEnded, 'Transaction already ended.');
    return this.#tx;
  }

  #waitForTransactionCommitted() {
    if (this.#txCommitted) return;
    const committedResolver = resolver<void>();
    this.#transactionCommittedSubscriptions.add(committedResolver);
    return committedResolver.promise;
  }

  #setTransactionEnded(errored = false, error?: unknown) {
    if (this.#txEnded) return;
    this.#txEnded = true;
    for (const r of this.#transactionEndedSubscriptions) {
      if (errored) {
        r.reject(error);
      } else {
        r.resolve();
      }
    }
    this.#transactionEndedSubscriptions.clear();
  }
}
