import type {DB, Transaction} from '@op-engineering/op-sqlite';
import {type SQLiteTransaction} from '../../replicache/src/kv/sqlite-store.ts';

export class OPSQLiteTransaction implements SQLiteTransaction {
  private readonly _db: DB;
  #tx: Transaction | null = null;
  #transactionCommittedSubscriptions = new Set<() => void>();
  #txCommitted = false;
  #transactionEndedSubscriptions = new Set<{
    resolve: () => void;
    reject: (reason?: unknown) => void;
  }>();
  #txEnded = false;

  constructor(db: DB) {
    this._db = db;
  }

  // op-sqlite doesn't support readonly
  async begin() {
    await new Promise<void>((resolve, reject) => {
      let didResolve = false;
      try {
        void this._db.transaction(async tx => {
          didResolve = true;
          this.#tx = tx;
          resolve();

          try {
            // op-sqlite auto-commits our transaction when this callback ends.
            // Lets artificially keep it open until we commit.
            await this.#waitForTransactionCommitted();
            this.#setTransactionEnded(false);
          } catch (error) {
            this.#setTransactionEnded(true, error);
          }
        });
      } catch {
        if (!didResolve) {
          reject(new Error('Did not resolve'));
        }
      }
    });
  }

  async execute<T>(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ) {
    const tx = this.#assertTransactionReady();
    const {rows} = await tx.execute(sqlStatement, args);

    return rows as T[];
  }

  async commit() {
    const tx = this.#assertTransactionReady();
    await tx.commit();
    this.#txCommitted = true;
    for (const resolver of this.#transactionCommittedSubscriptions) {
      resolver();
    }
    this.#transactionCommittedSubscriptions.clear();
  }

  waitForTransactionEnded() {
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

  #setTransactionEnded(errored: boolean, error?: unknown) {
    this.#txEnded = true;
    for (const {resolve, reject} of this.#transactionEndedSubscriptions) {
      if (errored) {
        reject(error);
      } else {
        resolve();
      }
    }
    this.#transactionEndedSubscriptions.clear();
  }
}
