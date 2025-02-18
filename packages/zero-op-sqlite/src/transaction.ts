import * as OPSQLite from '@op-engineering/op-sqlite';
import {
  SQLiteTransaction,
  type SQLResultSetRowList,
} from '../../replicache/src/kv/sqlite-store.ts';

export class OPSQLiteTransaction extends SQLiteTransaction {
  #tx: OPSQLite.Transaction | null = null;
  #transactionCommittedSubscriptions = new Set<() => void>();
  #txCommitted = false;
  #transactionEndedSubscriptions = new Set<{
    resolve: () => void;
    reject: () => void;
  }>();
  #txEnded = false;

  // TODO: where is the type for OPSQLiteConnection?
  // eslint-disable-next-line @typescript-eslint/parameter-properties
  constructor(private readonly _db: OPSQLite.OPSQLiteConnection) {
    super();
  }

  // op-sqlite doesn't support readonly
  start() {
    return new Promise<void>((resolve, reject) => {
      let didResolve = false;
      try {
        this._db.transaction(async tx => {
          didResolve = true;
          this.#tx = tx;
          resolve();

          try {
            // op-sqlite auto-commits our transaction when this callback ends.
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

  async execute(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ): Promise<SQLResultSetRowList> {
    const tx = this.#assertTransactionReady();
    const {rows} = await tx.execute(sqlStatement, args);

    return {
      item: (idx: number) => ({
        value: String(rows?.[idx]?.value ?? ''),
      }),
      length: rows?.length ?? 0,
    };
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
