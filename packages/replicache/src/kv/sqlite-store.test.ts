import {afterEach, describe, expect, test} from 'vitest';
import {withWriteNoImplicitCommit} from '../with-transactions.ts';
import type {PreparedStatement, SQLiteDatabase} from './sqlite-store.ts';
import {SQLiteStore, clearAllNamedStoresForTesting} from './sqlite-store.ts';

class FakeSQLiteDatabase implements SQLiteDatabase {
  readonly #rows = new Map<string, string>();
  readonly #failOnPut: boolean;
  readonly #forceRollbackError: string | undefined;
  readonly #noTransactionRollbackError: string;
  #inTransaction = false;

  constructor(
    failOnPut: boolean,
    noTransactionRollbackError: string,
    forceRollbackError?: string | undefined,
  ) {
    this.#failOnPut = failOnPut;
    this.#noTransactionRollbackError = noTransactionRollbackError;
    this.#forceRollbackError = forceRollbackError;
  }

  close(): void {}

  destroy(): void {}

  prepare(sql: string): PreparedStatement {
    if (sql.includes('SELECT 1 FROM entry')) {
      return {
        firstValue: (_params: string[]) => Promise.resolve(undefined),
        exec: (_params: string[]) => Promise.resolve(),
      };
    }

    if (sql.includes('SELECT value FROM entry')) {
      return {
        firstValue: ([key]: string[]) => Promise.resolve(this.#rows.get(key)),
        exec: (_params: string[]) => Promise.resolve(),
      };
    }

    if (sql.includes('INSERT OR REPLACE INTO entry')) {
      return {
        firstValue: (_params: string[]) => Promise.resolve(undefined),
        exec: ([key, value]: string[]) => {
          if (this.#failOnPut) {
            // Simulate SQLite aborting the transaction before the caller's cleanup.
            this.#inTransaction = false;
            return Promise.reject(new Error('original put failure'));
          }
          this.#rows.set(key, value);
          return Promise.resolve();
        },
      };
    }

    if (sql.includes('DELETE FROM entry')) {
      return {
        firstValue: (_params: string[]) => Promise.resolve(undefined),
        exec: ([key]: string[]) => {
          this.#rows.delete(key);
          return Promise.resolve();
        },
      };
    }

    return {
      firstValue: (_params: string[]) => Promise.resolve(undefined),
      exec: (_params: string[]) => Promise.resolve(),
    };
  }

  execSync(sql: string): void {
    if (sql === 'BEGIN' || sql === 'BEGIN IMMEDIATE') {
      this.#inTransaction = true;
      return;
    }
    if (sql === 'COMMIT') {
      this.#inTransaction = false;
      return;
    }
    if (sql === 'ROLLBACK') {
      if (this.#forceRollbackError !== undefined) {
        throw new Error(this.#forceRollbackError);
      }
      if (!this.#inTransaction) {
        throw new Error(this.#noTransactionRollbackError);
      }
      this.#inTransaction = false;
      return;
    }
  }
}

describe('kv/sqlite-store', () => {
  afterEach(() => {
    clearAllNamedStoresForTesting();
  });

  test('withWriteNoImplicitCommit reports both operation and release errors', async () => {
    const db = new FakeSQLiteDatabase(
      true,
      'cannot rollback - no transaction is active',
    );
    const store = new SQLiteStore('auto-aborted', () => db);

    const err = await withWriteNoImplicitCommit(store, async write => {
      await write.put('key', 'value');
    }).then(
      () => undefined,
      e => e,
    );

    expect(err).toBeInstanceOf(AggregateError);
    expect((err as AggregateError).errors).toHaveLength(2);
    expect(String((err as AggregateError).errors[0])).toContain(
      'original put failure',
    );
    expect(String((err as AggregateError).errors[1])).toContain(
      'cannot rollback - no transaction is active',
    );

    await store.close();
  });

  test('release throws rollback errors when there is no operation error', async () => {
    const db = new FakeSQLiteDatabase(
      false,
      'cannot rollback - no transaction is active',
      'unexpected rollback failure',
    );
    const store = new SQLiteStore('rollback-failure', () => db);

    const err = await withWriteNoImplicitCommit(store, async write => {
      await write.put('key', 'value');
    }).then(
      () => undefined,
      e => e,
    );
    expect(String(err)).toContain('unexpected rollback failure');
    await store.close();
  });
});
