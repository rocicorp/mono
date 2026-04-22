import {afterEach, describe, expect, test} from 'vitest';
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

  test('release preserves original error when transaction already auto-aborted', async () => {
    const db = new FakeSQLiteDatabase(
      true,
      'cannot rollback - no transaction is active',
    );
    const store = new SQLiteStore('auto-aborted', () => db);

    const write = await store.write();
    const err = await write.put('key', 'value').then(
      () => undefined,
      e => e,
    );

    expect(String(err)).toContain('original put failure');
    expect(() => write.release()).not.toThrow();
    expect(write.closed).toBe(true);

    await store.close();
  });

  test('release still throws unexpected rollback errors', async () => {
    const db = new FakeSQLiteDatabase(
      false,
      'cannot rollback - no transaction is active',
      'unexpected rollback failure',
    );
    const store = new SQLiteStore('rollback-failure', () => db);

    const write = await store.write();
    await write.put('key', 'value');

    expect(() => write.release()).toThrow('unexpected rollback failure');
    await store.close();
  });
});
