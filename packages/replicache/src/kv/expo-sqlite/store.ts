import {Lock} from '@rocicorp/lock';
import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteDatabase as DB,
  type SQLiteStatement,
} from 'expo-sqlite';
import type {
  PreparedStatement,
  SQLiteDatabase,
  SQLiteStoreOptions,
} from '../sqlite-store.ts';
import {dropStore, SQLiteStore} from '../sqlite-store.ts';
import type {StoreProvider} from '../store.ts';

export type ExpoSQLiteStoreOptions = SQLiteStoreOptions;

export function dropExpoSQLiteStore(
  name: string,
  opts?: ExpoSQLiteStoreOptions,
): Promise<void> {
  return dropStore(name, filename => new ExpoSQLiteDatabase(filename), opts);
}

/**
 * Creates a StoreProvider for SQLite-based stores using expo-sqlite.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 */
export function expoSQLiteStoreProvider(
  opts?: ExpoSQLiteStoreOptions,
): StoreProvider {
  return {
    create: name =>
      new SQLiteStore(name, name => new ExpoSQLiteDatabase(name), opts),
    drop: name => dropExpoSQLiteStore(name, opts),
  };
}

/**
 * An expo-sqlite prepared statement is a stateful cursor over a single
 * `sqlite3_stmt`: `executeForRawResultAsync` resets, rebinds and steps the
 * first row, and the returned result's `getAllAsync` then steps the *remaining*
 * rows of whatever the statement is currently bound to. Both are separate
 * native round trips.
 *
 * `SQLiteStore` shares one prepared statement per SQL across all concurrent
 * readers, so without serialization reader B's `executeForRawResultAsync` can
 * rebind the statement inside reader A's await gap, and A's `getAllAsync` then
 * returns B's rows (and B is starved of them). That yields wrong or missing
 * values and, when a ref count delta is computed from such a read and written,
 * a persistently corrupted store.
 *
 * Every call on a statement is therefore run under a per-statement lock so the
 * execute + fetch pair is atomic with respect to other users of that statement.
 */
class ExpoSQLitePreparedStatement implements PreparedStatement {
  readonly #statement: SQLiteStatement;
  readonly #lock = new Lock();

  constructor(statement: SQLiteStatement) {
    this.#statement = statement;
  }

  exec(params: string[]): Promise<void> {
    return this.#lock.withLock(async () => {
      await this.#statement.executeForRawResultAsync(params);
    });
  }

  all(params: string[]): Promise<unknown[][]> {
    return this.#lock.withLock(async () => {
      const result = await this.#statement.executeForRawResultAsync(params);
      // Must be awaited inside the lock so that no other caller can reset the
      // statement before all rows have been stepped.
      return (await result.getAllAsync()) as unknown[][];
    });
  }
}

class ExpoSQLiteDatabase implements SQLiteDatabase {
  readonly #db: DB;
  readonly #filename: string;
  readonly #statements: Set<SQLiteStatement> = new Set();

  constructor(filename: string) {
    this.#filename = filename;
    this.#db = openDatabaseSync(filename);
  }

  close(): void {
    for (const stmt of this.#statements) {
      stmt.finalizeSync();
    }
    this.#db.closeSync();
  }

  destroy(): void {
    deleteDatabaseSync(this.#filename);
  }

  prepare(sql: string): PreparedStatement {
    const statement = this.#db.prepareSync(sql);
    this.#statements.add(statement);
    return new ExpoSQLitePreparedStatement(statement);
  }

  execSync(sql: string): void {
    this.#db.execSync(sql);
  }
}
