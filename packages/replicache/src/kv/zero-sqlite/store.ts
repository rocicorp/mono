import open, {type Statement} from '@rocicorp/zero-sqlite3';
import {existsSync, unlinkSync} from 'node:fs';
import type {
  PreparedStatement,
  SQLiteDatabase,
  SQLiteStoreOptions,
} from '../sqlite-store.ts';
import {dropStore, SQLiteStore} from '../sqlite-store.ts';
import type {StoreProvider} from '../store.ts';
export {safeFilename} from '../sqlite-store.ts';

export type ZeroSQLiteStoreOptions = SQLiteStoreOptions;

export function dropZeroSQLiteStore(name: string): Promise<void> {
  return dropStore(name, filename => new ZeroSQLiteDatabase(filename));
}

/**
 * Creates a StoreProvider for SQLite-based stores using @rocicorp/zero-sqlite3.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 */
export function zeroSQLiteStoreProvider(
  opts?: ZeroSQLiteStoreOptions,
): StoreProvider {
  return {
    create: name =>
      new SQLiteStore(name, name => new ZeroSQLiteDatabase(name), opts),
    drop: dropZeroSQLiteStore,
  };
}

class ZeroSQLitePreparedStatement implements PreparedStatement {
  readonly #statement: Statement;

  constructor(statement: Statement) {
    this.#statement = statement;
  }

  // eslint-disable-next-line require-await
  async firstValue(params: string[]): Promise<unknown | undefined> {
    const result = this.#statement.all(...params);
    if (result === undefined || result.length === 0) {
      return undefined;
    }
    return Object.values(result[0] as Record<string, unknown>)[0];
  }

  // eslint-disable-next-line require-await
  async exec(params: string[]): Promise<void> {
    this.#statement.run(params);
  }
}

class ZeroSQLiteDatabase implements SQLiteDatabase {
  readonly #db: open.Database;
  readonly #filename: string;

  constructor(filename: string, opts?: ZeroSQLiteStoreOptions) {
    this.#filename = filename;
    const openOpts = opts?.busyTimeout
      ? {
          timeout: opts.busyTimeout,
        }
      : undefined;
    this.#db = open(filename, openOpts);
  }

  close(): void {
    this.#db.close();
  }

  destroy(): void {
    // Use node file system to delete the database file
    if (existsSync(this.#filename)) unlinkSync(this.#filename);
  }

  prepare(sql: string): PreparedStatement {
    const statement = this.#db.prepare(sql);
    return new ZeroSQLitePreparedStatement(statement);
  }

  execSync(sql: string): void {
    this.#db.exec(sql);
  }
}
