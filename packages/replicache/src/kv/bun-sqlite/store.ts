/* eslint-disable @typescript-eslint/ban-ts-comment */
// @ts-ignore - Bun modules are available at runtime in Bun environments
import {Database, type Statement} from 'bun:sqlite';
// @ts-ignore - fs module is available at runtime in Bun environments
import {unlinkSync, existsSync} from 'fs';
import type {
  PreparedStatement,
  SQLiteDatabase,
  SQLiteStoreOptions,
} from '../sqlite-store.ts';
import {dropStore, SQLiteStore} from '../sqlite-store.ts';
import type {StoreProvider} from '../store.ts';
export type BunSQLiteStoreOptions = SQLiteStoreOptions;

export function bunSQLiteStoreProvider(
  opts?: BunSQLiteStoreOptions,
): StoreProvider {
  return {
    create: name =>
      new SQLiteStore(name, name => new BunSQLiteDatabase(name), opts),
    drop: dropBunSQLiteStore,
  };
}

class BunSQLitePreparedStatement implements PreparedStatement {
  readonly #statement: Statement;

  constructor(statement: Statement) {
    this.#statement = statement;
  }

  // eslint-disable-next-line require-await -- Required by PreparedStatement interface
  async firstValue(params: string[]): Promise<string | undefined> {
    return Promise.resolve().then(() => {
      const row = this.#statement.get(params);

      if (row === null || row === undefined) {
        return undefined;
      }

      // Handle different row structures based on the query
      // For has() queries: SELECT 1 FROM entry WHERE key = ? LIMIT 1
      // Returns: {1: 1} when key exists
      if (typeof row === 'object' && row !== null && '1' in row) {
        return '1';
      }

      // For get() queries: SELECT value FROM entry WHERE key = ?
      // Returns: {value: "json_string"} when key exists
      if (typeof row === 'object' && row !== null && 'value' in row) {
        return (row as {value: string}).value;
      }

      return undefined;
    });
  }

  async exec(params: string[]): Promise<void> {
    await Promise.resolve().then(() => {
      this.#statement.run(params);
    });
  }
}

class BunSQLiteDatabase implements SQLiteDatabase {
  readonly #db: Database;
  readonly #filename: string;
  readonly #statements: Set<Statement> = new Set();

  constructor(filename: string) {
    this.#filename = filename;
    this.#db = Database.open(filename);
  }

  close(): void {
    for (const stmt of this.#statements) {
      stmt.finalize();
    }
    this.#db.close();
  }

  destroy(): void {
    if (
      (this.#db as {destroy?: () => void})?.destroy &&
      process.env.NODE_ENV === 'test'
    ) {
      (this.#db as unknown as {destroy(): void}).destroy();
    }

    if (existsSync(this.#filename)) {
      unlinkSync(this.#filename);
    }
  }

  prepare(sql: string): PreparedStatement {
    const statement = this.#db.prepare(sql);
    this.#statements.add(statement);
    return new BunSQLitePreparedStatement(statement);
  }

  execSync(sql: string): void {
    this.#db.run(sql);
  }
}

export function dropBunSQLiteStore(name: string): Promise<void> {
  return dropStore(name, filename => new BunSQLiteDatabase(filename));
}
