import type {
  PreparedStatement,
  SQLiteDatabase,
  SQLiteStoreOptions,
} from '../sqlite-store.ts';
import {dropStore, SQLiteStore} from '../sqlite-store.ts';
import type {StoreProvider} from '../store.ts';
import {open, type DB} from './types.ts';

export type OpSQLiteStoreOptions = SQLiteStoreOptions & {
  // OpSQLite-specific options
  location?: 'default' | 'Library' | 'Documents' | 'Temporary';
  encryptionKey?: string;
};

function dropOpSQLiteStore(name: string): Promise<void> {
  return dropStore(
    name,
    (filename, opts) => new OpSQLiteDatabase(filename, opts),
  );
}

/**
 * Creates a StoreProvider for SQLite-based stores using @op-engineering/op-sqlite.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 * Uses parameterized queries for safety and performance.
 */
export function opSQLiteStoreProvider(
  opts?: OpSQLiteStoreOptions,
): StoreProvider {
  return {
    create: name =>
      new SQLiteStore(
        name,
        (name, options) => new OpSQLiteDatabase(name, options),
        opts,
      ),
    drop: dropOpSQLiteStore,
  };
}

class OpSQLitePreparedStatement implements PreparedStatement {
  readonly #db: DB;
  readonly #sql: string;

  constructor(db: DB, sql: string) {
    this.#db = db;
    this.#sql = sql;
  }

  async firstValue(params: string[]): Promise<string | undefined> {
    const rows = await this.#db.executeRaw(this.#sql, params);
    return rows[0]?.[0];
  }

  async exec(params: string[]): Promise<void> {
    await this.#db.executeRaw(this.#sql, params);
  }
}

class OpSQLiteDatabase implements SQLiteDatabase {
  readonly #db: DB;
  readonly #filename: string;

  constructor(filename: string, opts?: OpSQLiteStoreOptions) {
    this.#filename = filename;
    const openOpts: {
      name: string;
      location?: string;
      encryptionKey?: string;
    } = {name: filename};

    if (opts?.location) {
      openOpts.location = opts.location;
    }
    if (opts?.encryptionKey) {
      openOpts.encryptionKey = opts.encryptionKey;
    }

    this.#db = open(openOpts);
  }

  close(): void {
    this.#db.close();
  }

  destroy(): void {
    // OpSQLite uses delete method on the database instance
    // We need to create a temporary connection to delete the database
    try {
      const tempDb = open({name: this.#filename});
      tempDb.delete();
      tempDb.close();
    } catch (error) {
      // Database might not exist, which is fine
    }
  }

  prepare(sql: string): PreparedStatement {
    return new OpSQLitePreparedStatement(this.#db, sql);
  }

  execSync(sql: string): void {
    this.#db.executeRawSync(sql, []);
  }
}
