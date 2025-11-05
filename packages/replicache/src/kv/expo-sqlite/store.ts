import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteDatabase as DB,
  type SQLiteStatement,
} from 'expo-sqlite';
import {IDBDatabasesStore} from '../../persist/idb-databases-store.ts';
import type {
  PreparedStatement,
  SQLiteDatabase,
  SQLiteStoreOptions,
} from '../sqlite-store.ts';
import {dropStore, SQLiteStore} from '../sqlite-store.ts';
import type {StoreProvider} from '../store.ts';

export type ExpoSQLiteStoreOptions = SQLiteStoreOptions;

export function dropExpoSQLiteStore(name: string): Promise<void> {
  return dropStore(name, filename => new ExpoSQLiteDatabase(filename));
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
    drop: dropExpoSQLiteStore,
  };
}

/**
 * Drops all expo-sqlite databases associated with Replicache.
 * Useful for testing.
 */
export async function dropAllExpoSQLiteDatabases(): Promise<void> {
  const storeProvider = expoSQLiteStoreProvider();
  const idbDatabasesStore = new IDBDatabasesStore(storeProvider.create);

  const databases = await idbDatabasesStore.getDatabases();
  const dbNames = Object.values(databases).map(db => db.name);

  await Promise.all(
    dbNames.map(async name => {
      await storeProvider.drop(name);
      await idbDatabasesStore.deleteDatabases([name]);
    }),
  );
}

class ExpoSQLitePreparedStatement implements PreparedStatement {
  readonly #statement: SQLiteStatement;

  constructor(statement: SQLiteStatement) {
    this.#statement = statement;
  }

  async firstValue(params: string[]): Promise<string | undefined> {
    const result = await this.#statement.executeForRawResultAsync(params);
    const row = await result.getFirstAsync();
    return row === null ? undefined : row[0];
  }

  async exec(params: string[]): Promise<void> {
    await this.#statement.executeForRawResultAsync(params);
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
