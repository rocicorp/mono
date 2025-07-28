import {RWLock} from '@rocicorp/lock';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  promiseUndefined,
  promiseVoid,
} from '../../../shared/src/resolved-promises.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Read, Store, Write} from './store.ts';

/**
 * A SQLite prepared statement.
 *
 * `run` executes the statement with optional parameters.
 * `all` executes the statement and returns the result rows.
 * `finalize` releases the statement.
 */
export interface PreparedStatement {
  run(...params: unknown[]): void;
  all<T>(...params: unknown[]): T[];
  finalize(): void;
}

export interface SQLiteDatabase {
  /**
   * Close the database connection.
   */
  close(): void;

  /**
   * Destroy or delete the database (e.g. delete file).
   */
  destroy(): void;

  /**
   * Prepare a SQL string, returning a statement you can execute.
   * E.g. `const stmt = db.prepare("SELECT * FROM todos WHERE id=?");`
   */
  prepare(sql: string): PreparedStatement;

  /**
   * Check if the database is in a transaction.
   */
  isInTransaction(): boolean;
}

type SQLitePreparedStatements = {
  begin: PreparedStatement;
  beginImmediate: PreparedStatement;
  commit: PreparedStatement;
  rollback: PreparedStatement;

  savepoint: PreparedStatement;
  release: PreparedStatement;

  get: PreparedStatement;
  put: PreparedStatement;
  del: PreparedStatement;
};

const getPreparedStatementsForSQLiteDatabase = (
  db: SQLiteDatabase,
): SQLitePreparedStatements => ({
  begin: db.prepare('BEGIN'),
  beginImmediate: db.prepare('BEGIN IMMEDIATE'),
  commit: db.prepare('COMMIT'),
  rollback: db.prepare('ROLLBACK'),

  // Similar to https://github.com/WiseLibs/better-sqlite3/blob/674ce6be68a26742d9e24f8672da7888cea0aebb/lib/methods/transaction.js#L35-L39
  savepoint: db.prepare('SAVEPOINT `\t_rc.\t`'),
  release: db.prepare('RELEASE `\t_rc.\t`'),

  get: db.prepare('SELECT value FROM entry WHERE key = ?'),
  put: db.prepare('INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)'),
  del: db.prepare('DELETE FROM entry WHERE key = ?'),
});

const rwLocks = new Map<string, RWLock>();

/**
 * A SQLite-based Store implementation.
 *
 * This store provides a generic SQLite implementation that can be used with different
 * SQLite providers (expo-sqlite, better-sqlite3, etc). It implements the Store
 * interface using a single 'entry' table with key-value pairs.
 */
export class SQLiteStore implements Store {
  readonly #name: string;
  readonly #dbm: SQLiteDatabaseManager;
  readonly #rwLock: RWLock;

  readonly #db: SQLiteDatabase;

  readonly #preparedStatements: SQLitePreparedStatements;

  #closed = false;

  constructor(
    name: string,
    dbm: SQLiteDatabaseManager,
    opts?: SQLiteDatabaseManagerOptions | undefined,
  ) {
    this.#name = name;
    this.#dbm = dbm;
    this.#rwLock = rwLocks.get(name) ?? new RWLock();
    rwLocks.set(name, this.#rwLock);

    this.#db = this.#dbm.open(name, opts);

    this.#preparedStatements = getPreparedStatementsForSQLiteDatabase(this.#db);
  }

  async read(): Promise<Read> {
    const release = await this.#rwLock.read();
    return new SQLiteStoreRead(
      this.#preparedStatements,
      this.#db.isInTransaction(),
      release,
    );
  }

  async write(): Promise<Write> {
    const release = await this.#rwLock.write();
    return new SQLiteStoreWrite(this.#preparedStatements, release);
  }

  close(): Promise<void> {
    for (const stmt of Object.values(this.#preparedStatements)) {
      stmt.finalize();
    }

    this.#dbm.close(this.#name);
    this.#closed = true;

    return promiseVoid;
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class SQLiteStoreRWBase {
  protected readonly _preparedStatements: SQLitePreparedStatements;
  readonly #release: () => void;
  #closed = false;

  constructor(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ) {
    this._preparedStatements = preparedStatements;
    this.#release = release;
  }

  has(key: string): Promise<boolean> {
    const unsafeValue = this.#getSql(key);
    return Promise.resolve(unsafeValue !== undefined);
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    const unsafeValue = this.#getSql(key);
    if (unsafeValue === undefined) return promiseUndefined;
    const parsedValue = JSON.parse(unsafeValue) as ReadonlyJSONValue;
    const frozenValue = deepFreeze(parsedValue);
    return Promise.resolve(frozenValue);
  }

  #getSql(key: string): string | undefined {
    const rows = this._preparedStatements.get.all<{value: string}>([key]);
    if (rows.length === 0) return undefined;
    return rows?.[0]?.value;
  }

  protected _release(): void {
    this.#closed = true;
    this.#release();
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export class SQLiteStoreRead extends SQLiteStoreRWBase implements Read {
  readonly #isInTransaction: boolean;

  constructor(
    preparedStatements: SQLitePreparedStatements,
    isInTransaction: boolean,
    release: () => void,
  ) {
    super(preparedStatements, release);
    this.#isInTransaction = isInTransaction;

    if (!this.#isInTransaction) {
      // BEGIN
      this._preparedStatements.begin.run();
    } else {
      // SAVEPOINT rc
      this._preparedStatements.savepoint.run();
    }
  }

  release(): void {
    if (!this.#isInTransaction) {
      // COMMIT
      this._preparedStatements.commit.run();
    } else {
      // RELEASE rc
      this._preparedStatements.release.run();
    }

    this._release();
  }
}

export class SQLiteStoreWrite extends SQLiteStoreRWBase implements Write {
  #committed = false;

  constructor(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ) {
    super(preparedStatements, release);

    // BEGIN IMMEDIATE grabs a RESERVED lock
    this._preparedStatements.beginImmediate.run();
  }

  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    this._preparedStatements.put.run([key, JSON.stringify(value)]);
    return promiseVoid;
  }

  del(key: string): Promise<void> {
    this._preparedStatements.del.run([key]);
    return promiseVoid;
  }

  commit(): Promise<void> {
    // COMMIT
    this._preparedStatements.commit.run();
    this.#committed = true;
    return promiseVoid;
  }

  release(): void {
    if (!this.#committed) {
      // ROLLBACK if not committed
      this._preparedStatements.rollback.run();
    }

    this._release();
  }
}

export interface GenericSQLiteDatabaseManager {
  open(fileName: string): SQLiteDatabase;
}

// we replace non-alphanumeric characters with underscores
// because SQLite doesn't allow them in database names
const safeFilename = (name: string) => name.replace(/[^a-zA-Z0-9]/g, '_');

/**
 * Creates a function that returns new SQLite store instances.
 * This is the main entry point for using the SQLite store implementation.
 *
 * @param dbm The SQLite database manager implementation
 * @returns A function that creates new store instances
 */
export function createSQLiteStore(dbm: SQLiteDatabaseManager) {
  return (name: string, opts?: SQLiteDatabaseManagerOptions | undefined) =>
    new SQLiteStore(name, dbm, opts);
}

export type SQLiteDatabaseManagerOptions = {
  busyTimeout?: number | undefined;
  journalMode?: 'WAL' | 'DELETE' | undefined;
};

export class SQLiteDatabaseManager {
  readonly #dbm: GenericSQLiteDatabaseManager;
  readonly #dbInstances = new Map<
    string,
    {db: SQLiteDatabase; state: 'open' | 'closed'}
  >();

  constructor(dbm: GenericSQLiteDatabaseManager) {
    this.#dbm = dbm;
  }

  clearAllStoresForTesting(): void {
    for (const dbInstance of this.#dbInstances.values()) {
      dbInstance.db.destroy();
      dbInstance.state = 'closed';
    }
    this.#dbInstances.clear();
  }

  open(
    name: string,
    opts: SQLiteDatabaseManagerOptions | undefined = {
      busyTimeout: 200,
    },
  ): SQLiteDatabase {
    const dbInstance = this.#dbInstances.get(name);

    if (dbInstance?.state === 'open') return dbInstance.db;

    const fileName = safeFilename(name);
    const newDb = this.#dbm.open(fileName);

    if (!dbInstance) {
      this.#ensureSchema(newDb);
    } else {
      dbInstance.state = 'open';
    }

    // we set a busy timeout to wait for write locks to be released
    newDb.prepare(`PRAGMA busy_timeout = ${opts.busyTimeout}`).run();
    if (opts.journalMode) {
      // WAL allows concurrent readers (improves write throughput ~15x and read throughput ~1.5x)
      // but does not work on all platforms (e.g. Expo)
      newDb.prepare(`PRAGMA journal_mode = ${opts.journalMode}`).run();
    }
    // tradeoff of durability vs performance over FULL
    newDb.prepare('PRAGMA synchronous = NORMAL').run();
    // don't read uncommitted data
    newDb.prepare('PRAGMA read_uncommitted = false').run();

    this.#dbInstances.set(name, {db: newDb, state: 'open'});

    return newDb;
  }

  close(name: string) {
    const dbInstance = this.#dbInstances.get(name);
    if (!dbInstance) return;

    dbInstance.db.close();
    dbInstance.state = 'closed';
  }

  destroy(name: string): void {
    const dbInstance = this.#dbInstances.get(name);
    if (!dbInstance) return;

    dbInstance.db.destroy();
    dbInstance.state = 'closed';
  }

  #ensureSchema(db: SQLiteDatabase): void {
    db.prepare('BEGIN IMMEDIATE').run();

    try {
      // WITHOUT ROWID increases write throughput by ~3.6x and ~0.1x to read throughput
      db.prepare(
        'CREATE TABLE IF NOT EXISTS entry (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID',
      ).run();

      db.prepare('COMMIT').run();
    } catch (e) {
      db.prepare('ROLLBACK').run();
      throw e;
    }
  }
}
