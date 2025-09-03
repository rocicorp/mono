import {Lock, RWLock} from '@rocicorp/lock';
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
}

type SQLiteTransactionPreparedStatements = {
  begin: PreparedStatement;
  beginImmediate: PreparedStatement;
  commit: PreparedStatement;
  rollback: PreparedStatement;
};

const getTransactionPreparedStatements = (
  db: SQLiteDatabase,
): SQLiteTransactionPreparedStatements => ({
  begin: db.prepare('BEGIN'),
  beginImmediate: db.prepare('BEGIN IMMEDIATE'),
  commit: db.prepare('COMMIT'),
  rollback: db.prepare('ROLLBACK'),
});

type SQLiteRWPreparedStatements = {
  get: PreparedStatement;
  put: PreparedStatement;
  del: PreparedStatement;
};

const getRWPreparedStatements = (
  db: SQLiteDatabase,
): SQLiteRWPreparedStatements => ({
  get: db.prepare('SELECT value FROM entry WHERE key = ?'),
  put: db.prepare('INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)'),
  del: db.prepare('DELETE FROM entry WHERE key = ?'),
});

type SQLitePreparedStatements = SQLiteTransactionPreparedStatements &
  SQLiteRWPreparedStatements;

interface SQLiteConnectionManager {
  acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }>;
}

type SQLitePreparedStatementPoolEntry = {
  lock: Lock;
  preparedStatements: SQLitePreparedStatements;
};

/**
 * Manages a pool of read-only SQLite connections.
 *
 * Each connection in the pool is protected by its own `Lock` instance which
 * guarantees that it is held by at most one reader at a time. Consumers call
 * {@link acquire} to get access to a set of prepared statements for a
 * connection and must invoke the provided `release` callback when they are
 * finished.
 *
 * The pool eagerly creates the configured number of connections up-front so
 * that the first `acquire` call never has to pay the connection setup cost.
 */
class SQLiteReadConnectionManager implements SQLiteConnectionManager {
  #pool: SQLitePreparedStatementPoolEntry[] = [];
  #nextIndex = 0;
  readonly #rwLock: RWLock;

  constructor(
    name: string,
    manager: SQLiteDatabaseManager,
    rwLock: RWLock,
    opts: SQLiteDatabaseManagerOptions,
  ) {
    if (opts.readPoolSize <= 1) {
      throw new Error('readPoolSize must be greater than 1');
    }

    this.#rwLock = rwLock;

    for (let i = 0; i < opts.readPoolSize; i++) {
      // create a new readonly SQLiteDatabase for each instance in the pool
      const {preparedStatements} = manager.open(name, opts);
      this.#pool.push({
        lock: new Lock(),
        preparedStatements,
      });
    }
  }

  /**
   * Acquire a round-robin read connection from the pool.
   *
   * The returned `release` callback **must** be invoked once the caller is done
   * using the prepared statements, otherwise other readers may be blocked
   * indefinitely.
   */
  async acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }> {
    const slot = this.#nextIndex;
    this.#nextIndex = (this.#nextIndex + 1) % this.#pool.length;

    const entry = this.#pool[slot];

    // we have two levels of locking
    // 1. the RWLock to prevent concurrent read operations while a write is in progress
    // 2. the Lock to prevent concurrent read operations on the same connection
    const releaseRWLock = await this.#rwLock.read();
    const releaseLock = await entry.lock.lock();

    return {
      preparedStatements: entry.preparedStatements,
      release: () => {
        releaseRWLock();
        releaseLock();
      },
    };
  }
}

/**
 * Manages a single write connection with an external RWLock.
 */
class SQLiteWriteConnectionManager implements SQLiteConnectionManager {
  readonly #rwLock: RWLock;
  readonly #preparedStatements: SQLitePreparedStatements;

  constructor(
    name: string,
    manager: SQLiteDatabaseManager,
    rwLock: RWLock,
    opts: SQLiteDatabaseManagerOptions,
  ) {
    const {preparedStatements} = manager.open(name, opts);
    this.#preparedStatements = preparedStatements;
    this.#rwLock = rwLock;
  }

  async acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }> {
    const release = await this.#rwLock.write();
    return {preparedStatements: this.#preparedStatements, release};
  }
}

/**
 * A SQLite-based Store implementation.
 *
 * This store provides a generic SQLite implementation that can be used with different
 * SQLite providers (expo-sqlite, better-sqlite3, etc). It implements the Store
 * interface using a single 'entry' table with key-value pairs.
 *
 * The store uses a single RWLock to prevent concurrent read and write operations.
 *
 * The store also uses a pool of read connections to allow concurrent reads with separate transactions.
 */
export class SQLiteStore implements Store {
  readonly #name: string;
  readonly #dbm: SQLiteDatabaseManager;
  readonly #writeConnectionManager: SQLiteConnectionManager;
  readonly #readConnectionManager: SQLiteConnectionManager;
  readonly #rwLock = new RWLock();

  #closed = false;

  constructor(
    name: string,
    dbm: SQLiteDatabaseManager,
    opts: SQLiteDatabaseManagerOptions,
  ) {
    this.#name = name;
    this.#dbm = dbm;

    this.#writeConnectionManager = new SQLiteWriteConnectionManager(
      name,
      dbm,
      this.#rwLock,
      opts,
    );
    this.#readConnectionManager = new SQLiteReadConnectionManager(
      name,
      dbm,
      this.#rwLock,
      opts,
    );
  }

  async read(): Promise<Read> {
    const {preparedStatements, release} =
      await this.#readConnectionManager.acquire();
    return new SQLiteStoreRead(preparedStatements, release);
  }

  async write(): Promise<Write> {
    const {preparedStatements, release} =
      await this.#writeConnectionManager.acquire();
    return new SQLiteStoreWrite(preparedStatements, release);
  }

  close(): Promise<void> {
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
    const rows = this._preparedStatements.get.all<{value: string}>(key);
    if (rows.length === 0) return undefined;
    return rows[0].value;
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
  constructor(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ) {
    super(preparedStatements, release);

    // BEGIN
    this._preparedStatements.begin.run();
  }

  release(): void {
    // COMMIT
    this._preparedStatements.commit.run();

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
    this._preparedStatements.put.run(key, JSON.stringify(value));
    return promiseVoid;
  }

  del(key: string): Promise<void> {
    this._preparedStatements.del.run(key);
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
export function safeFilename(name: string) {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

export type SQLiteDatabaseManagerOptions = {
  /**
   * The number of read connections to keep open.
   *
   * This must be greater than 1 to support concurrent reads.
   */
  readPoolSize: number;

  busyTimeout?: number | undefined;
  journalMode?: 'WAL' | 'DELETE' | undefined;
  synchronous?: 'NORMAL' | 'FULL' | undefined;
  readUncommitted?: boolean | undefined;
};

const OPEN = 1;
const CLOSED = 0;

type DBInstance = {
  instances: {
    db: SQLiteDatabase;
    preparedStatements: SQLitePreparedStatements;
    state: typeof OPEN | typeof CLOSED;
  }[];
};

export class SQLiteDatabaseManager {
  readonly #dbm: GenericSQLiteDatabaseManager;
  readonly #dbInstances = new Map<string, DBInstance>();

  constructor(dbm: GenericSQLiteDatabaseManager) {
    this.#dbm = dbm;
  }

  clearAllStoresForTesting(): void {
    for (const [name] of this.#dbInstances) {
      this.destroy(name);
    }
  }

  open(
    name: string,
    opts: Omit<SQLiteDatabaseManagerOptions, 'poolSize'>,
  ): {db: SQLiteDatabase; preparedStatements: SQLitePreparedStatements} {
    const dbInstance = this.#dbInstances.get(name);

    const fileName = safeFilename(name);
    const newDb = this.#dbm.open(fileName);

    const txPreparedStatements = getTransactionPreparedStatements(newDb);

    const exec = (sql: string) => {
      const statement = newDb.prepare(sql);
      statement.run();
      statement.finalize();
    };

    if (!dbInstance) {
      // we only ensure the schema for the first open
      // the schema is the same for all connections
      this.#ensureSchema(exec, txPreparedStatements);
    }

    if (opts.busyTimeout !== undefined) {
      // we set a busy timeout to wait for write locks to be released
      exec(`PRAGMA busy_timeout = ${opts.busyTimeout}`);
    }
    if (opts.journalMode !== undefined) {
      // WAL allows concurrent readers (improves write throughput ~15x and read throughput ~1.5x)
      // but does not work on all platforms (e.g. Expo)
      exec(`PRAGMA journal_mode = ${opts.journalMode}`);
    }
    if (opts.synchronous !== undefined) {
      exec(`PRAGMA synchronous = ${opts.synchronous}`);
    }
    if (opts.readUncommitted !== undefined) {
      exec(
        `PRAGMA read_uncommitted = ${opts.readUncommitted ? 'true' : 'false'}`,
      );
    }

    // we prepare these after the schema is created
    const rwPreparedStatements = getRWPreparedStatements(newDb);

    const preparedStatements = {
      ...txPreparedStatements,
      ...rwPreparedStatements,
    };

    this.#dbInstances.set(name, {
      instances: [
        ...(dbInstance?.instances ?? []),
        {db: newDb, preparedStatements, state: OPEN},
      ],
    });

    return {
      db: newDb,
      preparedStatements,
    };
  }

  #closeDBInstance(name: string): DBInstance | undefined {
    const dbInstance = this.#dbInstances.get(name);
    if (dbInstance) {
      for (const instance of dbInstance.instances) {
        if (instance.state === CLOSED) {
          continue;
        }

        for (const stmt of Object.values(instance.preparedStatements)) {
          stmt.finalize();
        }
        instance.db.close();
        instance.state = CLOSED;
      }
    }
    return dbInstance;
  }

  close(name: string) {
    this.#closeDBInstance(name);
  }

  destroy(name: string): void {
    const dbInstance = this.#closeDBInstance(name);

    // All the instances in dbInstance share one underlying file.
    dbInstance?.instances[0].db.destroy();

    this.#dbInstances.delete(name);
  }

  #ensureSchema(
    exec: (sql: string) => void,
    preparedStatements: SQLiteTransactionPreparedStatements,
  ): void {
    preparedStatements.begin.run();

    try {
      // WITHOUT ROWID increases write throughput
      exec(
        'CREATE TABLE IF NOT EXISTS entry (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID',
      );
      preparedStatements.commit.run();
    } catch (e) {
      preparedStatements.rollback.run();
      throw e;
    }
  }
}
