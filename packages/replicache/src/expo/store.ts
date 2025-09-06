import {RWLock} from '@rocicorp/lock';
import {
  deleteDatabaseSync,
  openDatabaseSync,
  type SQLiteDatabase,
  type SQLiteStatement,
} from 'expo-sqlite';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Read, Store, StoreProvider, Write} from '../kv/store.ts';

export type ExpoSQLiteStoreOptions = {
  busyTimeout?: number | undefined;
  journalMode?: 'WAL' | 'DELETE' | undefined;
  synchronous?: 'NORMAL' | 'FULL' | undefined;
  readUncommitted?: boolean | undefined;
};

function safeFilename(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

type Statements = {
  has: SQLiteStatement;
  get: SQLiteStatement;
  put: SQLiteStatement;
  del: SQLiteStatement;
};

/**
 * Creates prepared statements for common database operations.
 * These statements are reused for better performance.
 */
function createStatements(db: SQLiteDatabase): Statements {
  return {
    has: db.prepareSync('SELECT 1 FROM entry WHERE key = ? LIMIT 1'),
    get: db.prepareSync('SELECT value FROM entry WHERE key = ?'),
    put: db.prepareSync(
      'INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)',
    ),
    del: db.prepareSync('DELETE FROM entry WHERE key = ?'),
  };
}

type StoreEntry = {
  readonly lock: RWLock;
  readonly db: SQLiteDatabase;
  readonly statements: Statements;
  refCount: number;
};

// Global map to share database connections between multiple store instances with the same name
const stores = new Map<string, StoreEntry>();

/**
 * Gets an existing store entry or creates a new one if it doesn't exist.
 * This implements the shared connection pattern where multiple stores with the same
 * name share the same database connection, lock, and prepared statements.
 */
function getOrCreateEntry(
  filename: string,
  opts?: ExpoSQLiteStoreOptions,
): StoreEntry {
  const entry = stores.get(filename);

  if (entry) {
    entry.refCount++;
    return entry;
  }

  const db = openDatabaseSync(filename);

  db.execSync(`PRAGMA busy_timeout = ${opts?.busyTimeout ?? 200}`);
  db.execSync(`PRAGMA journal_mode = '${opts?.journalMode ?? 'WAL'}'`);
  db.execSync(`PRAGMA synchronous = '${opts?.synchronous ?? 'NORMAL'}'`);
  db.execSync(`PRAGMA read_uncommitted = ${Boolean(opts?.readUncommitted)}`);

  db.execSync(`
    CREATE TABLE IF NOT EXISTS entry (
      key TEXT PRIMARY KEY, 
      value TEXT NOT NULL
    ) WITHOUT ROWID
  `);

  const statements = createStatements(db);
  const lock = new RWLock();

  const newEntry: StoreEntry = {lock, db, statements, refCount: 1};
  stores.set(filename, newEntry);
  return newEntry;
}

/**
 * Decrements the reference count for a shared store and cleans up resources
 * when the last reference is released.
 */
function decrementStoreRefCount(
  filename: string,
  statements: Statements,
  db: SQLiteDatabase,
): void {
  const entry = stores.get(filename);
  if (entry) {
    entry.refCount--;
    if (entry.refCount <= 0) {
      for (const stmt of Object.values(statements)) {
        stmt.finalizeSync();
      }
      db.closeSync();
      stores.delete(filename);
    }
  }
}

export function clearAllNamedExpoSQLiteStoresForTesting(): void {
  for (const entry of stores.values()) {
    entry.db.closeSync();
  }
  stores.clear();
}

export function dropExpoSQLiteStore(name: string): Promise<void> {
  const filename = safeFilename(name);
  const entry = stores.get(filename);
  if (entry) {
    entry.db.closeSync();
    stores.delete(filename);
  }
  deleteDatabaseSync(filename);
  return Promise.resolve();
}

/**
 * SQLite-based implementation of the Store interface using expo-sqlite.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 */
export class ExpoSQLiteStore implements Store {
  readonly #db: SQLiteDatabase;
  readonly #rwLock: RWLock;
  readonly #statements: Statements;
  readonly #filename: string;
  #sharedReadTransaction = false;
  #activeReaders = 0;
  #closed = false;

  constructor(name: string, opts?: ExpoSQLiteStoreOptions) {
    const filename = safeFilename(name);
    this.#filename = filename;

    const entry = getOrCreateEntry(filename, opts);
    this.#rwLock = entry.lock;
    this.#db = entry.db;
    this.#statements = entry.statements;
  }

  async read(): Promise<Read> {
    if (this.#closed) {
      throw new Error('Store is closed');
    }

    const release = await this.#rwLock.read();

    // Start shared read transaction if this is the first reader
    // This ensures consistent reads across all concurrent readers
    if (this.#activeReaders === 0 && !this.#sharedReadTransaction) {
      this.#db.execSync('BEGIN');
      this.#sharedReadTransaction = true;
    }
    this.#activeReaders++;

    return new ExpoSQLiteRead(() => {
      this.#activeReaders--;
      // Commit shared read transaction when last reader finishes
      if (this.#activeReaders === 0 && this.#sharedReadTransaction) {
        this.#db.execSync('COMMIT');
        this.#sharedReadTransaction = false;
      }
      release();
    }, this.#statements);
  }

  async write(): Promise<Write> {
    if (this.#closed) {
      throw new Error('Store is closed');
    }

    const release = await this.#rwLock.write();

    // At this point, RWLock guarantees no active readers
    // The last reader would have already committed the shared transaction

    this.#db.execSync('BEGIN IMMEDIATE');

    return new ExpoSQLiteWrite(release, this.#statements, this.#db);
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }

    const writeRelease = await this.#rwLock.write();

    if (this.#sharedReadTransaction) {
      this.#db.execSync('COMMIT');
      this.#sharedReadTransaction = false;
    }

    // Handle reference counting for shared stores - only close database
    // when this is the last store instance using it
    decrementStoreRefCount(this.#filename, this.#statements, this.#db);

    this.#closed = true;
    writeRelease();
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class ExpoSQLiteRead implements Read {
  readonly #release: () => void;
  readonly #statements: Statements;
  #closed = false;

  constructor(release: () => void, statements: Statements) {
    this.#release = release;
    this.#statements = statements;
  }

  #throwIfClosed(): void {
    if (this.#closed) {
      throw new Error('Read transaction is closed');
    }
  }

  async has(key: string): Promise<boolean> {
    this.#throwIfClosed();
    const result = await this.#statements.has.executeAsync([key]);
    const rows = await result.getAllAsync();
    return rows.length > 0;
  }

  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    this.#throwIfClosed();
    const result = await this.#statements.get.executeAsync([key]);
    const rows = (await result.getAllAsync()) as {value: string}[];

    if (rows.length === 0) {
      return undefined;
    }

    const parsedValue = JSON.parse(rows[0].value) as ReadonlyJSONValue;
    return deepFreeze(parsedValue);
  }

  release(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#release();
    }
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class ExpoSQLiteWrite implements Write {
  readonly #release: () => void;
  readonly #statements: Statements;
  readonly #db: SQLiteDatabase;
  #committed = false;
  #closed = false;

  constructor(release: () => void, statements: Statements, db: SQLiteDatabase) {
    this.#release = release;
    this.#statements = statements;
    this.#db = db;
  }

  #throwIfClosed(): void {
    if (this.#closed) {
      throw new Error('Write transaction is closed');
    }
  }

  async has(key: string): Promise<boolean> {
    this.#throwIfClosed();
    const result = await this.#statements.has.executeAsync([key]);
    const rows = await result.getAllAsync();
    return rows.length > 0;
  }

  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    this.#throwIfClosed();
    const result = await this.#statements.get.executeAsync([key]);
    const rows = (await result.getAllAsync()) as {value: string}[];

    if (rows.length === 0) {
      return undefined;
    }

    const parsedValue = JSON.parse(rows[0].value) as ReadonlyJSONValue;
    return deepFreeze(parsedValue);
  }

  async put(key: string, value: ReadonlyJSONValue): Promise<void> {
    this.#throwIfClosed();
    await this.#statements.put.executeAsync([key, JSON.stringify(value)]);
  }

  async del(key: string): Promise<void> {
    this.#throwIfClosed();
    await this.#statements.del.executeAsync([key]);
  }

  // eslint-disable-next-line require-await
  async commit(): Promise<void> {
    this.#throwIfClosed();

    this.#db.execSync('COMMIT');
    this.#committed = true;
  }

  release(): void {
    if (!this.#closed) {
      this.#closed = true;

      if (!this.#committed) {
        this.#db.execSync('ROLLBACK');
      }

      this.#release();
    }
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export function expoSQLiteStoreProvider(
  opts?: ExpoSQLiteStoreOptions,
): StoreProvider {
  return {
    create: name => new ExpoSQLiteStore(name, opts),
    drop: name => dropExpoSQLiteStore(name),
  };
}
