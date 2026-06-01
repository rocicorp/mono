import {RWLock} from '@rocicorp/lock';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Read, Store, Write} from './store.ts';
import {
  maybeTransactionIsClosedRejection,
  throwIfStoreClosed,
  transactionError,
} from './throw-if-closed.ts';
import {deleteSentinel, WriteImplBase} from './write-impl-base.ts';

/**
 * A SQLite prepared statement.
 */
export interface PreparedStatement {
  exec(params: string[]): Promise<void>;
  all(params: string[]): Promise<unknown[][]>;
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

  // for PRAGMA statements, schema creation and transaction control.
  execSync(sql: string): void;
}

export type CreateSQLiteDatabase = (
  filename: string,
  opts?: SQLiteStoreOptions,
) => SQLiteDatabase;

/**
 * SQLite-based implementation of the Store interface using a configurable delegate.
 * Supports shared connections between multiple store instances with the same name,
 * providing efficient resource utilization and proper transaction isolation.
 * Uses parameterized queries for safety and performance.
 */
export class SQLiteStore implements Store {
  readonly #filename: string;
  readonly #entry: StoreEntry;

  #closed = false;

  constructor(
    name: string,
    create: CreateSQLiteDatabase,
    opts?: SQLiteStoreOptions,
  ) {
    this.#filename = resolveFilename(name, opts);
    this.#entry = getOrCreateEntry(this.#filename, create, opts);
  }

  async read(): Promise<Read> {
    throwIfStoreClosed(this);

    const entry = this.#entry;
    const {db, lock, preparedStatements} = entry;
    const release = await lock.read();

    // Start shared read transaction if this is the first reader
    // This ensures consistent reads across all concurrent readers
    if (entry.activeReaders === 0) {
      db.execSync('BEGIN');
    }
    entry.activeReaders++;

    return new SQLiteStoreRead(() => {
      entry.activeReaders--;
      // Commit shared read transaction when last reader finishes
      if (entry.activeReaders === 0) {
        db.execSync('COMMIT');
      }
      release();
    }, preparedStatements);
  }

  async write(): Promise<Write> {
    throwIfStoreClosed(this);

    const {lock, db, preparedStatements} = this.#entry;
    const release = await lock.write();

    // At this point, RWLock guarantees no active readers
    // The last reader would have already committed the shared transaction

    db.execSync('BEGIN IMMEDIATE');

    return new SQLiteWrite(release, db, preparedStatements);
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }

    const {lock, db} = this.#entry;
    // Wait for all readers and writers to finish.
    const writeRelease = await lock.write();

    // Handle reference counting for shared stores - only close database
    // when this is the last store instance using it
    decrementStoreRefCount(this.#filename, db);

    this.#closed = true;
    writeRelease();
  }

  get closed(): boolean {
    return this.#closed;
  }
}

export function safeFilename(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

function resolveFilename(name: string, opts?: SQLiteStoreOptions): string {
  const safe = safeFilename(name);
  const dir = opts?.directory;
  return dir ? `${dir}/${safe}` : safe;
}

export type PreparedStatements = {
  has: PreparedStatement;
  get: PreparedStatement;
  hasMany: PreparedStatement;
  getMany: PreparedStatement;
  put: PreparedStatement;
  del: PreparedStatement;
};

export interface SQLiteStoreOptions {
  // Common options
  busyTimeout?: number;
  journalMode?: 'WAL' | 'DELETE';
  synchronous?: 'NORMAL' | 'FULL';
  readUncommitted?: boolean;
  /** Directory in which to create the SQLite file. Defaults to the process CWD. */
  directory?: string | undefined;
}

/**
 * Common database setup logic shared between expo-sqlite and op-sqlite implementations.
 * Configures SQLite pragmas, creates the entry table, and prepares common statements.
 */

export function setupDatabase(
  delegate: SQLiteDatabase,
  opts?: SQLiteStoreOptions,
): PreparedStatements {
  // Configure SQLite pragmas for optimal performance
  delegate.execSync(`PRAGMA busy_timeout = ${opts?.busyTimeout ?? 200}`);
  delegate.execSync(`PRAGMA journal_mode = '${opts?.journalMode ?? 'WAL'}'`);
  delegate.execSync(`PRAGMA synchronous = '${opts?.synchronous ?? 'NORMAL'}'`);
  delegate.execSync(
    `PRAGMA read_uncommitted = ${Boolean(opts?.readUncommitted)}`,
  );

  // Create the entry table
  delegate.execSync(`
    CREATE TABLE IF NOT EXISTS entry (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) WITHOUT ROWID
  `);

  // Prepare common statements
  return {
    has: delegate.prepare(`SELECT 1 FROM entry WHERE key = ? LIMIT 1`),
    get: delegate.prepare('SELECT value FROM entry WHERE key = ?'),
    hasMany: delegate.prepare(
      `SELECT key FROM entry WHERE key IN (SELECT value FROM json_each(?))`,
    ),
    getMany: delegate.prepare(
      `SELECT key, value FROM entry WHERE key IN (SELECT value FROM json_each(?))`,
    ),
    put: delegate.prepare(
      `INSERT OR REPLACE INTO entry (key, value)
   SELECT e.value->>0, e.value->1 FROM json_each(?) e`,
    ),
    del: delegate.prepare(
      `DELETE FROM entry WHERE key IN (SELECT value FROM json_each(?))`,
    ),
  };
}

// Callbacks are stored as striped pairs: [resolve, reject, resolve, reject, ...]
const CB_STRIDE = 2;
const CB_RESOLVE = 0;
const CB_REJECT = 1;

type GetResolve = (v: ReadonlyJSONValue | undefined) => void;
type HasResolve = (v: boolean) => void;
type Reject = (e: unknown) => void;

function rejectAll(callbacks: unknown[], e: unknown): void {
  for (let i = CB_REJECT; i < callbacks.length; i += CB_STRIDE) {
    (callbacks[i] as Reject)(e);
  }
}

function parseRawValue(raw: string | undefined): ReadonlyJSONValue | undefined {
  return raw === undefined
    ? undefined
    : deepFreeze(JSON.parse(raw) as ReadonlyJSONValue);
}

function resolveGet(
  resolve: GetResolve,
  reject: Reject,
  raw: string | undefined,
): void {
  try {
    resolve(parseRawValue(raw));
  } catch (e) {
    reject(e);
  }
}

async function flushGets(
  keys: string[],
  callbacks: unknown[],
  ps: PreparedStatements,
): Promise<void> {
  let rows: unknown[][];
  try {
    rows =
      keys.length === 1
        ? await ps.get.all([keys[0]])
        : await ps.getMany.all([JSON.stringify(keys)]);
  } catch (e) {
    rejectAll(callbacks, e);
    return;
  }
  if (keys.length === 1) {
    resolveGet(
      callbacks[CB_RESOLVE] as GetResolve,
      callbacks[CB_REJECT] as Reject,
      rows[0]?.[0] as string | undefined,
    );
    return;
  }
  const resultMap = new Map(rows as [string, string][]);
  for (let i = 0; i < keys.length; i++) {
    resolveGet(
      callbacks[i * CB_STRIDE + CB_RESOLVE] as GetResolve,
      callbacks[i * CB_STRIDE + CB_REJECT] as Reject,
      resultMap.get(keys[i]),
    );
  }
}

async function flushHas(
  keys: string[],
  callbacks: unknown[],
  ps: PreparedStatements,
): Promise<void> {
  let rows: unknown[][];
  try {
    rows =
      keys.length === 1
        ? await ps.has.all([keys[0]])
        : await ps.hasMany.all([JSON.stringify(keys)]);
  } catch (e) {
    rejectAll(callbacks, e);
    return;
  }
  if (keys.length === 1) {
    (callbacks[CB_RESOLVE] as HasResolve)(rows.length > 0);
    return;
  }
  const existingKeys = new Set(rows.map(row => row[0] as string));
  for (let i = 0; i < keys.length; i++) {
    (callbacks[i * CB_STRIDE + CB_RESOLVE] as HasResolve)(
      existingKeys.has(keys[i]),
    );
  }
}

export class SQLiteStoreRead implements Read {
  readonly #release: () => void;
  readonly #preparedStatements: PreparedStatements;
  #closed = false;
  #pendingGetKeys: string[] = [];
  #pendingGetCallbacks: unknown[] = [];
  #pendingHasKeys: string[] = [];
  #pendingHasCallbacks: unknown[] = [];
  #scheduled = false;

  constructor(release: () => void, preparedStatements: PreparedStatements) {
    this.#release = release;
    this.#preparedStatements = preparedStatements;
  }

  has(key: string): Promise<boolean> {
    return (
      maybeTransactionIsClosedRejection(this) ??
      new Promise((resolve, reject) => {
        this.#pendingHasKeys.push(key);
        this.#pendingHasCallbacks.push(resolve, reject);
        this.#scheduleLookup();
      })
    );
  }

  get(key: string): Promise<ReadonlyJSONValue | undefined> {
    return (
      maybeTransactionIsClosedRejection(this) ??
      new Promise((resolve, reject) => {
        this.#pendingGetKeys.push(key);
        this.#pendingGetCallbacks.push(resolve, reject);
        this.#scheduleLookup();
      })
    );
  }

  #scheduleLookup(): void {
    if (!this.#scheduled) {
      this.#scheduled = true;
      queueMicrotask(() => {
        this.#scheduled = false;

        const ps = this.#preparedStatements;
        const getKeys = this.#pendingGetKeys;
        this.#pendingGetKeys = [];
        const getCallbacks = this.#pendingGetCallbacks;
        this.#pendingGetCallbacks = [];
        const hasKeys = this.#pendingHasKeys;
        this.#pendingHasKeys = [];
        const hasCallbacks = this.#pendingHasCallbacks;
        this.#pendingHasCallbacks = [];

        if (this.#closed) {
          const e = transactionError();
          rejectAll(getCallbacks, e);
          rejectAll(hasCallbacks, e);
          return;
        }

        if (getKeys.length > 0) {
          void flushGets(getKeys, getCallbacks, ps);
        }
        if (hasKeys.length > 0) {
          void flushHas(hasKeys, hasCallbacks, ps);
        }
      });
    }
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

export class SQLiteWrite extends WriteImplBase implements Write {
  readonly #release: () => void;
  readonly #dbDelegate: SQLiteDatabase;
  readonly #preparedStatements: PreparedStatements;
  #committed = false;
  #closed = false;

  constructor(
    release: () => void,
    dbDelegate: SQLiteDatabase,
    preparedStatements: PreparedStatements,
  ) {
    super(new SQLiteStoreRead(() => undefined, preparedStatements));
    this.#release = release;
    this.#dbDelegate = dbDelegate;
    this.#preparedStatements = preparedStatements;
  }

  async commit(): Promise<void> {
    if (this.#closed) {
      throw transactionError();
    }

    const deleteKeys: string[] = [];
    for (const entry of this._pending) {
      if (entry[1] === deleteSentinel) {
        deleteKeys.push(entry[0]);
        this._pending.delete(entry[0]);
      }
    }

    const delP =
      deleteKeys.length > 0
        ? this.#preparedStatements.del.exec([JSON.stringify(deleteKeys)])
        : undefined;
    const putP =
      this._pending.size > 0
        ? this.#preparedStatements.put.exec([
            JSON.stringify([...this._pending]),
          ])
        : undefined;

    if (putP) await putP;
    if (delP) await delP;

    this.#dbDelegate.execSync('COMMIT');
    this._pending.clear();
    this.#committed = true;
  }

  release(): void {
    if (!this.#closed) {
      this.#closed = true;
      super.release();
      let rollbackError: unknown;
      if (!this.#committed) {
        try {
          this.#dbDelegate.execSync('ROLLBACK');
        } catch (e) {
          rollbackError = e;
        }
      }
      this.#release();
      if (rollbackError !== undefined) {
        throw rollbackError;
      }
    }
  }

  get closed(): boolean {
    return this.#closed;
  }
}

type StoreEntry = {
  readonly lock: RWLock;
  readonly db: SQLiteDatabase;
  refCount: number;
  activeReaders: number;
  preparedStatements: PreparedStatements;
};

// Global map to share database connections between multiple store instances with the same name
const stores = new Map<string, StoreEntry>();

/**
 * Gets an existing store entry or creates a new one if it doesn't exist.
 * This implements the shared connection pattern where multiple stores with the same
 * name share the same database connection, lock, and delegate.
 */
function getOrCreateEntry(
  filename: string,
  create: (filename: string, opts?: SQLiteStoreOptions) => SQLiteDatabase,
  opts?: SQLiteStoreOptions,
): StoreEntry {
  const entry = stores.get(filename);

  if (entry) {
    entry.refCount++;
    return entry;
  }

  const dbDelegate = create(filename, opts);
  const preparedStatements = setupDatabase(dbDelegate, opts);

  const lock = new RWLock();

  const newEntry: StoreEntry = {
    lock,
    db: dbDelegate,
    refCount: 1,
    activeReaders: 0,
    preparedStatements,
  };
  stores.set(filename, newEntry);
  return newEntry;
}

/**
 * Decrements the reference count for a shared store and cleans up resources
 * when the last reference is released.
 */

function decrementStoreRefCount(
  filename: string,
  dbDelegate: SQLiteDatabase,
): void {
  const entry = stores.get(filename);
  if (entry) {
    entry.refCount--;
    if (entry.refCount <= 0) {
      dbDelegate.close();
      stores.delete(filename);
    }
  }
}
export function clearAllNamedStoresForTesting(): void {
  for (const entry of stores.values()) {
    entry.db.close();
  }
  stores.clear();
}

export function dropStore(
  name: string,
  createDelegate: (
    filename: string,
    opts?: SQLiteStoreOptions,
  ) => SQLiteDatabase,
  opts?: SQLiteStoreOptions,
): Promise<void> {
  const filename = resolveFilename(name, opts);
  const entry = stores.get(filename);
  if (entry) {
    try {
      entry.db.close();
    } catch {
      // Ignore close errors
    }
    stores.delete(filename);
  }

  // Create a temporary delegate to handle database deletion
  const tempDelegate = createDelegate(filename, opts);
  try {
    // we close the db before destroying it - this
    // caused an issue with expo-sqlite since it requires this
    tempDelegate.close();
  } catch {
    // Ignore close errors
  }
  try {
    tempDelegate.destroy();
  } catch {
    // Destroy errors shouldn't be fatal; the file may already be gone or locked
  }

  return Promise.resolve();
}
