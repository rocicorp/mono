import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Store as KVStore} from './store.ts';

export interface SQLResultSetRowList {
  length: number;
  item(index: number): {value: string};
}

export abstract class SQLiteTransaction {
  abstract start(readonly?: boolean): Promise<void>;

  abstract execute(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ): Promise<SQLResultSetRowList>;

  abstract commit(): Promise<void>;
}

export interface SQLDatabase {
  transaction: () => SQLiteTransaction;
  destroy: () => Promise<void>;
  close: () => Promise<void>;
}

export interface GenericSQLiteDatabaseManager {
  open: (name: string) => Promise<SQLDatabase>;
}

/**
 * A SQLite-based Store implementation.
 *
 * This store provides a generic SQLite implementation that can be used with different
 * SQLite providers (like expo-sqlite, better-sqlite3, etc). It implements the Store
 * interface using a single 'entry' table with key-value pairs.
 *
 * The store ensures strict serializable transactions by using SQLite's native
 * transaction support. Read transactions use SQLite's READ mode while write
 * transactions use the default mode.
 */
export class ReplicacheGenericStore implements KVStore {
  readonly #name: string;
  readonly #dbm: SQLiteDatabaseManager;
  #closed = false;

  constructor(name: string, dbm: SQLiteDatabaseManager) {
    this.#name = name;
    this.#dbm = dbm;
  }

  async read() {
    const db = await this._getDb();
    const tx = db.transaction();
    await tx.start(true);
    return new SQLiteStoreRead(tx);
  }

  async withRead<R>(
    fn: (read: Awaited<ReturnType<KVStore['read']>>) => R | Promise<R>,
  ): Promise<R> {
    const read = await this.read();
    try {
      return await fn(read);
    } finally {
      await read.release();
    }
  }

  async write(): Promise<Awaited<ReturnType<KVStore['write']>>> {
    const db = await this._getDb();
    const tx = db.transaction();
    await tx.start(false);
    return new SQLiteStoreWrite(tx);
  }

  async withWrite<R>(
    fn: (write: Awaited<ReturnType<KVStore['write']>>) => R | Promise<R>,
  ): Promise<R> {
    const write = await this.write();
    try {
      return await fn(write);
    } finally {
      write.release();
    }
  }

  async close() {
    await this.#dbm.close(this.#name);
    this.#closed = true;
  }

  get closed(): boolean {
    return this.#closed;
  }

  private _getDb() {
    return this.#dbm.open(this.#name);
  }
}

/**
 * Creates a function that returns new SQLite store instances.
 * This is the main entry point for using the SQLite store implementation.
 *
 * @param db The SQLite database manager implementation
 * @returns A function that creates new store instances
 */
export function getCreateReplicacheSQLiteKVStore(db: SQLiteDatabaseManager) {
  return (name: string) => new ReplicacheGenericStore(name, db);
}

/**
 * Implementation of the Read interface for SQLite stores.
 * Provides read-only access to the underlying SQLite database.
 */
export class SQLiteStoreRead implements Awaited<ReturnType<KVStore['read']>> {
  protected _tx: SQLiteTransaction | null;

  constructor(tx: SQLiteTransaction) {
    this._tx = tx;
  }

  async has(key: string) {
    const unsafeValue = await this.#getSql(key);
    return unsafeValue === undefined;
  }

  async get(key: string) {
    const unsafeValue = await this.#getSql(key);
    if (unsafeValue === undefined) return;
    const parsedValue = JSON.parse(unsafeValue) as ReadonlyJSONValue;
    const frozenValue = deepFreeze(parsedValue);
    return frozenValue;
  }

  async release() {
    const tx = this._assertTx();
    await tx.commit();
    this._tx = null;
  }

  get closed(): boolean {
    return this._tx === null;
  }

  async #getSql(key: string) {
    const rows = await this._assertTx().execute(
      'SELECT value FROM entry WHERE key = ?',
      [key],
    );

    if (rows.length === 0) return undefined;

    return rows.item(0).value;
  }

  protected _assertTx() {
    if (this._tx === null) throw new Error('Transaction is closed');
    return this._tx;
  }
}

/**
 * Implementation of the Write interface for SQLite stores.
 * Extends SQLiteStoreRead to provide write capabilities.
 */
export class SQLiteStoreWrite
  extends SQLiteStoreRead
  implements Awaited<ReturnType<KVStore['write']>>
{
  async put(key: string, value: ReadonlyJSONValue) {
    const jsonValueString = JSON.stringify(value);
    await this._assertTx().execute(
      'INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)',
      [key, jsonValueString],
    );
  }

  async del(key: string) {
    await this._assertTx().execute('DELETE FROM entry WHERE key = ?', [key]);
  }

  async commit() {
    // Do nothing and wait for release.
  }
}

/**
 * Manages SQLite database instances and their lifecycle.
 * Handles database creation, schema setup, and cleanup.
 */
export class SQLiteDatabaseManager {
  readonly #dbm: GenericSQLiteDatabaseManager;
  #dbInstances = new Map<string, {db: SQLDatabase; state: 'open' | 'closed'}>();

  constructor(dbm: GenericSQLiteDatabaseManager) {
    this.#dbm = dbm;
  }

  async open(name: string) {
    const dbInstance = this.#dbInstances.get(name);
    if (dbInstance?.state === 'open') return dbInstance.db;

    const newDb = await this.#dbm.open(`replicache-${name}.sqlite`);
    if (!dbInstance) {
      await this.#setupSchema(newDb);
      this.#dbInstances.set(name, {state: 'open', db: newDb});
    } else {
      dbInstance.state = 'open';
    }

    return newDb;
  }

  async close(name: string) {
    const dbInstance = this.#dbInstances.get(name);
    if (!dbInstance) return;

    await dbInstance.db.close();
    dbInstance.state = 'closed';
  }

  async truncate(name: string) {
    const db = await this.open(name);
    const tx = db.transaction();
    await tx.start(false);
    await tx.execute('DELETE FROM entry', []);
    await tx.commit();
  }

  async destroy(name: string) {
    const dbInstance = this.#dbInstances.get(name);
    if (!dbInstance) return;

    await dbInstance.db.destroy();
    this.#dbInstances.delete(name);
  }

  async #setupSchema(db: SQLDatabase) {
    const tx = db.transaction();
    await tx.start(false);
    await tx.execute(
      'CREATE TABLE IF NOT EXISTS entry (key TEXT PRIMARY KEY, value TEXT)',
      [],
    );
    await tx.commit();
  }
}
