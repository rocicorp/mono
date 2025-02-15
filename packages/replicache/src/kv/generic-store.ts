import type {KVStore, ReadonlyJSONValue} from '../mod.ts';

export class ReplicacheGenericStore implements KVStore {
  private _closed = false;

  constructor(
    private readonly name: string,
    private readonly _dbm: ReplicacheGenericSQLiteDatabaseManager,
  ) {}

  async read() {
    const db = await this._getDb();
    const tx = db.transaction();
    await tx.start(true);
    return new ReplicacheGenericSQLiteReadImpl(tx);
  }

  async withRead<R>(
    fn: (read: Awaited<ReturnType<KVStore['read']>>) => R | Promise<R>,
  ): Promise<R> {
    const read = await this.read();
    try {
      return await fn(read);
    } finally {
      read.release();
    }
  }

  async write(): Promise<Awaited<ReturnType<KVStore['write']>>> {
    const db = await this._getDb();
    const tx = db.transaction();
    await tx.start(false);
    return new ReplicacheGenericSQLiteWriteImpl(tx);
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
    await this._dbm.close(this.name);
    this._closed = true;
  }

  get closed(): boolean {
    return this._closed;
  }

  private async _getDb() {
    return await this._dbm.open(this.name);
  }
}

export function getCreateReplicacheSQLiteKVStore(
  db: ReplicacheGenericSQLiteDatabaseManager,
) {
  return (name: string) => new ReplicacheGenericStore(name, db);
}

export class ReplicacheGenericSQLiteReadImpl
  implements Awaited<ReturnType<KVStore['read']>>
{
  protected _tx: ReplicacheGenericSQLiteTransaction | null;

  constructor(tx: ReplicacheGenericSQLiteTransaction) {
    this._tx = tx;
  }

  async has(key: string) {
    const unsafeValue = await this._getSql(key);
    return unsafeValue === undefined;
  }

  async get(key: string) {
    const unsafeValue = await this._getSql(key);
    if (unsafeValue === undefined) return;
    const parsedValue = JSON.parse(unsafeValue) as ReadonlyJSONValue;
    // @ts-ignore
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

  private async _getSql(key: string) {
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

export class ReplicacheGenericSQLiteWriteImpl
  extends ReplicacheGenericSQLiteReadImpl
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

export interface GenericSQLResultSetRowList {
  length: number;
  item(index: number): any;
}

export abstract class ReplicacheGenericSQLiteTransaction {
  public abstract start(readonly?: boolean): Promise<void>;

  public abstract execute(
    sqlStatement: string,
    args?: (string | number | null)[] | undefined,
  ): Promise<GenericSQLResultSetRowList>;

  public abstract commit(): Promise<void>;
}

export interface GenericSQLDatabase {
  transaction: () => ReplicacheGenericSQLiteTransaction;
  destroy: () => Promise<void>;
  close: () => Promise<void>;
}

export interface GenericDatabaseManager {
  open: (name: string) => Promise<GenericSQLDatabase>;
}

export class ReplicacheGenericSQLiteDatabaseManager {
  private _dbInstances = new Map<
    string,
    {db: GenericSQLDatabase; state: 'open' | 'closed'}
  >();

  constructor(private readonly _dbm: GenericDatabaseManager) {}

  async open(name: string) {
    const dbInstance = this._dbInstances.get(name);
    if (dbInstance?.state === 'open') return dbInstance.db;

    const newDb = await this._dbm.open(`replicache-${name}.sqlite`);
    if (!dbInstance) {
      await this._setupSchema(newDb);
      this._dbInstances.set(name, {state: 'open', db: newDb});
    } else {
      dbInstance.state = 'open';
    }

    return newDb;
  }

  async close(name: string) {
    const dbInstance = this._dbInstances.get(name);
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
    const dbInstances = this._dbInstances.get(name);
    if (!dbInstances) return;

    await dbInstances.db.destroy();
    this._dbInstances.delete(name);
  }

  private async _setupSchema(db: GenericSQLDatabase) {
    const tx = db.transaction();
    await tx.start(false);
    await tx.execute(
      'CREATE TABLE IF NOT EXISTS entry (key TEXT PRIMARY KEY, value TEXT)',
      [],
    );
    await tx.commit();
  }
}
