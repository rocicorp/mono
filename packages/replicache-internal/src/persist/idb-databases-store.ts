import {assert, assertNumber, assertObject, assertString} from '../asserts';
import * as kv from '../kv/mod';
import {uuid} from '../uuid';

const IDB_DATABASES_VERSION = 0;
const IDB_DATABASES_DB_NAME = 'replicache-dbs-v' + IDB_DATABASES_VERSION;
const DBS_KEY = 'dbs';
const PROFILE_ID_KEY = 'profileId';

let testNamespace = '';

/** Namespace db name in test to isolate tests' indexeddb state. */
export function setupForTest(): void {
  testNamespace = uuid();
}

export function teardownForTest(): Promise<void> {
  const idbDatabasesDBName = getIDBDatabasesDBName();
  testNamespace = '';
  return kv.dropIDBStore(idbDatabasesDBName);
}

function getIDBDatabasesDBName(): string {
  return testNamespace + IDB_DATABASES_DB_NAME;
}

// TODO: make an opaque type
export type IndexedDBName = string;

export type IndexedDBDatabase = {
  readonly name: IndexedDBName;
  readonly replicacheName: string;
  readonly replicacheFormatVersion: number;
  readonly schemaVersion: string;
  readonly lastOpenedTimestampMS?: number;
};

export type IndexedDBDatabaseRecord = {
  readonly [name: IndexedDBName]: IndexedDBDatabase;
};

function assertIndexedDBDatabaseRecord(
  value: unknown,
): asserts value is IndexedDBDatabaseRecord {
  assertObject(value);
  for (const [name, db] of Object.entries(value)) {
    assertString(name);
    assertIndexedDBDatabase(db);
    assert(name === db.name);
  }
}

function assertIndexedDBDatabase(
  value: unknown,
): asserts value is IndexedDBDatabase {
  assertObject(value);
  assertString(value.name);
  assertString(value.replicacheName);
  assertNumber(value.replicacheFormatVersion);
  assertString(value.schemaVersion);
  if (value.lastOpenedTimestampMS !== undefined) {
    assertNumber(value.lastOpenedTimestampMS);
  }
}

export class IDBDatabasesStore {
  private readonly _kvStore: kv.Store;

  constructor(
    createKVStore: (name: string) => kv.Store = name => new kv.IDBStore(name),
  ) {
    this._kvStore = createKVStore(getIDBDatabasesDBName());
  }

  putDatabase(db: IndexedDBDatabase): Promise<IndexedDBDatabaseRecord> {
    return this._putDatabase({...db, lastOpenedTimestampMS: Date.now()});
  }

  putDatabaseForTesting(
    db: IndexedDBDatabase,
  ): Promise<IndexedDBDatabaseRecord> {
    return this._putDatabase(db);
  }

  private _putDatabase(
    db: IndexedDBDatabase,
  ): Promise<IndexedDBDatabaseRecord> {
    return this._kvStore.withWrite(async write => {
      const oldDbRecord = await this._getDatabases(write);
      const dbRecord = {
        ...oldDbRecord,
        [db.name]: db,
      };
      await write.put(DBS_KEY, dbRecord);
      await write.commit();
      return dbRecord;
    });
  }

  clearDatabases(): Promise<void> {
    return this._kvStore.withWrite(async write => {
      await write.del(DBS_KEY);
      await write.commit();
    });
  }

  deleteDatabases(names: Iterable<IndexedDBName>): Promise<void> {
    return this._kvStore.withWrite(async write => {
      const oldDbRecord = await this._getDatabases(write);
      const dbRecord = {
        ...oldDbRecord,
      };
      for (const name of names) {
        delete dbRecord[name];
      }
      await write.put(DBS_KEY, dbRecord);
      await write.commit();
    });
  }

  getDatabases(): Promise<IndexedDBDatabaseRecord> {
    return this._kvStore.withRead(read => this._getDatabases(read));
  }

  close(): Promise<void> {
    return this._kvStore.close();
  }

  private async _getDatabases(read: kv.Read): Promise<IndexedDBDatabaseRecord> {
    let dbRecord = await read.get(DBS_KEY);
    if (!dbRecord) {
      dbRecord = {};
    }
    assertIndexedDBDatabaseRecord(dbRecord);
    return dbRecord;
  }

  async getProfileID(): Promise<string> {
    return this._kvStore.withWrite(async write => {
      let profileId = await write.get(PROFILE_ID_KEY);
      if (profileId === undefined) {
        // Profile id is 'p' followed by the guid with no dashes.
        profileId = `p${uuid().replace(/-/g, '')}`;
        await write.put(PROFILE_ID_KEY, profileId);
        await write.commit();
      }
      assertString(profileId);
      return profileId;
    });
  }
}
