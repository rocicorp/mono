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
  name: IndexedDBName;
  replicacheName: string;
  replicacheFormatVersion: number;
  schemaVersion: string;
};

export type IndexedDBDatabaseRecord = Record<IndexedDBName, IndexedDBDatabase>;

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
}

export class IDBDatabasesStore {
  private readonly _kvStore: kv.Store;

  constructor(
    createKVStore: (name: string) => kv.Store = name => new kv.IDBStore(name),
  ) {
    this._kvStore = createKVStore(IDB_DATABASES_DB_NAME);
  }

  putDatabase(db: IndexedDBDatabase): Promise<IndexedDBDatabaseRecord> {
    return this._kvStore.withWrite(async write => {
      const dbRecord = await this._getDatabases(write);
      dbRecord[db.name] = db;
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

  getDatabases(): Promise<IndexedDBDatabaseRecord> {
    return this._kvStore.withRead(async read => this._getDatabases(read));
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
