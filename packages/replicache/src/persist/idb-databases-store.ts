import {
  assert,
  assertNumber,
  assertObject,
  assertString,
} from '../../../shared/src/asserts.ts';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {CreateStore, Read, Store} from '../kv/store.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {getIDBDatabasesDBName} from './idb-databases-store-db-name.ts';
import {makeClientID} from './make-client-id.ts';

const DBS_KEY = 'dbs';
export const PROFILE_ID_KEY = 'profileId';

// TODO: make an opaque type
export type IndexedDBName = string;

export type IndexedDBDatabase = {
  readonly name: IndexedDBName;
  readonly replicacheName: string;
  readonly replicacheFormatVersion: number;
  readonly schemaVersion: string;
  /** @deprecated No longer used. Kept for backwards compatibility when reading old data. */
  readonly lastOpenedTimestampMS?: number | undefined;
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
  readonly #kvStore: Store;

  constructor(createKVStore: CreateStore) {
    this.#kvStore = createKVStore(getIDBDatabasesDBName());
  }

  putDatabase(db: IndexedDBDatabase): Promise<IndexedDBDatabaseRecord> {
    return this.#putDatabase(db);
  }

  putDatabaseForTesting(
    db: IndexedDBDatabase,
  ): Promise<IndexedDBDatabaseRecord> {
    return this.#putDatabase(db);
  }

  #putDatabase(db: IndexedDBDatabase): Promise<IndexedDBDatabaseRecord> {
    return withWrite(this.#kvStore, async write => {
      const oldDbRecord = await getDatabases(write);
      const dbRecord = {
        ...oldDbRecord,
        [db.name]: db,
      };
      await write.put(DBS_KEY, dbRecord);
      return dbRecord;
    });
  }

  clearDatabases(): Promise<void> {
    return withWrite(this.#kvStore, write => write.del(DBS_KEY));
  }

  deleteDatabases(names: Iterable<IndexedDBName>): Promise<void> {
    return withWrite(this.#kvStore, async write => {
      const oldDbRecord = await getDatabases(write);
      const dbRecord = {
        ...oldDbRecord,
      };
      for (const name of names) {
        delete dbRecord[name];
      }
      await write.put(DBS_KEY, dbRecord);
    });
  }

  getDatabases(): Promise<IndexedDBDatabaseRecord> {
    return withRead(this.#kvStore, getDatabases);
  }

  close(): Promise<void> {
    return this.#kvStore.close();
  }

  getProfileID(): Promise<string> {
    return withWrite(this.#kvStore, async write => {
      let profileId = await write.get(PROFILE_ID_KEY);
      if (profileId === undefined) {
        // Not in the kv store. Try localStorage in case we are using a non persistent kv store.
        const maybeLocalStorage = getBrowserGlobal('localStorage');
        if (maybeLocalStorage) {
          profileId = maybeLocalStorage.getItem(PROFILE_ID_KEY) ?? undefined;
        }

        if (profileId === undefined) {
          // Profile id is 'p' followed by a random number.
          profileId = `p${makeClientID()}`;
        }

        await write.put(PROFILE_ID_KEY, profileId);
        if (maybeLocalStorage) {
          maybeLocalStorage.setItem(PROFILE_ID_KEY, profileId);
        }
      }
      assertString(profileId);
      return profileId;
    });
  }
}

async function getDatabases(read: Read): Promise<IndexedDBDatabaseRecord> {
  let dbRecord = await read.get(DBS_KEY);
  if (!dbRecord) {
    dbRecord = deepFreeze({});
  }
  assertIndexedDBDatabaseRecord(dbRecord);
  return dbRecord;
}
