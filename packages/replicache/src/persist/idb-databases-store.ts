import type {LogContext} from '@rocicorp/logger';
import {
  assertNumber,
  assertObject,
  assertString,
} from '../../../shared/src/asserts.ts';
import {getBrowserGlobal} from '../../../shared/src/browser-env.ts';
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

function isIndexedDBDatabase(value: unknown): value is IndexedDBDatabase {
  try {
    assertIndexedDBDatabase(value);
    return true;
  } catch {
    return false;
  }
}

export class IDBDatabasesStore {
  readonly #kvStore: Store;
  readonly #lc: LogContext | undefined;

  constructor(createKVStore: CreateStore, lc?: LogContext | undefined) {
    this.#kvStore = createKVStore(getIDBDatabasesDBName());
    this.#lc = lc;
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
      const oldDbRecord = await getDatabases(write, this.#lc);
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
      const oldDbRecord = await getDatabases(write, this.#lc);
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
    return withRead(this.#kvStore, read => getDatabases(read, this.#lc));
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

const EMPTY_RECORD: IndexedDBDatabaseRecord = Object.freeze({});

/**
 * Reads the registry, leaving out any entry that is not a valid database
 * record (or whose key is not its name), and reading a record that is not an
 * object as empty. Every open and every drop reads through here, so one
 * malformed entry must not make them all fail; `putDatabase` and
 * `deleteDatabases` write the record back without it, which repairs the
 * registry. A database whose entry is left out is no longer collected, and
 * nothing could reach it while the entry failed to read either.
 */
async function getDatabases(
  read: Read,
  lc: LogContext | undefined,
): Promise<IndexedDBDatabaseRecord> {
  const dbRecord = await read.get(DBS_KEY);
  if (dbRecord === undefined) {
    return EMPTY_RECORD;
  }
  if (
    typeof dbRecord !== 'object' ||
    dbRecord === null ||
    Array.isArray(dbRecord)
  ) {
    lc?.warn?.('Ignoring the databases registry: it is not an object.');
    return EMPTY_RECORD;
  }
  const valid: Record<IndexedDBName, IndexedDBDatabase> = {};
  let malformed = false;
  for (const [name, db] of Object.entries(dbRecord)) {
    if (isIndexedDBDatabase(db) && db.name === name) {
      valid[name] = db;
    } else {
      malformed = true;
      lc?.warn?.(
        `Ignoring malformed entry "${name}" in the databases registry.`,
      );
    }
  }
  return malformed ? valid : (dbRecord as IndexedDBDatabaseRecord);
}
