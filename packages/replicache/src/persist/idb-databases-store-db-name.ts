import {dropIDBStoreWithMemFallback} from '../kv/idb-store-with-mem-fallback.js';
import {uuid} from '../uuid.js';

const IDB_DATABASES_VERSION = 0;
const IDB_DATABASES_DB_NAME = 'replicache-dbs-v' + IDB_DATABASES_VERSION;

let testNamespace = '';

/** Namespace db name in test to isolate tests' indexeddb state. */
export function setupForTest(): void {
  testNamespace = uuid();
}

export function teardownForTest(): Promise<void> {
  const idbDatabasesDBName = getIDBDatabasesDBName();
  testNamespace = '';
  return dropIDBStoreWithMemFallback(idbDatabasesDBName);
}

export function getIDBDatabasesDBName(): string {
  return testNamespace + IDB_DATABASES_DB_NAME;
}
