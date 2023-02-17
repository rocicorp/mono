import {dropStore as dropIDBStore} from '../kv/idb-util.js';
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
  return dropIDBStore(idbDatabasesDBName);
}

export function getIDBDatabasesDBName(): string {
  return testNamespace + IDB_DATABASES_DB_NAME;
}
