/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {randomUint64} from '../../../shared/src/random-uint64.ts';
import {dropIDBStoreWithMemFallback} from '../kv/idb-store-with-mem-fallback.ts';

const IDB_DATABASES_VERSION = 0;
const IDB_DATABASES_DB_NAME = 'replicache-dbs-v' + IDB_DATABASES_VERSION;

let testNamespace = '';

/** Namespace db name in test to isolate tests' indexeddb state. */
export function setupForTest(): void {
  testNamespace = randomUint64().toString(36);
}

export function teardownForTest(): Promise<void> {
  const idbDatabasesDBName = getIDBDatabasesDBName();
  testNamespace = '';
  return dropIDBStoreWithMemFallback(idbDatabasesDBName);
}

export function getIDBDatabasesDBName(): string {
  return testNamespace + IDB_DATABASES_DB_NAME;
}
