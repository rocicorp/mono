/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {afterEach, expect, test, vi} from 'vitest';
import {TestMemStore} from '../kv/test-mem-store.ts';
import {
  IDBDatabasesStore,
  type IndexedDBDatabase,
} from './idb-databases-store.ts';

afterEach(() => {
  vi.restoreAllMocks();
});

test('getDatabases with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  expect(await store.getDatabases()).to.deep.equal({});
});

test('putDatabase with no existing record in db', async () => {
  vi.setSystemTime(1);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
});

test('putDatabase updates lastOpenedTimestampMS', async () => {
  vi.setSystemTime(1);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 1),
  });

  vi.setSystemTime(2);
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
});

test('putDatabase ignores passed in lastOpenedTimestampMS', async () => {
  vi.setSystemTime(2);

  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
    lastOpenedTimestampMS: 1,
  };
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
});

function withLastOpenedTimestampMS(
  db: IndexedDBDatabase,
  lastOpenedTimestampMS: number,
): IndexedDBDatabase {
  return {
    ...db,
    lastOpenedTimestampMS,
  };
}

test('putDatabase sequence', async () => {
  vi.setSystemTime(1);
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
  });

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  vi.setSystemTime(2);

  expect(await store.putDatabase(testDB2)).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
    testName2: withLastOpenedTimestampMS(testDB2, 2),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, 1),
    testName2: withLastOpenedTimestampMS(testDB2, 2),
  });
});

test('close closes kv store', async () => {
  const memstore = new TestMemStore();
  const store = new IDBDatabasesStore(_ => memstore);
  expect(memstore.closed).to.be.false;
  await store.close();
  expect(memstore.closed).to.be.true;
});

test('clear', async () => {
  vi.setSystemTime(1);
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, Date.now()),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, Date.now()),
  });

  await store.clearDatabases();

  expect(await store.getDatabases()).to.deep.equal({});

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  vi.setSystemTime(2);

  expect(await store.putDatabase(testDB2)).to.deep.equal({
    testName2: withLastOpenedTimestampMS(testDB2, Date.now()),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName2: withLastOpenedTimestampMS(testDB2, Date.now()),
  });
});

test('getProfileID', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const profileID = await store.getProfileID();
  expect(profileID).to.be.a('string');
  expect(profileID).to.match(/^p[a-zA-Z0-9]+$/);
  const profileID2 = await store.getProfileID();
  expect(profileID2).to.equal(profileID);
});
