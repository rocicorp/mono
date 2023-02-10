import {expect} from '@esm-bundle/chai';
import {TestMemStore} from '../kv/test-mem-store.js';
import {IDBDatabasesStore, IndexedDBDatabase} from './idb-databases-store.js';
import * as sinon from 'sinon';

teardown(() => {
  sinon.restore();
});

test('getDatabases with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  expect(await store.getDatabases()).to.deep.equal({});
});

test('putDatabase with no existing record in db', async () => {
  sinon.replace(Date, 'now', () => 1);

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
  let now = 1;
  sinon.replace(Date, 'now', () => now);

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

  now = 2;
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: withLastOpenedTimestampMS(testDB, 2),
  });
});

test('putDatabase ignores passed in lastOpenedTimestampMS', async () => {
  const now = 2;
  sinon.replace(Date, 'now', () => now);

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
  let now = 1;
  sinon.replace(Date, 'now', () => now);
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

  now = 2;

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
  let now = 1;
  sinon.replace(Date, 'now', () => now);
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, now),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: withLastOpenedTimestampMS(testDB1, now),
  });

  await store.clearDatabases();

  expect(await store.getDatabases()).to.deep.equal({});

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  now = 2;

  expect(await store.putDatabase(testDB2)).to.deep.equal({
    testName2: withLastOpenedTimestampMS(testDB2, now),
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName2: withLastOpenedTimestampMS(testDB2, now),
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
