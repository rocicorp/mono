import {expect} from '@esm-bundle/chai';
import {TestMemStore} from '../kv/test-mem-store';
import {IDBDatabasesStore} from './idb-databases-store';

test('getDatabases with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  expect(await store.getDatabases()).to.deep.equal({});
});

test('putDatabase with no existing record in db', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB = {
    name: 'testName',
    replicacheName: 'testReplicacheName',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion',
  };
  expect(await store.putDatabase(testDB)).to.deep.equal({
    testName: testDB,
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName: testDB,
  });
});

test('putDatabase sequence', async () => {
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).to.deep.equal({
    testName1: testDB1,
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: testDB1,
  });

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  expect(await store.putDatabase(testDB2)).to.deep.equal({
    testName1: testDB1,
    testName2: testDB2,
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: testDB1,
    testName2: testDB2,
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
  const store = new IDBDatabasesStore(_ => new TestMemStore());
  const testDB1 = {
    name: 'testName1',
    replicacheName: 'testReplicacheName1',
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(testDB1)).to.deep.equal({
    testName1: testDB1,
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName1: testDB1,
  });

  await store.clearDatabases();

  expect(await store.getDatabases()).to.deep.equal({});

  const testDB2 = {
    name: 'testName2',
    replicacheName: 'testReplicacheName2',
    replicacheFormatVersion: 2,
    schemaVersion: 'testSchemaVersion2',
  };

  expect(await store.putDatabase(testDB2)).to.deep.equal({
    testName2: testDB2,
  });
  expect(await store.getDatabases()).to.deep.equal({
    testName2: testDB2,
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
