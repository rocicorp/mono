import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeEach, expect, test} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {SQLiteStore} from './sqlite-store.ts';
import {runAll} from './store-test-util.ts';

const sqlite3DatabaseManager = getTestSQLiteDatabaseManager();

runAll(
  'SQLiteStore',
  () => new SQLiteStore(':memory:', sqlite3DatabaseManager, {
    journalMode: 'WAL',
  }),
);

beforeEach(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

afterAll(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

test('creating multiple with same name shares data after close', async () => {
  const store = new SQLiteStore('test', sqlite3DatabaseManager);
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  await store.close();

  const store2 = new SQLiteStore('test', sqlite3DatabaseManager);
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });

  await store2.close();
});

test('creating multiple with different name gets unique data', async () => {
  const store = new SQLiteStore('test', sqlite3DatabaseManager);
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const store2 = new SQLiteStore('test2', sqlite3DatabaseManager);
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal(undefined);
  });
});

test('multiple reads at the same time', async () => {
  const store = new SQLiteStore('test', sqlite3DatabaseManager);
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise, resolve} = resolver();

  let readCounter = 0;
  const p1 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    await promise;
    expect(readCounter).equal(1);
    readCounter++;
  });
  const p2 = withRead(store, async rt => {
    expect(readCounter).equal(0);
    readCounter++;
    expect(await rt.get('foo')).equal('bar');
    resolve();
  });
  expect(readCounter).equal(0);
  await Promise.all([p1, p2]);
  expect(readCounter).equal(2);
});

test('single write at a time', async () => {
  const store = new SQLiteStore('test', sqlite3DatabaseManager);
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise: promise1, resolve: resolve1} = resolver();
  const {promise: promise2, resolve: resolve2} = resolver();

  let writeCounter = 0;
  const p1 = withWrite(store, async wt => {
    await promise1;
    expect(await wt.get('foo')).equal('bar');
    expect(writeCounter).equal(0);
    writeCounter++;
  });
  const p2 = withWrite(store, async wt => {
    await promise2;
    expect(writeCounter).equal(1);
    expect(await wt.get('foo')).equal('bar');
    writeCounter++;
  });

  // Doesn't matter that resolve2 is called first, because p2 is waiting on p1.
  resolve2();
  await sleep(10);
  resolve1();

  await Promise.all([p1, p2]);
  expect(writeCounter).equal(2);
});

test('single write across multiple SQLiteStores at a time', async () => {
  // Two distinct store instances pointing at the same underlying database
  const store1 = new SQLiteStore('test-concurrent', sqlite3DatabaseManager);
  const store2 = new SQLiteStore('test-concurrent', sqlite3DatabaseManager);

  // Seed the database so we have something to read back during the writes
  await withWrite(store1, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise: promise1, resolve: resolve1} = resolver();
  const {promise: promise2, resolve: resolve2} = resolver();

  let writeCounter = 0;

  const p1 = withWrite(store1, async wt => {
    await promise1;
    expect(await wt.get('foo')).equal('bar');
    expect(writeCounter).equal(0);
    writeCounter++;
    await wt.put('bar', 'baz');
  });

  const p2 = withWrite(store2, async wt => {
    await promise2;
    // p2 waited for p1 to finish its write.
    expect(writeCounter).equal(1);
    expect(await wt.get('foo')).equal('bar');
    expect(await wt.get('bar')).equal('baz');
    writeCounter++;
  });

  // Intentionally resolve the second write first.
  resolve2();
  await sleep(10);
  resolve1();

  await Promise.all([p1, p2]);
  expect(writeCounter).equal(2);

  await Promise.all([store1.close(), store2.close()]);
});

test('closed reflects status after close', async () => {
  const store = new SQLiteStore('closed-flag', sqlite3DatabaseManager);
  expect(store.closed).to.be.false;
  await store.close();
  expect(store.closed).to.be.true;
});

test('closing a store multiple times', async () => {
  const store = new SQLiteStore('double-close', sqlite3DatabaseManager);
  await store.close();
  // Second close should be a no-op and must not throw.
  await store.close();
  expect(store.closed).to.be.true;
});

test('data persists after store is closed and reopened', async () => {
  const name = 'persist-after-close';
  const store1 = new SQLiteStore(name, sqlite3DatabaseManager);
  await withWrite(store1, async wt => {
    await wt.put('foo', 'bar');
  });
  await store1.close();

  const store2 = new SQLiteStore(name, sqlite3DatabaseManager);
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });
  await store2.close();
});
