import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeEach, expect, test} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {createSQLiteStore} from './sqlite-store.ts';
import {runAll} from './store-test-util.ts';

const sqlite3DatabaseManager = getTestSQLiteDatabaseManager();
const createStore = createSQLiteStore(sqlite3DatabaseManager);

const getNewStore = (name: string) =>
  createStore(name, {
    readPoolSize: 2,
    journalMode: 'WAL',
    synchronous: 'NORMAL',
    readUncommitted: false,
    busyTimeout: 200,
  });

runAll('SQLiteStore', () => getNewStore('test'));

beforeEach(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

afterAll(() => {
  sqlite3DatabaseManager.clearAllStoresForTesting();
});

test('creating multiple with same name shares data after close', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  await store.close();

  const store2 = getNewStore('test');
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });

  await store2.close();
});

test('creating multiple with different name gets unique data', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const store2 = getNewStore('test2');
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal(undefined);
  });
});

test('multiple reads at the same time', async () => {
  const store = getNewStore('test');
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

test('multiple reads at the same time with first committing early', async () => {
  const store = getNewStore('test');
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  const {promise: promise1, resolve: resolve1} = resolver();
  const {promise: promise2, resolve: resolve2} = resolver();

  // this runs COMMIT
  const p1 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    await promise1;
    resolve2();
  });
  // runs after the previous tx
  const p2 = withRead(store, async rt => {
    expect(await rt.get('foo')).equal('bar');
    resolve1();
  });
  const p3 = withRead(store, async rt => {
    await promise2;
    expect(await rt.get('foo')).equal('bar');
  });

  await Promise.all([p1, p2, p3]);
});

test('read before write', async () => {
  const store = getNewStore('test');

  const {promise, resolve} = resolver();

  const readP = withRead(store, async rt => {
    await promise;
    expect(await rt.get('foo')).equal(undefined);
  });

  const writeP = withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  resolve();

  await Promise.all([readP, writeP]);
});

test('single write at a time', async () => {
  const store = getNewStore('test');
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

test('closed reflects status after close', async () => {
  const store = getNewStore('flag');
  expect(store.closed).to.be.false;
  await store.close();
  expect(store.closed).to.be.true;
});

test('closing a store multiple times', async () => {
  const store = getNewStore('double');
  await store.close();
  // Second close should be a no-op and must not throw.
  await store.close();
  expect(store.closed).to.be.true;
});

test('data persists after store is closed and reopened', async () => {
  const name = 'persist';
  const store1 = getNewStore(name);
  await withWrite(store1, async wt => {
    await wt.put('foo', 'bar');
  });
  await store1.close();

  const store2 = getNewStore(name);
  await withRead(store2, async rt => {
    expect(await rt.get('foo')).equal('bar');
  });
  await store2.close();
});
