import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {dropIDBStoreWithMemFallback} from './idb-store-with-mem-fallback.ts';
import {IDBNotFoundError, IDBStore} from './idb-store.ts';
import {runAll} from './store-test-util.ts';

async function newRandomIDBStore() {
  const name = `test-idbstore-${Math.random()}`;
  await dropIDBStoreWithMemFallback(name);
  return new IDBStore(name);
}

runAll('idbstore', newRandomIDBStore);

test('dropStore', async () => {
  const name = `drop-store-${Math.random()}`;
  await dropIDBStoreWithMemFallback(name);
  let store = new IDBStore(name);

  // Write a value.
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
  });

  // Verify it's there.
  await withRead(store, async rt => {
    expect(await rt.get('foo')).to.deep.equal('bar');
  });

  // Drop db
  await store.close();
  await dropIDBStoreWithMemFallback(name);

  // Reopen store, verify data is gone
  store = new IDBStore(name);
  await withRead(store, async rt => {
    expect(await rt.has('foo')).to.be.false;
  });
});

describe('reopening IDB', () => {
  let name: string;
  let idb: Promise<IDBDatabase>;
  let store: IDBStore;

  beforeEach(async () => {
    name = `reopen-${Math.random()}`;
    await dropIDBStoreWithMemFallback(name);

    // Use spy to get a hold of the request object and then the idb instance.
    const openSpy = vi.spyOn(indexedDB, 'open');

    store = new IDBStore(name);
    const result = openSpy.mock.results[0];
    assert(result.type === 'return');
    const req = result.value;
    idb = new Promise((resolve, reject) => {
      req.addEventListener('success', () => resolve(req.result));
      req.addEventListener('error', () => reject(req.error));
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('succeeds if IDB still exists', async () => {
    // Write a value.
    await withWrite(store, async wt => {
      await wt.put('foo', 'bar');
    });

    // close the IDB from under the IDBStore
    (await idb).close();

    // write again, without error
    await withWrite(store, async wt => {
      await wt.put('baz', 'qux');
    });

    await withRead(store, async rt => {
      expect(await rt.get('foo')).to.deep.equal('bar');
      expect(await rt.get('baz')).to.deep.equal('qux');
    });
  });

  test('throws if IDB was deleted', async () => {
    // Write a value.
    await withWrite(store, async wt => {
      await wt.put('foo', 'bar');
    });

    await dropIDBStoreWithMemFallback(name);

    let ex;
    try {
      await withWrite(store, async wt => {
        await wt.put('baz', 'qux');
      });
    } catch (e) {
      ex = e;
    }
    expect(ex as Error).to.match(/Expected IndexedDB not found/);

    // ensure that any db creation during the reopening process was aborted
    const req = indexedDB.open(name);
    let aborted = false;

    const promise = new Promise((resolve, reject) => {
      req.onupgradeneeded = evt => (aborted = evt.oldVersion === 0);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

    await promise;
    expect(aborted).to.be.true;
  });

  test('deletes corrupt IDB and throws error', async () => {
    await dropIDBStoreWithMemFallback(name);

    // create a corrupt IDB (ver. 1, no object stores)
    const createReq = new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open(name);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

    (await createReq).close();

    // a new IDBStore encountering the corrupt db
    store = new IDBStore(name);

    let ex;
    try {
      await withWrite(store, async wt => {
        await wt.put('baz', 'qux');
      });
    } catch (e) {
      ex = e;
    }
    expect((ex as Error).message).to.match(
      /Expected IndexedDB .* missing object store/,
    );

    // ensure that the corrupt db was deleted
    const req = indexedDB.open(name);
    let newlyCreated = false;

    const promise = new Promise((resolve, reject) => {
      req.onupgradeneeded = evt => (newlyCreated = evt.oldVersion === 0);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

    await promise;
    expect(newlyCreated).to.be.true;
  });
});

test('Throws if IDB dropped while open', async () => {
  const name = `drop-store-${Math.random()}`;

  const idb = new IDBStore(name);

  await dropIDBStoreWithMemFallback(name);

  let err;
  try {
    await withRead(idb, async tx => {
      await tx.has('foo');
    });
  } catch (e) {
    err = e;
  }
  expect(err).instanceOf(IDBNotFoundError);
  expect((err as Error).message).to.match(/Expected IndexedDB/);
});
