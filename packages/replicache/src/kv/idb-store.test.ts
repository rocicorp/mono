import {expect} from 'chai';
import {withRead, withWrite} from '../with-transactions.js';
import {IDBNotFoundError, IDBStore} from './idb-store.js';
import {dropStore} from './idb-util.js';
import {runAll} from './store-test-util.js';

async function newRandomIDBStore() {
  const name = `test-idbstore-${Math.random()}`;
  await dropStore(name);
  return new IDBStore(name);
}

runAll('idbstore', newRandomIDBStore);

test('dropStore', async () => {
  const name = `drop-store-${Math.random()}`;
  await dropStore(name);
  let store = new IDBStore(name);

  // Write a value.
  await withWrite(store, async wt => {
    await wt.put('foo', 'bar');
    await wt.commit();
  });

  // Verify it's there.
  await withRead(store, async rt => {
    expect(await rt.get('foo')).to.deep.equal('bar');
  });

  // Drop db
  await store.close();
  await dropStore(name);

  // Reopen store, verify data is gone
  store = new IDBStore(name);
  await withRead(store, async rt => {
    expect(await rt.has('foo')).to.be.false;
  });
});

suite('reopening IDB', () => {
  let name: string;
  let idb: Promise<IDBDatabase>;
  let store: IDBStore;

  setup(async () => {
    name = `reopen-${Math.random()}`;
    await dropStore(name);

    store = new IDBStore(name);
    // @ts-expect-error _db is private
    idb = store._db;
  });

  test('succeeds if IDB still exists', async () => {
    // Write a value.
    await withWrite(store, async wt => {
      await wt.put('foo', 'bar');
      await wt.commit();
    });

    // close the IDB from under the IDBStore
    (await idb).close();

    // write again, without error
    await withWrite(store, async wt => {
      await wt.put('baz', 'qux');
      await wt.commit();
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
      await wt.commit();
    });

    await dropStore(name);

    let ex;
    try {
      await withWrite(store, async wt => {
        await wt.put('baz', 'qux');
      });
    } catch (e) {
      ex = e;
    }
    expect(ex as Error).to.match(/Replicache IndexedDB not found/);

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
    await dropStore(name);

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
      /Replicache IndexedDB .* missing object store/,
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

  await dropStore(name);

  let err;
  try {
    await withRead(idb, async tx => {
      await tx.has('foo');
    });
  } catch (e) {
    err = e;
  }
  expect(err).instanceOf(IDBNotFoundError);
  expect((err as Error).message).to.match(/Replicache IndexedDB/);
});
