import {runAll, TestStore} from './store-test-util.js';
import {IDBStore} from './idb-store.js';
import {dropStore} from './idb-util.js';
import {expect} from '@esm-bundle/chai';

async function newRandomIDBStore() {
  const name = `test-idbstore-${Math.random()}`;
  await dropStore(name);
  return new IDBStore(name);
}

runAll('idbstore', newRandomIDBStore);

test('dropStore', async () => {
  const name = `drop-store-${Math.random()}`;
  await dropStore(name);
  let idb = new IDBStore(name);
  let store = new TestStore(idb);

  // Write a value.
  await store.withWrite(async wt => {
    await wt.put('foo', 'bar');
    await wt.commit();
  });

  // Verify it's there.
  await store.withRead(async rt => {
    expect(await rt.get('foo')).to.deep.equal('bar');
  });

  // Drop db
  await store.close();
  await dropStore(name);

  // Reopen store, verify data is gone
  idb = new IDBStore(name);
  store = new TestStore(idb);
  await store.withRead(async rt => {
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
    const propAccessor = store as unknown as {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      _db: Promise<IDBDatabase>;
    };
    idb = propAccessor._db;
  });

  test('succeeds if IDB still exists', async () => {
    // Write a value.
    await store.withWrite(async wt => {
      await wt.put('foo', 'bar');
      await wt.commit();
    });

    // close the IDB from under the IDBStore
    (await idb).close();

    // write again, without error
    await store.withWrite(async wt => {
      await wt.put('baz', 'qux');
      await wt.commit();
    });

    await store.withRead(async rt => {
      expect(await rt.get('foo')).to.deep.equal('bar');
      expect(await rt.get('baz')).to.deep.equal('qux');
    });
  });

  test('throws if IDB was deleted', async () => {
    // Write a value.
    await store.withWrite(async wt => {
      await wt.put('foo', 'bar');
      await wt.commit();
    });

    await dropStore(name);

    let ex;
    try {
      await store.withWrite(async wt => {
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
      await store.withWrite(async wt => {
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
