import {runAll, TestStore} from './store-test-util';
import {dropStore, IDBStore} from './idb-store';
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

suite('reopening IDB', async () => {
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

    try {
      await store.withWrite(async wt => {
        await wt.put('baz', 'qux');
      });
    } catch (e) {
      expect(e as Error).to.match(/Replicache IndexedDB not found/);
    }

    // ensure that the db created in the reopening process was cleaned up
    const req = indexedDB.open(name);

    let dbDeleted = false;

    const promise = new Promise((resolve, reject) => {
      req.onupgradeneeded = () => (dbDeleted = true);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

    await promise;
    expect(dbDeleted).to.be.true;
  });
});
