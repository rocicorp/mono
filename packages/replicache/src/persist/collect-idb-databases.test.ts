import {LogContext} from '@rocicorp/logger';
import {type SinonFakeTimers, useFakeTimers} from 'sinon';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Store} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {fakeHash} from '../hash.ts';
import {IDBStore} from '../kv/idb-store.ts';
import {hasMemStore} from '../kv/mem-store.ts';
import {TestMemStore} from '../kv/test-mem-store.ts';
import type {OnClientsDeleted} from '../replicache-impl.ts';
import {getKVStoreProvider} from '../replicache.ts';
import type {ClientID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {makeClientGroupMap} from './client-groups.test.ts';
import {type ClientGroupMap, setClientGroups} from './client-groups.ts';
import {
  makeClientMapDD31,
  setClientsForTesting,
} from './clients-test-helpers.ts';
import type {ClientMap} from './clients.ts';
import {
  collectIDBDatabases,
  dropAllDatabases,
  dropDatabase,
} from './collect-idb-databases.ts';
import {
  IDBDatabasesStore,
  type IndexedDBDatabase,
  type IndexedDBName,
} from './idb-databases-store.ts';

describe('collectIDBDatabases', () => {
  let clock: SinonFakeTimers;

  beforeEach(() => {
    clock = useFakeTimers(0);
  });

  afterEach(() => {
    clock.restore();
  });

  type Entries = [IndexedDBDatabase, ClientMap, ClientGroupMap?][];

  const makeIndexedDBDatabase = ({
    name,
    lastOpenedTimestampMS = Date.now(),
    replicacheFormatVersion = FormatVersion.Latest,
    schemaVersion = 'schemaVersion-' + name,
    replicacheName = 'replicacheName-' + name,
  }: {
    name: string;
    lastOpenedTimestampMS?: number;
    replicacheFormatVersion?: number;
    schemaVersion?: string;
    replicacheName?: string;
  }): IndexedDBDatabase => ({
    name,
    replicacheFormatVersion,
    schemaVersion,
    replicacheName,
    lastOpenedTimestampMS,
  });

  const t = (
    name: string,
    entries: Entries,
    now: number,
    expectedDatabases: string[],
    expectedClientsDeleted?: ClientID[],
  ) => {
    test(name + ' > time ' + now, async () => {
      const store = new IDBDatabasesStore(_ => new TestMemStore());
      const dropStore = (name: string) => store.deleteDatabases([name]);
      const clientDagStores = new Map<IndexedDBName, Store>();
      for (const [db, clients, clientGroups] of entries) {
        const dagStore = new TestStore();
        clientDagStores.set(db.name, dagStore);

        await store.putDatabaseForTesting(db);

        await setClientsForTesting(clients, dagStore);
        if (clientGroups) {
          await withWrite(dagStore, async tx => {
            await setClientGroups(clientGroups, tx);
          });
        }
      }

      const newDagStore = (name: string) => {
        const dagStore = clientDagStores.get(name);
        assertNotUndefined(dagStore);
        return dagStore;
      };

      const maxAge = 1000;

      const onClientsDeleted = vi.fn<OnClientsDeleted>();

      await collectIDBDatabases(
        store,
        now,
        maxAge,
        dropStore,
        onClientsDeleted,
        newDagStore,
      );

      expect(Object.keys(await store.getDatabases())).to.deep.equal(
        expectedDatabases,
      );

      if (expectedClientsDeleted) {
        expect(onClientsDeleted).toHaveBeenCalledOnce();
        expect(onClientsDeleted).toHaveBeenLastCalledWith(
          expectedClientsDeleted,
        );
      }
    });
  };

  t('empty', [], 0, []);

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];

    t('one idb, one client', entries, 0, ['a']);
    t('one idb, one client', entries, 1000, []);
    t('one idb, one client', entries, 2000, []);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 1000}),
        makeClientMapDD31({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 1000,
          },
        }),
      ],
    ];
    t('x', entries, 0, ['a', 'b']);
    t('x', entries, 1000, ['b']);
    t('x', entries, 2000, []);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 2000}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
          clientA2: {
            headHash: fakeHash('a2'),
            heartbeatTimestampMs: 2000,
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 1000}),
        makeClientMapDD31({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 1000,
          },
        }),
      ],
    ];
    t('two idb, three clients', entries, 0, ['a', 'b']);
    t('two idb, three clients', entries, 1000, ['a', 'b']);
    t('two idb, three clients', entries, 2000, ['a']);
    t('two idb, three clients', entries, 3000, []);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 3000}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 1000,
          },
          clientA2: {
            headHash: fakeHash('a2'),
            heartbeatTimestampMs: 3000,
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 4000}),
        makeClientMapDD31({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 2000,
          },
          clientB2: {
            headHash: fakeHash('b2'),
            heartbeatTimestampMs: 4000,
          },
        }),
      ],
    ];
    t('two idb, four clients', entries, 1000, ['a', 'b']);
    t('two idb, four clients', entries, 2000, ['a', 'b']);
    t('two idb, four clients', entries, 3000, ['a', 'b']);
    t('two idb, four clients', entries, 4000, ['b']);
    t('two idb, four clients', entries, 5000, []);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({
          name: 'a',
          lastOpenedTimestampMS: 0,
          replicacheFormatVersion: FormatVersion.Latest + 1,
        }),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];
    t('one idb, one client, format version too new', entries, 0, ['a']);
    t('one idb, one client, format version too new', entries, 1000, ['a']);
    t('one idb, one client, format version too new', entries, 2000, ['a']);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({
          name: 'a',
          lastOpenedTimestampMS: 0,
          replicacheFormatVersion: FormatVersion.V6,
        }),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {
            headHash: fakeHash('a1'),
            mutationIDs: {
              clientA1: 2,
            },
            lastServerAckdMutationIDs: {
              clientA1: 1,
            },
          },
        }),
      ],
    ];
    t('one idb, one client, with pending mutations', entries, 0, ['a']);
    t('one idb, one client, with pending mutations', entries, 1000, ['a']);
    t('one idb, one client, with pending mutations', entries, 2000, ['a']);
    t('one idb, one client, with pending mutations', entries, 5000, ['a']);
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {
            headHash: fakeHash('a1'),
            mutationIDs: {
              clientA1: 2,
            },
            lastServerAckdMutationIDs: {
              clientA1: 2,
            },
          },
        }),
      ],
    ];

    t(
      'one idb with one client without any pending mutations should call onClientIDsDeleted',
      entries,
      5000,
      [],
      ['clientA1'],
    );
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
          clientA2: {
            headHash: fakeHash('a2'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {
            headHash: fakeHash('a1'),
            mutationIDs: {
              clientA1: 2,
              clientA2: 5,
            },
            lastServerAckdMutationIDs: {
              clientA1: 2,
              clientA2: 5,
            },
          },
        }),
      ],
    ];

    t(
      'one idb with two clients without any pending mutations should call onClientIDsDeleted',
      entries,
      5000,
      [],
      ['clientA1', 'clientA2'],
    );
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {
            headHash: fakeHash('a1'),
            mutationIDs: {
              clientA1: 2,
            },
            lastServerAckdMutationIDs: {
              clientA1: 2,
            },
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupB1',
          },
        }),
        makeClientGroupMap({
          clientGroupB1: {
            headHash: fakeHash('b1'),
            mutationIDs: {
              clientB1: 2,
            },
            lastServerAckdMutationIDs: {
              clientB1: 2,
            },
          },
        }),
      ],
    ];

    t(
      'two idb with one client in each without any pending mutations should call onClientIDsDeleted',
      entries,
      5000,
      [],
      ['clientA1', 'clientB1'],
    );
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {
            headHash: fakeHash('a1'),
            mutationIDs: {
              clientA1: 2,
            },
            lastServerAckdMutationIDs: {
              clientA1: 1,
            },
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 0}),
        makeClientMapDD31({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupB1',
          },
        }),
        makeClientGroupMap({
          clientGroupB1: {
            headHash: fakeHash('b1'),
            mutationIDs: {
              clientB1: 2,
            },
            lastServerAckdMutationIDs: {
              clientB1: 2,
            },
          },
        }),
      ],
    ];

    t(
      'two idb with one client in each, one client has pending mutations should call onClientIDsDeleted',
      entries,
      5000,
      ['a'],
      ['clientB1'],
    );
  }
});

test('dropDatabases mem', async () => {
  const createStore = getKVStoreProvider(new LogContext(), 'mem').create;
  const store = new IDBDatabasesStore(createStore);
  const numDbs = 10;

  for (let i = 0; i < numDbs; i++) {
    const db = {
      name: `db${i}`,
      replicacheName: `testReplicache${i}`,
      replicacheFormatVersion: 1,
      schemaVersion: 'testSchemaVersion1',
    };

    expect(await store.putDatabase(db)).to.have.property(db.name);
    const kvStore = createStore(db.name);
    await withWrite(kvStore, async write => {
      await write.put('foo', {
        baz: 'bar',
      });
    });
  }

  for (let i = 0; i < numDbs; i++) {
    const dbName = `db${i}`;
    const store = hasMemStore(dbName);
    expect(store).to.be.true;
  }

  expect(Object.values(await store.getDatabases())).to.have.length(numDbs);

  const result = await dropAllDatabases({
    kvStore: 'mem',
  });

  for (let i = 0; i < numDbs; i++) {
    const dbName = `db${i}`;
    const store = hasMemStore(dbName);
    expect(store).to.be.false;
  }

  expect(Object.values(await store.getDatabases())).to.have.length(0);
  expect(result.dropped).to.have.length(numDbs);
  expect(result.errors).to.have.length(0);
});

test('dropDatabases idb', async () => {
  const createStore = getKVStoreProvider(new LogContext(), 'idb').create;
  const store = new IDBDatabasesStore(createStore);
  const numDbs = 10;

  for (let i = 0; i < numDbs; i++) {
    const db = {
      name: `db${i}`,
      replicacheName: `testReplicache${i}`,
      replicacheFormatVersion: 1,
      schemaVersion: 'testSchemaVersion1',
    };

    expect(await store.putDatabase(db)).to.have.property(db.name);
    const kvStore = createStore(db.name);
    await withWrite(kvStore, async write => {
      await write.put('foo', {
        baz: 'bar',
      });
    });
  }

  for (let i = 0; i < numDbs; i++) {
    const dbName = `db${i}`;
    const request = indexedDB.open(dbName);
    request.onsuccess = event => {
      const db = (event.target as IDBRequest<IDBDatabase>).result;
      const transaction = db.transaction(['chunks'], 'readonly');
      const objectStore = transaction.objectStore('chunks');
      const getRequest = objectStore.get('foo');
      getRequest.onsuccess = _event => {
        expect(getRequest.result).to.deep.equal({baz: 'bar'});
        db.close();
      };
    };
  }
  //idb interfaces and loop and make sure that it actually wrote
  expect(Object.values(await store.getDatabases())).to.have.length(numDbs);

  const result = await dropAllDatabases({kvStore: 'idb'});

  const dbPromise = [];
  for (let i = 0; i < numDbs; i++) {
    const dbName = `db${i}`;
    const promise = new Promise((resolve, _reject) => {
      const request = indexedDB.deleteDatabase(dbName);
      request.onsuccess = event => {
        const db = (event.target as IDBRequest<IDBDatabase>).result;
        resolve(db);
      };
    });
    dbPromise.push(promise);
  }

  const foundDbs = await Promise.all(dbPromise);
  const foundDbCount = foundDbs.filter(db => db !== undefined).length;
  expect(foundDbCount).to.equal(0);

  expect(Object.values(await store.getDatabases())).to.have.length(0);
  expect(result.dropped).to.have.length(numDbs);
  expect(result.errors).to.have.length(0);
});

test('dropDatabase', async () => {
  const createKVStore = (name: string) => new IDBStore(name);
  const store = new IDBDatabasesStore(createKVStore);

  const db = {
    name: `foo`,
    replicacheName: `fooRep`,
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(db)).to.have.property(db.name);

  expect(Object.values(await store.getDatabases())).to.have.length(1);
  await dropDatabase(db.name);

  expect(Object.values(await store.getDatabases())).to.have.length(0);

  // deleting non-existent db fails silently.
  await dropDatabase('bonk');
});
