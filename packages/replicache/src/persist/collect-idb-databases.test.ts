import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Store} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import {getDeletedClients, setDeletedClients} from '../deleted-clients.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {fakeHash} from '../hash.ts';
import {IDBStore} from '../kv/idb-store.ts';
import {hasMemStore} from '../kv/mem-store.ts';
import {TestMemStore} from '../kv/test-mem-store.ts';
import {getKVStoreProvider} from '../replicache.ts';
import type {ClientGroupID, ClientID} from '../sync/ids.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {type ClientGroupMap, setClientGroups} from './client-groups.ts';
import {makeClientMap, setClientsForTesting} from './clients-test-helpers.ts';
import type {ClientMap, OnClientsDeleted} from './clients.ts';
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
import {makeClientGroupMap} from './test-utils.ts';

describe('collectIDBDatabases', {timeout: 20_000}, () => {
  beforeEach(() => {
    vi.useFakeTimers({now: 0});
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  type Entries = [
    IndexedDBDatabase,
    ClientMap,
    (ClientGroupMap | undefined)?,
    (ClientID[] | undefined)?,
    (ClientGroupID[] | undefined)?,
  ][];

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

  const t = ({
    name,
    entries,
    now,
    expectedDatabases,
    expectedClientsDeleted = [],
    expectedClientGroupsDeleted = [],
    enableMutationRecovery = true,
  }: {
    name: string;
    entries: Entries;
    now: number;
    expectedDatabases: string[];
    expectedClientsDeleted?: ClientID[] | undefined;
    expectedClientGroupsDeleted?: ClientGroupID[] | undefined;
    enableMutationRecovery?: boolean | undefined;
  }) => {
    test(name + ' > time ' + now, async () => {
      const store = new IDBDatabasesStore(_ => new TestMemStore());
      const dropStore = (name: string) => store.deleteDatabases([name]);
      const clientDagStores = new Map<IndexedDBName, Store>();
      for (const [
        db,
        clients,
        clientGroups,
        deletedClientIDs,
        deletedClientGroupIDs,
      ] of entries) {
        const dagStore = new TestStore();
        clientDagStores.set(db.name, dagStore);

        await store.putDatabaseForTesting(db);

        await setClientsForTesting(clients, dagStore);
        if (clientGroups) {
          await withWrite(dagStore, dagWrite =>
            setClientGroups(clientGroups, dagWrite),
          );
        }
        if (deletedClientIDs || deletedClientGroupIDs) {
          await withWrite(dagStore, dagWrite =>
            setDeletedClients(
              dagWrite,
              deletedClientIDs ?? [],
              deletedClientGroupIDs ?? [],
            ),
          );
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
        enableMutationRecovery,
        onClientsDeleted,
        newDagStore,
      );

      expect(Object.keys(await store.getDatabases())).to.deep.equal(
        expectedDatabases,
      );

      if (
        expectedClientsDeleted.length > 0 ||
        expectedClientGroupsDeleted.length > 0
      ) {
        expect(onClientsDeleted).toHaveBeenCalledOnce();
        expect(onClientsDeleted).toHaveBeenLastCalledWith(
          expectedClientsDeleted,
          expectedClientGroupsDeleted,
        );
      } else {
        expect(onClientsDeleted).not.toHaveBeenCalledOnce();
      }

      // Make sure that all remaining databases have correct deleted clients head.
      if (expectedDatabases.length > 0) {
        for (const name of expectedDatabases) {
          const dagStore = newDagStore(name);
          expect(
            await withRead(dagStore, read => getDeletedClients(read)),
          ).toEqual({
            clientIDs: expectedClientsDeleted,
            clientGroupIDs: expectedClientGroupsDeleted,
          });
        }
      }
    });
  };

  t({name: 'empty', entries: [], now: 0, expectedDatabases: []});

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];

    t({name: 'one idb, one client', entries, now: 0, expectedDatabases: ['a']});
    t({
      name: 'one idb, one client',
      entries,
      now: 1000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1'],
      expectedClientGroupsDeleted: ['make-client-group-id'],
    });
    t({
      name: 'one idb, one client',
      entries,
      now: 2000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1'],
      expectedClientGroupsDeleted: ['make-client-group-id'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 1000}),
        makeClientMap({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 1000,
          },
        }),
      ],
    ];
    t({
      name: 'two idb, one client in each',
      entries,
      now: 0,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, one client in each',
      entries,
      now: 1000,
      expectedDatabases: ['b'],
      expectedClientsDeleted: ['clientA1'],
      expectedClientGroupsDeleted: ['make-client-group-id'],
    });
    t({
      name: 'two idb, one client in each',
      entries,
      now: 2000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientB1'],
      expectedClientGroupsDeleted: ['make-client-group-id'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 2000}),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
            clientGroupID: 'clientGroupA1',
          },
          clientA2: {
            headHash: fakeHash('a2'),
            heartbeatTimestampMs: 2000,
            clientGroupID: 'clientGroupA2',
          },
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 1000}),
        makeClientMap({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 1000,
            clientGroupID: 'clientGroupB1',
          },
        }),
      ],
    ];
    t({
      name: 'two idb, three clients',
      entries,
      now: 0,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, three clients',
      entries,
      now: 1000,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, three clients',
      entries,
      now: 2000,
      expectedDatabases: ['a'],
      expectedClientsDeleted: ['clientB1'],
      expectedClientGroupsDeleted: ['clientGroupB1'],
    });
    t({
      name: 'two idb, three clients',
      entries,
      now: 3000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientA2', 'clientB1'],
      expectedClientGroupsDeleted: [
        'clientGroupA1',
        'clientGroupA2',
        'clientGroupB1',
      ],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 3000}),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 1000,
            clientGroupID: 'clientGroupA1',
          },
          clientA2: {
            headHash: fakeHash('a2'),
            heartbeatTimestampMs: 3000,
            clientGroupID: 'clientGroupA1',
          },
        }),
        makeClientGroupMap({
          clientGroupA1: {headHash: fakeHash('a1')},
        }),
      ],
      [
        makeIndexedDBDatabase({name: 'b', lastOpenedTimestampMS: 4000}),
        makeClientMap({
          clientB1: {
            headHash: fakeHash('b1'),
            heartbeatTimestampMs: 2000,
            clientGroupID: 'clientGroupB1',
          },
          clientB2: {
            headHash: fakeHash('b2'),
            heartbeatTimestampMs: 4000,
            clientGroupID: 'clientGroupB1',
          },
        }),
      ],
    ];
    t({
      name: 'two idb, four clients',
      entries,
      now: 1000,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, four clients',
      entries,
      now: 2000,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, four clients',
      entries,
      now: 3000,
      expectedDatabases: ['a', 'b'],
    });
    t({
      name: 'two idb, four clients',
      entries,
      now: 4000,
      expectedDatabases: ['b'],
      expectedClientsDeleted: ['clientA1', 'clientA2'],
      expectedClientGroupsDeleted: ['clientGroupA1'],
    });
    t({
      name: 'two idb, four clients',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientA2', 'clientB1', 'clientB2'],
      expectedClientGroupsDeleted: ['clientGroupA1', 'clientGroupB1'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({
          name: 'a',
          lastOpenedTimestampMS: 0,
          replicacheFormatVersion: FormatVersion.Latest + 1,
        }),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];
    t({
      name: 'one idb, one client, format version too new',
      entries,
      now: 0,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, format version too new',
      entries,
      now: 1000,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, format version too new',
      entries,
      now: 2000,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, format version too new, enableMutationRecovery is false',
      entries,
      now: 2000,
      expectedDatabases: ['a'],
      enableMutationRecovery: false,
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({
          name: 'a',
          lastOpenedTimestampMS: 0,
          replicacheFormatVersion: FormatVersion.V6,
        }),
        makeClientMap({
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
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 0,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 1000,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 2000,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 5000,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, with pending mutations, enableMutationRecovery is false',
      entries,
      now: 5000,
      enableMutationRecovery: false,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1'],
      expectedClientGroupsDeleted: ['clientGroupA1'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
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

    t({
      name: 'one idb with one client without any pending mutations should call onClientIDsDeleted',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1'],
      expectedClientGroupsDeleted: ['clientGroupA1'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
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

    t({
      name: 'one idb with two clients without any pending mutations should call onClientIDsDeleted',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientA2'],
      expectedClientGroupsDeleted: ['clientGroupA1'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
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
        makeClientMap({
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

    t({
      name: 'two idb with one client in each without any pending mutations should call onClientIDsDeleted',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientB1'],
      expectedClientGroupsDeleted: ['clientGroupA1', 'clientGroupB1'],
    });
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({name: 'a', lastOpenedTimestampMS: 0}),
        makeClientMap({
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
        makeClientMap({
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

    t({
      name: 'two idb with one client in each, one client has pending mutations should call onClientIDsDeleted',
      entries,
      now: 5000,
      expectedDatabases: ['a'],
      expectedClientsDeleted: ['clientB1'],
      expectedClientGroupsDeleted: ['clientGroupB1'],
    });

    t({
      name: 'two idb with one client in each, one client has pending mutations but enableMutationRecovery is false should call onClientIDsDeleted',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedClientsDeleted: ['clientA1', 'clientB1'],
      expectedClientGroupsDeleted: ['clientGroupA1', 'clientGroupB1'],
      enableMutationRecovery: false,
    });

    {
      const entries2: Entries = [
        [entries[0][0], entries[0][1], entries[0][2], ['old-deleted-client-3']],
        [
          entries[1][0],
          entries[1][1],
          entries[1][2],
          ['old-deleted-client-1', 'old-deleted-client-2'],
        ],
      ];
      t({
        name: 'two idb with one client in each, one client has pending mutations should call onClientIDsDeleted. Also has old deleted clients',
        entries: entries2,
        now: 5000,
        expectedDatabases: [],
        expectedClientsDeleted: [
          'clientA1',
          'clientB1',
          'old-deleted-client-1',
          'old-deleted-client-2',
          'old-deleted-client-3',
        ],
        expectedClientGroupsDeleted: ['clientGroupA1', 'clientGroupB1'],
        enableMutationRecovery: false,
      });
    }
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

  const initialDatabasesLength = Object.values(
    await store.getDatabases(),
  ).length;

  const db = {
    name: `foo`,
    replicacheName: `fooRep`,
    replicacheFormatVersion: 1,
    schemaVersion: 'testSchemaVersion1',
  };

  expect(await store.putDatabase(db)).to.have.property(db.name);

  expect(Object.values(await store.getDatabases())).to.have.length(
    initialDatabasesLength + 1,
  );
  await dropDatabase(db.name);

  expect(Object.values(await store.getDatabases())).to.have.length(
    initialDatabasesLength,
  );

  // deleting non-existent db fails silently.
  await dropDatabase('bonk');
});
