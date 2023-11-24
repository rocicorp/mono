import {expect} from 'chai';
import {assertNotUndefined} from 'shared/src/asserts.js';
import * as sinon from 'sinon';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import type {Store} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {FormatVersion} from '../format-version.js';
import {fakeHash} from '../hash.js';
import {IDBStore} from '../kv/idb-store.js';
import {TestMemStore} from '../kv/test-mem-store.js';
import {withWrite} from '../with-transactions.js';
import {ClientGroupMap, setClientGroups} from './client-groups.js';
import {makeClientGroupMap} from './client-groups.test.js';
import {
  makeClientMapDD31,
  setClientsForTesting,
} from './clients-test-helpers.js';
import type {ClientMap} from './clients.js';
import {
  collectIDBDatabases,
  deleteAllReplicacheData,
  dropAllDatabases,
  dropDatabase,
} from './collect-idb-databases.js';
import {
  IDBDatabasesStore,
  IndexedDBDatabase,
  IndexedDBName,
} from './idb-databases-store.js';

suite('collectIDBDatabases', () => {
  let clock: SinonFakeTimers;

  setup(() => {
    clock = useFakeTimers(0);
  });

  teardown(() => {
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

  const NO_LEGACY = [false];
  const INCLUDE_LEGACY = [false, true];

  const t = ({
    name,
    entries,
    now,
    expectedDatabases,
    legacyValues = INCLUDE_LEGACY,
    expectedOnClientRemoved = [],
  }: {
    name: string;
    entries: Entries;
    now: number;
    expectedDatabases: string[];
    legacyValues?: boolean[];
    expectedOnClientRemoved?: string[];
  }) => {
    for (const legacy of legacyValues) {
      test(name + ' > time ' + now + (legacy ? ' > legacy' : ''), async () => {
        const store = new IDBDatabasesStore(_ => new TestMemStore());
        const clientDagStores = new Map<IndexedDBName, Store>();
        for (const [db, clients, clientGroups] of entries) {
          const dagStore = new TestStore();
          clientDagStores.set(db.name, dagStore);
          if (legacy) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const {lastOpenedTimestampMS: _, ...rest} = db;
            await store.putDatabaseForTesting(rest);
          } else {
            await store.putDatabaseForTesting(db);
          }

          await setClientsForTesting(clients, dagStore);
          if (clientGroups) {
            await withWrite(dagStore, tx => setClientGroups(clientGroups, tx));
          }
        }

        const newDagStore = (name: string) => {
          const dagStore = clientDagStores.get(name);
          assertNotUndefined(dagStore);
          return dagStore;
        };

        const maxAge = 1000;

        const onClientsRemoved = sinon.fake();

        await collectIDBDatabases(
          store,
          onClientsRemoved,
          now,
          maxAge,
          maxAge,
          newDagStore,
        );

        if (legacy) {
          expect(onClientsRemoved.callCount).equal(0);
        } else {
          if (expectedOnClientRemoved.length === 0) {
            expect(onClientsRemoved.callCount).equal(0);
          } else {
            expect(onClientsRemoved.callCount).equal(1);
            expect(onClientsRemoved.lastCall.args[0]).to.have.keys(
              ...expectedOnClientRemoved,
            );
          }
        }

        expect(Object.keys(await store.getDatabases())).to.deep.equal(
          expectedDatabases,
        );
      });
    }
  };

  t({name: 'empty', entries: [], now: 0, expectedDatabases: []});

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

    t({name: 'one idb, one client', entries, now: 0, expectedDatabases: ['a']});
    t({
      name: 'one idb, one client',
      entries,
      now: 1000,
      expectedDatabases: [],
      expectedOnClientRemoved: ['clientA1'],
    });
    t({
      name: 'one idb, one client',
      entries,
      now: 2000,
      expectedDatabases: [],
      expectedOnClientRemoved: ['clientA1'],
    });
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
    t({name: 'x', entries, now: 0, expectedDatabases: ['a', 'b']});
    t({
      name: 'x',
      entries,
      now: 1000,
      expectedDatabases: ['b'],
      expectedOnClientRemoved: ['clientA1'],
    });
    t({
      name: 'x',
      entries,
      now: 2000,
      expectedDatabases: [],
      expectedOnClientRemoved: ['clientA1', 'clientB1'],
    });
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
      expectedOnClientRemoved: ['clientB1'],
    });
    t({
      name: 'two idb, three clients',
      entries,
      now: 3000,
      expectedDatabases: [],
      expectedOnClientRemoved: ['clientA1', 'clientA2', 'clientB1'],
    });
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
      expectedOnClientRemoved: ['clientA1', 'clientA2'],
    });
    t({
      name: 'two idb, four clients',
      entries,
      now: 5000,
      expectedDatabases: [],
      expectedOnClientRemoved: ['clientA1', 'clientA2', 'clientB1', 'clientB2'],
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
        makeClientMapDD31({
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
  }

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase({
          name: 'a',
          lastOpenedTimestampMS: 0,
          replicacheFormatVersion: FormatVersion.SDD - 1,
        }),
        makeClientMapDD31({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];
    t({
      name: 'one idb, one client, old format version',
      entries,
      now: 0,
      expectedDatabases: ['a'],
    });
    t({
      name: 'one idb, one client, old format version',
      entries,
      now: 1000,
      expectedDatabases: [],
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
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 0,
      expectedDatabases: ['a'],
      legacyValues: NO_LEGACY,
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 1000,
      expectedDatabases: ['a'],
      legacyValues: NO_LEGACY,
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 2000,
      expectedDatabases: ['a'],
      legacyValues: NO_LEGACY,
    });
    t({
      name: 'one idb, one client, with pending mutations',
      entries,
      now: 5000,
      expectedDatabases: ['a'],
      legacyValues: NO_LEGACY,
    });
  }
});

test('dropAllDatabase', async () => {
  const createKVStore = (name: string) => new IDBStore(name);
  const store = new IDBDatabasesStore(createKVStore);
  const numDbs = 10;

  for (const f of [dropAllDatabases, deleteAllReplicacheData] as const) {
    for (let i = 0; i < numDbs; i++) {
      const db = {
        name: `db${i}`,
        replicacheName: `testReplicache${i}`,
        replicacheFormatVersion: 1,
        schemaVersion: 'testSchemaVersion1',
      };

      expect(await store.putDatabase(db)).to.have.property(db.name);
    }

    expect(Object.values(await store.getDatabases())).to.have.length(numDbs);

    const result = await f(createKVStore);

    expect(Object.values(await store.getDatabases())).to.have.length(0);
    expect(result.dropped).to.have.length(numDbs);
    expect(result.errors).to.have.length(0);
  }
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
