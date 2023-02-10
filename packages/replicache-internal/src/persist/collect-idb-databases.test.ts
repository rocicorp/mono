import {expect} from '@esm-bundle/chai';
import {fakeHash} from '../hash.js';
import {TestMemStore} from '../kv/test-mem-store';
import {makeClientMap, setClients} from './clients-test-helpers';
import {
  IDBDatabasesStore,
  IndexedDBDatabase,
  IndexedDBName,
} from './idb-databases-store';
import {
  collectIDBDatabases,
  deleteAllReplicacheData,
} from './collect-idb-databases';
import * as dag from '../dag/mod';
import type {ClientMap} from './clients.js';
import {assertNotUndefined} from '../asserts.js';
import {SinonFakeTimers, useFakeTimers} from 'sinon';
import {REPLICACHE_FORMAT_VERSION} from '../replicache.js';

suite('collectIDBDatabases', async () => {
  let clock: SinonFakeTimers;

  setup(() => {
    clock = useFakeTimers(0);
  });

  teardown(() => {
    clock.restore();
  });

  type Entries = [IndexedDBDatabase, ClientMap][];

  const makeIndexedDBDatabase = (
    name: string,
    lastOpenedTimestampMS = Date.now(),
    replicacheFormatVersion = REPLICACHE_FORMAT_VERSION,
    schemaVersion = 'schemaVersion-' + name,
    replicacheName = 'replicacheName-' + name,
  ): IndexedDBDatabase => ({
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
  ) => {
    for (const legacy of [false, true]) {
      test(name + ' > time ' + now + (legacy ? ' > legacy' : ''), async () => {
        const store = new IDBDatabasesStore(_ => new TestMemStore());
        const clientDagStores = new Map<IndexedDBName, dag.Store>();
        for (const [db, clients] of entries) {
          const dagStore = new dag.TestStore();
          clientDagStores.set(db.name, dagStore);
          if (legacy) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const {lastOpenedTimestampMS: _, ...rest} = db;
            await store.putDatabaseForTesting(rest);
          } else {
            await store.putDatabaseForTesting(db);
          }

          await setClients(clients, dagStore);
        }

        const newDagStore = (name: string) => {
          const dagStore = clientDagStores.get(name);
          assertNotUndefined(dagStore);
          return dagStore;
        };

        const maxAge = 1000;

        const controller = new AbortController();
        await collectIDBDatabases(
          store,
          controller.signal,
          now,
          maxAge,
          newDagStore,
        );

        expect(Object.keys(await store.getDatabases())).to.deep.equal(
          expectedDatabases,
        );
      });
    }
  };

  t('empty', [], 0, []);

  {
    const entries: Entries = [
      [
        makeIndexedDBDatabase('a', 0),
        makeClientMap({
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
        makeIndexedDBDatabase('a', 0),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
      [
        makeIndexedDBDatabase('b', 1000),
        makeClientMap({
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
        makeIndexedDBDatabase('a', 2000),
        makeClientMap({
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
        makeIndexedDBDatabase('b', 1000),
        makeClientMap({
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
        makeIndexedDBDatabase('a', 3000),
        makeClientMap({
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
        makeIndexedDBDatabase('b', 4000),
        makeClientMap({
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
        makeIndexedDBDatabase('a', 0, REPLICACHE_FORMAT_VERSION + 1),
        makeClientMap({
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
        makeIndexedDBDatabase('a', 0, REPLICACHE_FORMAT_VERSION - 1),
        makeClientMap({
          clientA1: {
            headHash: fakeHash('a1'),
            heartbeatTimestampMs: 0,
          },
        }),
      ],
    ];
    t('one idb, one client, old format version', entries, 0, ['a']);
    t('one idb, one client, old format version', entries, 1000, []);
  }
});

test('deleteAllReplicacheData', async () => {
  const store = new IDBDatabasesStore();
  const numDbs = 10;

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

  const result = await deleteAllReplicacheData();

  expect(Object.values(await store.getDatabases())).to.have.length(0);
  expect(result.dropped).to.have.length(numDbs);
  expect(result.errors).to.have.length(0);
});
