import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../shared/src/queue.js';
import {sleep} from '../../../../shared/src/sleep.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import {
  type Downstream,
  ErrorKind,
  type ErrorMessage,
  type PokePartBody,
  type PokeStartBody,
  type QueriesPatch,
} from '../../../../zero-protocol/src/mod.js';
import {Database} from '../../../../zqlite/src/db.js';
import {StatementRunner} from '../../db/statements.js';
import {testDBs} from '../../test/db.js';
import {DbFile} from '../../test/lite.js';
import {ErrorForClient} from '../../types/error-for-client.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import type {ReplicaState} from '../replicator/replicator.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.js';
import {
  fakeReplicator,
  type FakeReplicator,
  ReplicationMessages,
} from '../replicator/test-utils.js';
import {CVRStore} from './cvr-store.js';
import {CVRQueryDrivenUpdater} from './cvr.js';
import {
  type ClientGroupStorage,
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from './database-storage.js';
import {DrainCoordinator} from './drain-coordinator.js';
import {PipelineDriver} from './pipeline-driver.js';
import {initViewSyncerSchema} from './schema/init.js';
import {Snapshotter} from './snapshotter.js';
import {type SyncContext, ViewSyncerService} from './view-syncer.js';

const SHARD_ID = 'ABC';

const EXPECTED_LMIDS_AST: AST = {
  schema: '',
  table: 'zero_ABC.clients',
  where: [
    {
      type: 'simple',
      op: '=',
      field: 'clientGroupID',
      value: '9876',
    },
  ],
  orderBy: [
    ['clientGroupID', 'asc'],
    ['clientID', 'asc'],
  ],
};

const REPLICA_VERSION = '01';

describe('view-syncer/service', () => {
  let storageDB: Database;
  let replicaDbFile: DbFile;
  let replica: Database;
  let cvrDB: PostgresDB;
  const lc = createSilentLogContext();
  let stateChanges: Subscription<ReplicaState>;
  let drainCoordinator: DrainCoordinator;

  let operatorStorage: ClientGroupStorage;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let replicator: FakeReplicator;

  const SYNC_CONTEXT = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    schemaVersion: 2,
  };

  const messages = new ReplicationMessages({issues: 'id', users: 'id'});
  const zeroMessages = new ReplicationMessages(
    {schemaVersions: 'lock'},
    'zero',
  );

  beforeEach(async () => {
    storageDB = new Database(lc, ':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();

    replicaDbFile = new DbFile('view_syncer_service_test');
    replica = replicaDbFile.connect(lc);
    initChangeLog(replica);
    initReplicationState(replica, ['zero_data'], REPLICA_VERSION);

    replica.pragma('journal_mode = WAL');
    replica.pragma('busy_timeout = 1');
    replica.exec(`
    CREATE TABLE "zero_ABC.clients" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "lastMutationID" INTEGER,
      "userID"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID")
    );
    CREATE TABLE "zero.schemaVersions" (
      "lock"                INTEGER PRIMARY KEY,
      "minSupportedVersion" INTEGER,
      "maxSupportedVersion" INTEGER,
      _0_version            TEXT NOT NULL
    );
    CREATE TABLE issues (
      id text PRIMARY KEY,
      owner text,
      parent text,
      big INTEGER,
      title text,
      _0_version TEXT NOT NULL
    );
    CREATE TABLE users (
      id text PRIMARY KEY,
      name text,
      _0_version TEXT NOT NULL
    );

    INSERT INTO "zero_ABC.clients" ("clientGroupID", "clientID", "lastMutationID", _0_version)
      VALUES ('9876', 'foo', 42, '00');
    INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
      VALUES (1, 2, 3, '00');  

    INSERT INTO users (id, name, _0_version) VALUES ('100', 'Alice', '00');
    INSERT INTO users (id, name, _0_version) VALUES ('101', 'Bob', '00');
    INSERT INTO users (id, name, _0_version) VALUES ('102', 'Candice', '00');

    INSERT INTO issues (id, title, owner, big, _0_version) VALUES ('1', 'parent issue foo', 100, 9007199254740991, '00');
    INSERT INTO issues (id, title, owner, big, _0_version) VALUES ('2', 'parent issue bar', 101, -9007199254740991, '00');
    INSERT INTO issues (id, title, owner, parent, big, _0_version) VALUES ('3', 'foo', 102, 1, 123, '00');
    INSERT INTO issues (id, title, owner, parent, big, _0_version) VALUES ('4', 'bar', 101, 2, 100, '00');
    -- The last row should not match the ISSUES_TITLE_QUERY: "WHERE id IN (1, 2, 3, 4)"
    INSERT INTO issues (id, title, owner, parent, big, _0_version) VALUES ('5', 'not matched', 101, 2, 100, '00');
    `);

    cvrDB = await testDBs.create('view_syncer_service_test');
    await initViewSyncerSchema(lc, cvrDB);

    replicator = fakeReplicator(lc, replica);
    stateChanges = Subscription.create();
    drainCoordinator = new DrainCoordinator();
    operatorStorage = new DatabaseStorage(storageDB).createClientGroupStorage(
      serviceID,
    );
    vs = new ViewSyncerService(
      lc,
      serviceID,
      SHARD_ID,
      cvrDB,
      new PipelineDriver(
        lc.withContext('component', 'pipeline-driver'),
        new Snapshotter(lc, replicaDbFile.path),
        operatorStorage,
      ),
      stateChanges,
      drainCoordinator,
    );
    viewSyncerDone = vs.run();
  });

  async function connect(ctx: SyncContext, desiredQueriesPatch: QueriesPatch) {
    const stream = await vs.initConnection(ctx, [
      'initConnection',
      {desiredQueriesPatch},
    ]);
    const downstream = new Queue<Downstream>();

    void (async function () {
      try {
        for await (const msg of stream) {
          await downstream.enqueue(msg);
        }
      } catch (e) {
        await downstream.enqueueRejection(e);
      }
    })();

    return downstream;
  }

  async function nextPoke(client: Queue<Downstream>): Promise<Downstream[]> {
    const received: Downstream[] = [];
    for (;;) {
      const msg = await client.dequeue();
      received.push(msg);
      if (msg[0] === 'pokeEnd') {
        break;
      }
    }
    return received;
  }

  async function expectNoPokes(client: Queue<Downstream>) {
    // Use the dequeue() API that cancels the dequeue() request after a timeout.
    const timedOut = 'nothing' as unknown as Downstream;
    expect(await client.dequeue(timedOut, 10)).toBe(timedOut);
  }

  afterEach(async () => {
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    await replicaDbFile.unlink();
  });

  const serviceID = '9876';

  const ISSUES_QUERY: AST = {
    table: 'issues',
    where: [
      {
        type: 'simple',
        field: 'id',
        op: 'IN',
        value: ['1', '2', '3', '4'],
      },
    ],
    orderBy: [['id', 'asc']],
  };

  const ISSUES_QUERY2: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
  };

  const USERS_QUERY: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
  };

  test('adds desired queries from initConnectionMessage', async () => {
    await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    const cvrStore = new CVRStore(lc, cvrDB, serviceID);
    const cvr = await cvrStore.load();
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'query-hash1': {
          ast: ISSUES_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', minorVersion: 1},
    });
  });

  test('responds to changeQueriesPatch', async () => {
    await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Ignore messages from an old websockets.
    await vs.changeDesiredQueries({...SYNC_CONTEXT, wsID: 'old-wsid'}, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash-1234567890', ast: USERS_QUERY},
        ],
      },
    ]);

    // Change the set of queries.
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash2', ast: USERS_QUERY},
          {op: 'del', hash: 'query-hash1'},
        ],
      },
    ]);

    const cvrStore = new CVRStore(lc, cvrDB, serviceID);
    const cvr = await cvrStore.load();
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: EXPECTED_LMIDS_AST,
          internal: true,
          id: 'lmids',
        },
        'query-hash2': {
          ast: USERS_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 2}},
          id: 'query-hash2',
        },
      },
      version: {stateVersion: '00', minorVersion: 2},
    });
  });

  test('initial hydration', async () => {
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:02",
            "pokeID": "00:02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "clientsPatch": [
              {
                "clientID": "foo",
                "op": "put",
              },
            ],
            "desiredQueriesPatches": {
              "foo": [
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                    "where": [
                      {
                        "field": "id",
                        "op": "IN",
                        "type": "simple",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                    ],
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "ast": {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "issues",
                  "where": [
                    {
                      "field": "id",
                      "op": "IN",
                      "type": "simple",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                  ],
                },
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "00:02",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:02",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "zero_ABC.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('initial hydration, schemaVersion unsupported', async () => {
    const client = await connect({...SYNC_CONTEXT, schemaVersion: 1}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorMessage', [
      'error',
      'SchemaVersionNotSupported',
      'Schema version 1 is not in range of supported schema versions [2, 3].',
    ]);
  });

  test('initial hydration, schemaVersion unsupported with bad query', async () => {
    // Simulate a connection when the replica is already ready.
    stateChanges.push({state: 'version-ready'});
    await sleep(5);

    const client = await connect({...SYNC_CONTEXT, schemaVersion: 1}, [
      {
        op: 'put',
        hash: 'query-hash1',
        ast: {
          ...ISSUES_QUERY,
          // simulate an "invalid" query for an old schema version with an empty orderBy
          orderBy: [],
        },
      },
    ]);

    // Make sure it's the SchemaVersionNotSupported error that gets
    // propagated, and not any error related to the bad query.
    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorMessage', [
      'error',
      'SchemaVersionNotSupported',
      'Schema version 1 is not in range of supported schema versions [2, 3].',
    ]);
  });

  test('process advancement', async () => {
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: null,
        cookie: '00:02',
        pokeID: '00:02',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:02",
            "cookie": "01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "zero_ABC.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "00:02",
          "refCounts": {
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('process advancement that results in client having an unsupported schemaVersion', async () => {
    const client1 = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    const client2 = await connect(
      {...SYNC_CONTEXT, clientID: 'bar', schemaVersion: 3},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client1))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: null,
        cookie: '00:03',
        pokeID: '00:03',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);
    expect((await nextPoke(client2))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: null,
        cookie: '00:03',
        pokeID: '00:03',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
      zeroMessages.update('schemaVersions', {
        lock: true,
        minSupportedVersion: 3,
      }),
    );

    stateChanges.push({state: 'version-ready'});

    // client1 now has an unsupported version and is sent an error and no poke
    // client2 still has a supported version and is sent a poke with the
    // updated schemaVersions range
    const dequeuePromise = client1.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorMessage', [
      'error',
      'SchemaVersionNotSupported',
      'Schema version 2 is not in range of supported schema versions [3, 3].',
    ]);

    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:03",
            "cookie": "01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 3,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "01",
          },
        ],
      ]
    `);
  });

  test('process advancement with schema change', async () => {
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: null,
        cookie: '00:02',
        pokeID: '00:02',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction(
      '07',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    stateChanges.push({state: 'version-ready'});

    // The poke that encountered the schema change should be canceled.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:02",
            "cookie": "01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokeEnd",
          {
            "cancel": true,
            "pokeID": "01",
          },
        ],
      ]
    `);

    // The "newColumn" should be arrive in the nextPoke.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:02",
            "cookie": "01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "newColumn": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "newColumn": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "newColumn": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "newColumn": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "01",
          },
        ],
      ]
    `);
  });

  test('catch up client', async () => {
    const client1 = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: null,
      cookie: '00:02',
      pokeID: '00:02',
      schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
    });

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    const advancement = (await nextPoke(client1))[1][1] as PokePartBody;
    expect(advancement).toEqual({
      rowsPatch: [
        {
          tableName: 'issues',
          op: 'put',
          value: {
            big: 9007199254740991,
            id: '1',
            owner: '100.0',
            parent: null,
            title: 'new title',
          },
        },
        {
          id: {id: '2'},
          tableName: 'issues',
          op: 'del',
        },
      ],
      pokeID: '01',
    });

    // Connect with another client (i.e. tab) at older version '00:02'
    // (i.e. pre-advancement).
    const client2 = await connect(
      {
        clientID: 'bar',
        wsID: '9382',
        baseCookie: preAdvancement.cookie,
        schemaVersion: 2,
      },
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    // Response should catch client2 up with the rowsPatch from
    // the advancement.
    const response2 = await nextPoke(client2);
    expect(response2[1][1]).toMatchObject({
      ...advancement,
      pokeID: '01:01',
    });
    expect(response2).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:02",
            "cookie": "01:01",
            "pokeID": "01:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "clientsPatch": [
              {
                "clientID": "bar",
                "op": "put",
              },
            ],
            "desiredQueriesPatches": {
              "bar": [
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                    "where": [
                      {
                        "field": "id",
                        "op": "IN",
                        "type": "simple",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                    ],
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "01:01",
          },
        ],
      ]
    `);

    // client1 should be poked to get the new client2 config,
    // but no new entities.
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "01:01",
            "pokeID": "01:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "clientsPatch": [
              {
                "clientID": "bar",
                "op": "put",
              },
            ],
            "desiredQueriesPatches": {
              "bar": [
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                    "where": [
                      {
                        "field": "id",
                        "op": "IN",
                        "type": "simple",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                    ],
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "01:01",
          },
        ],
      ]
    `);
  });

  test('waits for replica to catch up', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '00'.
    const cvrStore = new CVRStore(lc, cvrDB, serviceID);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(),
      '07',
      REPLICA_VERSION,
    ).flush(lc);

    // Connect the client.
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Manually simulate advancements in the replica.
    const db = new StatementRunner(replica);
    replica.prepare(`DELETE from issues where id = '1'`).run();
    updateReplicationWatermark(db, '03');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '2'`).run();
    updateReplicationWatermark(db, '05');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '3'`).run();
    updateReplicationWatermark(db, '07');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica
      .prepare(`UPDATE issues SET title = 'caught up' where id = '4'`)
      .run();
    updateReplicationWatermark(db, '09'); // Caught up with stateVersion=07, watermark=09.
    stateChanges.push({state: 'version-ready'});

    // The single poke should only contain issues {id='4', title='caught up'}
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "07:02",
            "pokeID": "07:02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "clientsPatch": [
              {
                "clientID": "foo",
                "op": "put",
              },
            ],
            "desiredQueriesPatches": {
              "foo": [
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                    "where": [
                      {
                        "field": "id",
                        "op": "IN",
                        "type": "simple",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                    ],
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "ast": {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "issues",
                  "where": [
                    {
                      "field": "id",
                      "op": "IN",
                      "type": "simple",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                  ],
                },
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "07:02",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "owner": "101",
                  "parent": "2",
                  "title": "caught up",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "07:02",
          },
        ],
      ]
    `);
  });

  test('sends reset for CVR from different replica version up', async () => {
    const cvrStore = new CVRStore(lc, cvrDB, serviceID);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(),
      '07',
      '1' + REPLICA_VERSION, // Different replica version.
    ).flush(lc);

    // Connect the client.
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorMessage).toEqual([
      'error',
      ErrorKind.ClientNotFound,
      'Replica Version mismatch: CVR=101, DB=01',
    ] satisfies ErrorMessage);
  });

  test('clean up operator storage on close', async () => {
    const storage = operatorStorage.createStorage();
    storage.set('foo', 'bar');
    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(1);

    await vs.stop();
    await viewSyncerDone;

    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(0);
  });

  // Does not test the actual timeout logic, but better than nothing.
  test('keepalive return value', () => {
    expect(vs.keepalive()).toBe(true);
    void vs.stop();
    expect(vs.keepalive()).toBe(false);
  });

  test('elective drain', async () => {
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
      {op: 'put', hash: 'query-hash3', ast: USERS_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    // This should result in computing a non-zero hydration time.
    await nextPoke(client);

    drainCoordinator.drainNextIn(0);
    expect(drainCoordinator.shouldDrain()).toBe(true);
    const now = Date.now();
    await sleep(3); // Bump time forward to verify that the timeout is reset later.

    // Enqueue a dummy task so that the view-syncer can elect to drain.
    stateChanges.push({state: 'version-ready'});

    // Upon completion, the view-syncer should have called drainNextIn()
    // with its hydration time so that the next drain is not triggered
    // until that interval elapses.
    await viewSyncerDone;
    expect(drainCoordinator.nextDrainTime).toBeGreaterThan(now);
  });
});
