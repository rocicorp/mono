import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from 'zero-cache/src/test/lite.js';
import type {
  Downstream,
  PokePartBody,
  PokeStartBody,
  QueriesPatch,
} from 'zero-protocol';
import type {AST} from 'zql/src/zql/ast/ast.js';
import {Database} from 'zqlite/src/db.js';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from '../replicator/replicator.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.js';
import {CVRStore} from './cvr-store.js';
import {
  ClientGroupStorage,
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from './database-storage.js';
import {PipelineDriver} from './pipeline-driver.js';
import {initViewSyncerSchema} from './schema/pg-migrations.js';
import {Snapshotter} from './snapshotter.js';
import {SyncContext, ViewSyncerService} from './view-syncer.js';

const EXPECTED_LMIDS_AST: AST = {
  schema: '',
  table: 'zero.clients',
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

describe('view-syncer/service', () => {
  let storageDB: Database;
  let replicaDbFile: DbFile;
  let replica: Database;
  let cvrDB: PostgresDB;
  const lc = createSilentLogContext();
  let versionNotifications: Subscription<ReplicaVersionReady>;

  let operatorStorage: ClientGroupStorage;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let client1: Queue<Downstream>;

  const SYNC_CONTEXT = {clientID: 'foo', wsID: 'ws1', baseCookie: null};

  const messages = new ReplicationMessages({issues: 'id', users: 'id'});

  beforeEach(async () => {
    const lc = createSilentLogContext();
    storageDB = new Database(lc, ':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();

    replicaDbFile = new DbFile('view_syncer_service_test');
    replica = replicaDbFile.connect(lc);
    initChangeLog(replica);
    initReplicationState(replica, ['zero_data'], '01');

    replica.exec(`
    CREATE TABLE "zero.clients" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "lastMutationID" INTEGER,
      "userID"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID")
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

    INSERT INTO "zero.clients" ("clientGroupID", "clientID", "lastMutationID", _0_version)
                      VALUES ('9876', 'foo', 42, '00');

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

    versionNotifications = Subscription.create();
    operatorStorage = new DatabaseStorage(storageDB).createClientGroupStorage(
      serviceID,
    );
    vs = new ViewSyncerService(
      lc,
      serviceID,
      cvrDB,
      new PipelineDriver(
        lc,
        new Snapshotter(lc, replicaDbFile.path),
        operatorStorage,
      ),
      versionNotifications,
    );
    viewSyncerDone = vs.run();
    client1 = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
  });

  async function connect(ctx: SyncContext, desiredQueriesPatch: QueriesPatch) {
    const stream = await vs.initConnection(ctx, [
      'initConnection',
      {desiredQueriesPatch},
    ]);
    const downstream = new Queue<Downstream>();
    void pipeToQueue(stream, downstream);
    return downstream;
  }

  async function pipeToQueue(
    stream: AsyncIterable<Downstream>,
    queue: Queue<Downstream>,
  ) {
    try {
      for await (const msg of stream) {
        await queue.enqueue(msg);
      }
    } catch (e) {
      await queue.enqueueRejection(e);
    }
  }

  async function nextPoke(
    client: Queue<Downstream> = client1,
  ): Promise<Downstream[]> {
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

  const USERS_QUERY: AST = {
    table: 'users',
  };

  test('adds desired queries from initConnectionMessage', async () => {
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
    versionNotifications.push({});
    expect(await nextPoke()).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:02",
            "pokeID": "00:02",
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
            "entitiesPatch": [
              {
                "entityID": {
                  "id": "1",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "entityID": {
                  "id": "2",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "entityID": {
                  "id": "3",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "big": 123,
                  "id": "3",
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "entityID": {
                  "id": "4",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "big": 100,
                  "id": "4",
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
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
          "table": "zero.clients",
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

  test('process advancement', async () => {
    versionNotifications.push({});
    expect((await nextPoke())[0]).toEqual([
      'pokeStart',
      {
        baseCookie: null,
        cookie: '00:02',
        pokeID: '00:02',
      },
    ]);

    const replicator = fakeReplicator(lc, replica);
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

    versionNotifications.push({});
    expect(await nextPoke()).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:02",
            "cookie": "01",
            "pokeID": "01",
          },
        ],
        [
          "pokePart",
          {
            "entitiesPatch": [
              {
                "entityID": {
                  "id": "1",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "entityID": {
                  "id": "2",
                },
                "entityType": "issues",
                "op": "del",
              },
            ],
            "pokeID": "01",
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
          "table": "zero.clients",
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
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
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

  test('catch up client', async () => {
    versionNotifications.push({});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: null,
      cookie: '00:02',
      pokeID: '00:02',
    });

    const replicator = fakeReplicator(lc, replica);
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

    versionNotifications.push({});
    const advancement = (await nextPoke(client1))[1][1] as PokePartBody;
    expect(advancement).toEqual({
      entitiesPatch: [
        {
          entityID: {id: '1'},
          entityType: 'issues',
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
          entityID: {id: '2'},
          entityType: 'issues',
          op: 'del',
        },
      ],
      pokeID: '01',
    });

    // Connect with another client (i.e. tab) at older version '00:02'
    // (i.e. pre-advancement).
    const client2 = await connect(
      {clientID: 'bar', wsID: '9382', baseCookie: preAdvancement.cookie},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    // Response should catch client2 up with the entitiesPatch from
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
            "entitiesPatch": [
              {
                "entityID": {
                  "id": "1",
                },
                "entityType": "issues",
                "op": "put",
                "value": {
                  "_0_version": "01",
                  "big": 9007199254740991,
                  "id": "1",
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "entityID": {
                  "id": "2",
                },
                "entityType": "issues",
                "op": "del",
              },
            ],
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
});
