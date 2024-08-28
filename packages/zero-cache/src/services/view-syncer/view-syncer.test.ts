import Database from 'better-sqlite3';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from 'zero-cache/src/test/lite.js';
import type {Downstream} from 'zero-protocol';
import type {AST} from 'zql/src/zql/ast2/ast.js';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from '../replicator/replicator.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {CVRStore} from './cvr-store.js';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.js';
import {PipelineDriver} from './pipeline-driver.js';
import {initViewSyncerSchema} from './schema/pg-migrations.js';
import {Snapshotter} from './snapshotter.js';
import {ViewSyncerService} from './view-syncer.js';

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
  orderBy: [['clientID', 'asc']],
};

describe('view-syncer/service', () => {
  let storageDB: Database.Database;
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  const lc = createSilentLogContext();
  let versionNotifications: Subscription<ReplicaVersionReady>;

  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let downstream: Queue<Downstream>;

  const SYNC_CONTEXT = {clientID: 'foo', wsID: 'ws1', baseCookie: null};

  beforeEach(async () => {
    storageDB = new Database(':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();

    replicaDbFile = new DbFile('view_syncer_service_test');
    const replica = replicaDbFile.connect();
    initChangeLog(replica);
    initReplicationState(replica, ['zero_data'], '0/1');

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
      owner_id text,
      parent_id text,
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
                      VALUES ('9876', 'foo', 42, '0a');

    INSERT INTO users (id, name, _0_version) VALUES ('100', 'Alice', '0a');
    INSERT INTO users (id, name, _0_version) VALUES ('101', 'Bob', '0b');
    INSERT INTO users (id, name, _0_version) VALUES ('102', 'Candice', '0c');

    INSERT INTO issues (id, title, owner_id, big, _0_version) VALUES ('1', 'parent issue foo', 100, 9007199254740991, '1a0');
    INSERT INTO issues (id, title, owner_id, big, _0_version) VALUES ('2', 'parent issue bar', 101, -9007199254740991, '1ab');
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('3', 'foo', 102, 1, 123, '1ca');
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('4', 'bar', 101, 2, 100, '1cd');
    -- The last row should not match the ISSUES_TITLE_QUERY: "WHERE id IN (1, 2, 3, 4)"
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('5', 'not matched', 101, 2, 100, '1cd');
    `);

    cvrDB = await testDBs.create('view_syncer_service_test');
    await initViewSyncerSchema(lc, 'view-syncer', 'cvr', cvrDB);

    versionNotifications = Subscription.create();
    vs = new ViewSyncerService(
      lc,
      serviceID,
      cvrDB,
      new PipelineDriver(
        lc,
        new Snapshotter(lc, replicaDbFile.path),
        new DatabaseStorage(storageDB).createClientGroupStorage(serviceID),
      ),
      versionNotifications,
    );
    viewSyncerDone = vs.run();
    downstream = new Queue();
    const stream = await vs.initConnection(SYNC_CONTEXT, [
      'initConnection',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash1', ast: ISSUES_TITLE_QUERY},
        ],
      },
    ]);
    void pipeToQueue(stream, downstream);
  });

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

  async function nextPoke(): Promise<Downstream[]> {
    const received: Downstream[] = [];
    for (;;) {
      const msg = await downstream.dequeue();
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

  const ISSUES_TITLE_QUERY: AST = {
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

  const USERS_NAME_QUERY: AST = {
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
          ast: ISSUES_TITLE_QUERY,
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
          {op: 'put', hash: 'query-hash-1234567890', ast: USERS_NAME_QUERY},
        ],
      },
    ]);

    // Change the set of queries.
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash2', ast: USERS_NAME_QUERY},
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
          ast: USERS_NAME_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 2}},
          id: 'query-hash2',
        },
      },
      version: {stateVersion: '00', minorVersion: 2},
    });
  });

  test('initial hydration', async () => {
    versionNotifications.push({});
    // TODO: Get RowPatches working.
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
  });

  // Does not test the actual timeout logic, but better than nothing.
  test('keepalive return value', () => {
    expect(vs.keepalive()).toBe(true);
    void vs.stop();
    expect(vs.keepalive()).toBe(false);
  });
});
