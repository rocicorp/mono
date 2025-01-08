import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Queue} from '../../../../shared/src/queue.js';
import {sleep} from '../../../../shared/src/sleep.js';
import type {AST} from '../../../../zero-protocol/src/ast.js';
import {
  type Downstream,
  type ErrorBody,
  ErrorKind,
  type PokePartBody,
  type PokeStartBody,
  type QueriesPatch,
} from '../../../../zero-protocol/src/mod.js';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.js';
import type {PermissionsConfig} from '../../../../zero-schema/src/compiled-permissions.js';
import {definePermissions} from '../../../../zero-schema/src/permissions.js';
import type {ExpressionBuilder} from '../../../../zql/src/query/expression.js';
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
import {pickToken, type SyncContext, ViewSyncerService} from './view-syncer.js';

const SHARD_ID = 'ABC';

const EXPECTED_LMIDS_AST: AST = {
  schema: '',
  table: 'zero_ABC.clients',
  where: {
    type: 'simple',
    op: '=',
    left: {
      type: 'column',
      name: 'clientGroupID',
    },
    right: {
      type: 'literal',
      value: '9876',
    },
  },
  orderBy: [
    ['clientGroupID', 'asc'],
    ['clientID', 'asc'],
  ],
};

const ON_FAILURE = (e: unknown) => {
  throw e;
};

const REPLICA_VERSION = '01';
const TASK_ID = 'foo-task';
const serviceID = '9876';
const ISSUES_QUERY: AST = {
  table: 'issues',
  where: {
    type: 'simple',
    left: {
      type: 'column',
      name: 'id',
    },
    op: 'IN',
    right: {
      type: 'literal',
      value: ['1', '2', '3', '4'],
    },
  },
  orderBy: [['id', 'asc']],
};

const COMMENTS_QUERY: AST = {
  table: 'comments',
  orderBy: [['id', 'asc']],
};

const ISSUES_QUERY_WITH_EXISTS: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'issueLabels',
        alias: 'labels',
        orderBy: [
          ['issueID', 'asc'],
          ['labelID', 'asc'],
        ],
        where: {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            system: 'client',
            correlation: {
              parentField: ['labelID'],
              childField: ['id'],
            },
            subquery: {
              table: 'labels',
              alias: 'labels',
              orderBy: [['id', 'asc']],
              where: {
                type: 'simple',
                left: {
                  type: 'column',
                  name: 'name',
                },
                op: '=',
                right: {
                  type: 'literal',
                  value: 'bug',
                },
              },
            },
          },
        },
      },
    },
  },
};

const ISSUES_QUERY_WITH_RELATED: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'simple',
    left: {
      type: 'column',
      name: 'id',
    },
    op: 'IN',
    right: {
      type: 'literal',
      value: ['1', '2'],
    },
  },
  related: [
    {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      hidden: true,
      subquery: {
        table: 'issueLabels',
        alias: 'labels',
        orderBy: [
          ['issueID', 'asc'],
          ['labelID', 'asc'],
        ],
        related: [
          {
            system: 'client',
            correlation: {
              parentField: ['labelID'],
              childField: ['id'],
            },
            subquery: {
              table: 'labels',
              alias: 'labels',
              orderBy: [['id', 'asc']],
            },
          },
        ],
      },
    },
  ],
};

const ISSUES_QUERY2: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
};

const USERS_QUERY: AST = {
  table: 'users',
  orderBy: [['id', 'asc']],
};

const issues = {
  tableName: 'issues',
  columns: {
    id: {type: 'string'},
    title: {type: 'string'},
    owner: {type: 'string'},
    parent: {type: 'string'},
    big: {type: 'number'},
    json: {type: 'json'},
  },
  primaryKey: ['id'],
  relationships: {},
} as const;

const comments = {
  tableName: 'comments',
  columns: {
    id: {type: 'string'},
    issueID: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issue: {
      sourceField: 'issueID',
      destField: 'id',
      destSchema: issues,
    },
  },
} as const;

const schema = {
  version: 1,
  tables: {
    issues,
    comments,
  },
} as const;

type AuthData = {
  sub: string;
  role: 'user' | 'admin';
  iat: number;
};
const canSeeIssue = (
  authData: AuthData,
  eb: ExpressionBuilder<typeof schema.tables.issues>,
) => eb.cmpLit(authData.role, '=', 'admin');
const permissions: PermissionsConfig | undefined = await definePermissions<
  AuthData,
  typeof schema
>(schema, () => ({
  issues: {
    row: {
      select: [canSeeIssue],
    },
  },
  comments: {
    row: {
      select: [
        (authData, eb: ExpressionBuilder<typeof schema.tables.comments>) =>
          eb.exists('issue', iq =>
            iq.where(({eb}) => canSeeIssue(authData, eb)),
          ),
      ],
    },
  },
}));

async function setup(permissions: PermissionsConfig = {}) {
  const lc = createSilentLogContext();
  const storageDB = new Database(lc, ':memory:');
  storageDB.prepare(CREATE_STORAGE_TABLE).run();

  const replicaDbFile = new DbFile('view_syncer_service_test');
  const replica = replicaDbFile.connect(lc);
  initChangeLog(replica);
  initReplicationState(replica, ['zero_data'], REPLICA_VERSION);

  replica.pragma('journal_mode = WAL2');
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
    json JSON,
    _0_version TEXT NOT NULL
  );
  CREATE TABLE "issueLabels" (
    issueID TEXT,
    labelID TEXT,
    _0_version TEXT NOT NULL,
    PRIMARY KEY (issueID, labelID)
  );
  CREATE TABLE "labels" (
    id TEXT PRIMARY KEY,
    name TEXT,
    _0_version TEXT NOT NULL
  );
  CREATE TABLE users (
    id text PRIMARY KEY,
    name text,
    _0_version TEXT NOT NULL
  );
  CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    issueID TEXT,
    text TEXT,
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
  INSERT INTO issues (id, title, owner, parent, big, json, _0_version) VALUES 
    ('5', 'not matched', 101, 2, 100, '[123,{"foo":456,"bar":789},"baz"]', '00');

  INSERT INTO "issueLabels" (issueID, labelID, _0_version) VALUES ('1', '1', '00');
  INSERT INTO "labels" (id, name, _0_version) VALUES ('1', 'bug', '00');

  INSERT INTO "comments" (id, issueID, text, _0_version) VALUES ('1', '1', 'comment 1', '00');
  INSERT INTO "comments" (id, issueID, text, _0_version) VALUES ('2', '1', 'comment 2', '00');
  `);

  const cvrDB = await testDBs.create('view_syncer_service_test');
  await initViewSyncerSchema(lc, cvrDB);

  const replicator = fakeReplicator(lc, replica);
  const stateChanges: Subscription<ReplicaState> = Subscription.create();
  const drainCoordinator = new DrainCoordinator();
  const operatorStorage = new DatabaseStorage(
    storageDB,
  ).createClientGroupStorage(serviceID);
  const vs = new ViewSyncerService(
    lc,
    TASK_ID,
    serviceID,
    SHARD_ID,
    cvrDB,
    new PipelineDriver(
      lc.withContext('component', 'pipeline-driver'),
      new Snapshotter(lc, replicaDbFile.path),
      operatorStorage,
      'view-syncer.pg-test.ts',
    ),
    stateChanges,
    drainCoordinator,
    permissions,
  );
  const viewSyncerDone = vs.run();

  function connect(ctx: SyncContext, desiredQueriesPatch: QueriesPatch) {
    const stream = vs.initConnection(ctx, [
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

  return {
    storageDB,
    replicaDbFile,
    replica,
    cvrDB,
    stateChanges,
    drainCoordinator,
    operatorStorage,
    vs,
    viewSyncerDone,
    replicator,
    connect,
    nextPoke,
    expectNoPokes,
  };
}

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
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: QueriesPatch,
  ) => Queue<Downstream>;
  let nextPoke: (client: Queue<Downstream>) => Promise<Downstream[]>;
  let expectNoPokes: (client: Queue<Downstream>) => Promise<void>;

  const SYNC_CONTEXT = {
    clientID: 'foo',
    wsID: 'ws1',
    protocolVersion: PROTOCOL_VERSION,
    baseCookie: null,
    schemaVersion: 2,
    tokenData: undefined,
  };

  const messages = new ReplicationMessages({
    issues: 'id',
    users: 'id',
    issueLabels: ['issueID', 'labelID'],
  });
  const zeroMessages = new ReplicationMessages(
    {schemaVersions: 'lock'},
    'zero',
  );

  beforeEach(async () => {
    ({
      storageDB,
      replicaDbFile,
      replica,
      cvrDB,
      stateChanges,
      drainCoordinator,
      operatorStorage,
      vs,
      viewSyncerDone,
      replicator,
      connect,
      nextPoke,
      expectNoPokes,
    } = await setup());
  });

  afterEach(async () => {
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  test('adds desired queries from initConnectionMessage', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client);

    const cvrStore = new CVRStore(lc, cvrDB, TASK_ID, serviceID, ON_FAILURE);
    const cvr = await cvrStore.load(lc, Date.now());
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
    connect(SYNC_CONTEXT, [
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

    const cvrStore = new CVRStore(lc, cvrDB, TASK_ID, serviceID, ON_FAILURE);
    const cvr = await cvrStore.load(lc, Date.now());
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
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
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
                  "where": {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "IN",
                    "right": {
                      "type": "literal",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                    "type": "simple",
                  },
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
                "id": {
                  "id": "1",
                },
                "op": "put",
                "rest": {
                  "big": 9007199254740991,
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "put",
                "rest": {
                  "big": -9007199254740991,
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "put",
                "rest": {
                  "big": 123,
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "put",
                "rest": {
                  "big": 100,
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
                "tableName": "issues",
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

  test('initial hydration, rows in multiple queries', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                  },
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
              [
                [
                  "pokeStart",
                  {
                    "baseCookie": "00:01",
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
                          "where": {
                            "left": {
                              "name": "id",
                              "type": "column",
                            },
                            "op": "IN",
                            "right": {
                              "type": "literal",
                              "value": [
                                "1",
                                "2",
                                "3",
                                "4",
                              ],
                            },
                            "type": "simple",
                          },
                        },
                        "hash": "query-hash1",
                        "op": "put",
                      },
                      {
                        "ast": {
                          "orderBy": [
                            [
                              "id",
                              "asc",
                            ],
                          ],
                          "table": "issues",
                        },
                        "hash": "query-hash2",
                        "op": "put",
                      },
                    ],
                    "lastMutationIDChanges": {
                      "foo": 42,
                    },
                    "pokeID": "00:02",
                    "rowsPatch": [
                      {
                        "id": {
                          "id": "1",
                        },
                        "op": "put",
                        "rest": {
                          "big": 9007199254740991,
                          "json": null,
                          "owner": "100",
                          "parent": null,
                          "title": "parent issue foo",
                        },
                        "tableName": "issues",
                      },
                      {
                        "id": {
                          "id": "2",
                        },
                        "op": "put",
                        "rest": {
                          "big": -9007199254740991,
                          "json": null,
                          "owner": "101",
                          "parent": null,
                          "title": "parent issue bar",
                        },
                        "tableName": "issues",
                      },
                      {
                        "id": {
                          "id": "3",
                        },
                        "op": "put",
                        "rest": {
                          "big": 123,
                          "json": null,
                          "owner": "102",
                          "parent": "1",
                          "title": "foo",
                        },
                        "tableName": "issues",
                      },
                      {
                        "id": {
                          "id": "4",
                        },
                        "op": "put",
                        "rest": {
                          "big": 100,
                          "json": null,
                          "owner": "101",
                          "parent": "2",
                          "title": "bar",
                        },
                        "tableName": "issues",
                      },
                      {
                        "id": {
                          "id": "5",
                        },
                        "op": "put",
                        "rest": {
                          "big": 100,
                          "json": [
                            123,
                            {
                              "bar": 789,
                              "foo": 456,
                            },
                            "baz",
                          ],
                          "owner": "101",
                          "parent": "2",
                          "title": "not matched",
                        },
                        "tableName": "issues",
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
            "query-hash2": 1,
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
            "query-hash2": 1,
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
      ]
    `);
  });

  test('initial hydration, schemaVersion unsupported', async () => {
    const client = connect({...SYNC_CONTEXT, schemaVersion: 1}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);
    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 1 is not in range of supported schema versions [2, 3].',
    });
  });

  test('initial hydration, schemaVersion unsupported with bad query', async () => {
    // Simulate a connection when the replica is already ready.
    stateChanges.push({state: 'version-ready'});
    await sleep(5);

    const client = connect({...SYNC_CONTEXT, schemaVersion: 1}, [
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
    const dequeuePromise = nextPoke(client);
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 1 is not in range of supported schema versions [2, 3].',
    });
  });

  test('process successful advancement', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "ast": {
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "issues",
                  },
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": "00:01",
          "cookie": "00:02",
          "pokeID": "00:02",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);

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
                "id": {
                  "id": "1",
                },
                "op": "put",
                "rest": {
                  "big": 9007199254740991,
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
                "tableName": "issues",
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

    replicator.processTransaction('124', messages.truncate('issues'));

    stateChanges.push({state: 'version-ready'});

    // One canceled poke.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "123",
            "pokeID": "123",
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
            "pokeID": "123",
          },
        ],
      ]
    `);

    // Then a poke that deletes issues rows in the CVR.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "123",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "5",
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
            "pokeID": "123",
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
          "patchVersion": "01",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "00",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('process advancement that results in client having an unsupported schemaVersion', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    // Note: client2 is behind, so it does not get an immediate update on connect.
    //       It has to wait until a hydrate to catchup. However, client1 will get
    //       updated about client2.
    const client2 = connect(
      {...SYNC_CONTEXT, clientID: 'bar', schemaVersion: 3},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "cookie": "00:02",
            "pokeID": "00:02",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
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

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client1))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": "00:02",
          "cookie": "00:03",
          "pokeID": "00:03",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);
    expect((await nextPoke(client2))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": null,
          "cookie": "00:03",
          "pokeID": "00:03",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);

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
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 2 is not in range of supported schema versions [3, 3].',
    });

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
                  "id": {
                    "id": "1",
                  },
                  "op": "put",
                  "rest": {
                    "big": 9007199254740991,
                    "json": null,
                    "owner": "100.0",
                    "parent": null,
                    "title": "new title",
                  },
                  "tableName": "issues",
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
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: '00:01',
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
                      "id": {
                        "id": "1",
                      },
                      "op": "put",
                      "rest": {
                        "big": 9007199254740991,
                        "json": null,
                        "newColumn": null,
                        "owner": "100",
                        "parent": null,
                        "title": "parent issue foo",
                      },
                      "tableName": "issues",
                    },
                    {
                      "id": {
                        "id": "2",
                      },
                      "op": "put",
                      "rest": {
                        "big": -9007199254740991,
                        "json": null,
                        "newColumn": null,
                        "owner": "101",
                        "parent": null,
                        "title": "parent issue bar",
                      },
                      "tableName": "issues",
                    },
                    {
                      "id": {
                        "id": "3",
                      },
                      "op": "put",
                      "rest": {
                        "big": 123,
                        "json": null,
                        "newColumn": null,
                        "owner": "102",
                        "parent": "1",
                        "title": "foo",
                      },
                      "tableName": "issues",
                    },
                    {
                      "id": {
                        "id": "4",
                      },
                      "op": "put",
                      "rest": {
                        "big": 100,
                        "json": null,
                        "newColumn": null,
                        "owner": "101",
                        "parent": "2",
                        "title": "bar",
                      },
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

  test('catch up client', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "cookie": "00:01",
            "pokeID": "00:01",
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: '00:01',
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
          id: {id: '1'},
          rest: {
            big: 9007199254740991,
            owner: '100.0',
            parent: null,
            title: 'new title',
            json: null,
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
    const client2 = connect(
      {
        clientID: 'bar',
        wsID: '9382',
        baseCookie: preAdvancement.cookie,
        protocolVersion: PROTOCOL_VERSION,
        schemaVersion: 2,
        tokenData: undefined,
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "put",
                "rest": {
                  "big": 9007199254740991,
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
                "tableName": "issues",
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
                                    "where": {
                                      "left": {
                                        "name": "id",
                                        "type": "column",
                                      },
                                      "op": "IN",
                                      "right": {
                                        "type": "literal",
                                        "value": [
                                          "1",
                                          "2",
                                          "3",
                                          "4",
                                        ],
                                      },
                                      "type": "simple",
                                    },
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

  test('catch up new client before advancement', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client1);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: '00:01',
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

    // Connect a second client right as the advancement is about to be processed.
    await sleep(0.5);
    const client2 = connect({...SYNC_CONTEXT, clientID: 'bar'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Response should catch client2 from scratch.
    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
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
                "clientID": "foo",
                "op": "put",
              },
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
                  },
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
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
                  "where": {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "IN",
                    "right": {
                      "type": "literal",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                    "type": "simple",
                  },
                },
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01:01",
            "rowsPatch": [
              {
                "id": {
                  "id": "3",
                },
                "op": "put",
                "rest": {
                  "big": 123,
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "put",
                "rest": {
                  "big": 100,
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "1",
                },
                "op": "put",
                "rest": {
                  "big": 9007199254740991,
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
                "tableName": "issues",
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
  });

  test('waits for replica to catch up', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '00'.
    const cvrStore = new CVRStore(lc, cvrDB, TASK_ID, serviceID, ON_FAILURE);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      REPLICA_VERSION,
    ).flush(lc, Date.now());

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
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
                    "where": {
                      "left": {
                        "name": "id",
                        "type": "column",
                      },
                      "op": "IN",
                      "right": {
                        "type": "literal",
                        "value": [
                          "1",
                          "2",
                          "3",
                          "4",
                        ],
                      },
                      "type": "simple",
                    },
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
                  "where": {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "IN",
                    "right": {
                      "type": "literal",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                    "type": "simple",
                  },
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
                "id": {
                  "id": "4",
                },
                "op": "put",
                "rest": {
                  "big": 100,
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "caught up",
                },
                "tableName": "issues",
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
    const cvrStore = new CVRStore(lc, cvrDB, TASK_ID, serviceID, ON_FAILURE);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      '1' + REPLICA_VERSION, // Different replica version.
    ).flush(lc, Date.now());

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
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
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Replica Version mismatch: CVR=101, DB=01',
    } satisfies ErrorBody);
  });

  test('sends client not found if CVR is not found', async () => {
    // Connect the client at a non-empty base cookie.
    const client = connect({...SYNC_CONTEXT, baseCookie: '00:02'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Client not found',
    } satisfies ErrorBody);
  });

  test('sends invalid base cookie if client is ahead of CVR', async () => {
    const cvrStore = new CVRStore(lc, cvrDB, TASK_ID, serviceID, ON_FAILURE);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      REPLICA_VERSION,
    ).flush(lc, Date.now());

    // Connect the client with a base cookie from the future.
    const client = connect({...SYNC_CONTEXT, baseCookie: '08'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: 'CVR is at version 07',
    } satisfies ErrorBody);
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
    const client = connect(SYNC_CONTEXT, [
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

  test('retracting an exists relationship', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY_WITH_RELATED},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY_WITH_EXISTS},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    await nextPoke(client);

    replicator.processTransaction(
      '123',
      messages.delete('issueLabels', {
        issueID: '1',
        labelID: '1',
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
                            "id": {
                              "issueID": "1",
                              "labelID": "1",
                            },
                            "op": "del",
                            "tableName": "issueLabels",
                          },
                          {
                            "id": {
                              "id": "1",
                            },
                            "op": "del",
                            "tableName": "labels",
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
});

describe('permissions', () => {
  let stateChanges: Subscription<ReplicaState>;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: QueriesPatch,
  ) => Queue<Downstream>;
  let nextPoke: (client: Queue<Downstream>) => Promise<Downstream[]>;
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;

  const SYNC_CONTEXT = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: {
      raw: '',
      decoded: {sub: 'foo', role: 'user', iat: 0},
    },
  };

  beforeEach(async () => {
    ({
      stateChanges,
      connect,
      nextPoke,
      vs,
      viewSyncerDone,
      cvrDB,
      replicaDbFile,
    } = await setup(permissions));
  });

  afterEach(async () => {
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  test('client with no role followed by client with admin role', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // the user is not logged in as admin and so cannot see any issues.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
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
                  "where": {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "IN",
                    "right": {
                      "type": "literal",
                      "value": [
                        "1",
                        "2",
                        "3",
                        "4",
                      ],
                    },
                    "type": "simple",
                  },
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

    // New client connects with same everything (client group, user id) but brings a new role.
    // This should transform their existing queries to return the data they can now see.
    const client2 = connect(
      {
        ...SYNC_CONTEXT,
        clientID: 'bar',
        tokenData: {
          raw: '',
          decoded: {sub: 'foo', role: 'admin', iat: 1},
        },
      },
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
                      [
                        [
                          "pokeStart",
                          {
                            "baseCookie": null,
                            "cookie": "00:04",
                            "pokeID": "00:04",
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
                                    "where": {
                                      "left": {
                                        "name": "id",
                                        "type": "column",
                                      },
                                      "op": "IN",
                                      "right": {
                                        "type": "literal",
                                        "value": [
                                          "1",
                                          "2",
                                          "3",
                                          "4",
                                        ],
                                      },
                                      "type": "simple",
                                    },
                                  },
                                  "hash": "query-hash1",
                                  "op": "put",
                                },
                              ],
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
                                    "where": {
                                      "left": {
                                        "name": "id",
                                        "type": "column",
                                      },
                                      "op": "IN",
                                      "right": {
                                        "type": "literal",
                                        "value": [
                                          "1",
                                          "2",
                                          "3",
                                          "4",
                                        ],
                                      },
                                      "type": "simple",
                                    },
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
                                  "where": {
                                    "left": {
                                      "name": "id",
                                      "type": "column",
                                    },
                                    "op": "IN",
                                    "right": {
                                      "type": "literal",
                                      "value": [
                                        "1",
                                        "2",
                                        "3",
                                        "4",
                                      ],
                                    },
                                    "type": "simple",
                                  },
                                },
                                "hash": "query-hash1",
                                "op": "put",
                              },
                            ],
                            "lastMutationIDChanges": {
                              "foo": 42,
                            },
                            "pokeID": "00:04",
                            "rowsPatch": [
                              {
                                "id": {
                                  "id": "1",
                                },
                                "op": "put",
                                "rest": {
                                  "big": 9007199254740991,
                                  "json": null,
                                  "owner": "100",
                                  "parent": null,
                                  "title": "parent issue foo",
                                },
                                "tableName": "issues",
                              },
                              {
                                "id": {
                                  "id": "2",
                                },
                                "op": "put",
                                "rest": {
                                  "big": -9007199254740991,
                                  "json": null,
                                  "owner": "101",
                                  "parent": null,
                                  "title": "parent issue bar",
                                },
                                "tableName": "issues",
                              },
                              {
                                "id": {
                                  "id": "3",
                                },
                                "op": "put",
                                "rest": {
                                  "big": 123,
                                  "json": null,
                                  "owner": "102",
                                  "parent": "1",
                                  "title": "foo",
                                },
                                "tableName": "issues",
                              },
                              {
                                "id": {
                                  "id": "4",
                                },
                                "op": "put",
                                "rest": {
                                  "big": 100,
                                  "json": null,
                                  "owner": "101",
                                  "parent": "2",
                                  "title": "bar",
                                },
                                "tableName": "issues",
                              },
                            ],
                          },
                        ],
                        [
                          "pokeEnd",
                          {
                            "pokeID": "00:04",
                          },
                        ],
                      ]
                    `);
  });

  test('permissions via subquery', async () => {
    const client = await connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: COMMENTS_QUERY},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // Should not receive any comments b/c they cannot see any issues
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
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
            "gotQueriesPatch": [
              {
                "ast": {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "comments",
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
  });

  test('query for comments does not return issue rows as those are gotten by the permission system', async () => {
    const client = await connect(
      {
        ...SYNC_CONTEXT,
        tokenData: {
          raw: '',
          decoded: {sub: 'foo', role: 'admin', iat: 1},
        },
      },
      [{op: 'put', hash: 'query-hash2', ast: COMMENTS_QUERY}],
    );
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    // Should receive comments since they can see issues as the admin
    // but should not receive those issues since the query for them was added by
    // the auth system.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
                  [
                    [
                      "pokeStart",
                      {
                        "baseCookie": "00:01",
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
                        "gotQueriesPatch": [
                          {
                            "ast": {
                              "orderBy": [
                                [
                                  "id",
                                  "asc",
                                ],
                              ],
                              "table": "comments",
                            },
                            "hash": "query-hash2",
                            "op": "put",
                          },
                        ],
                        "lastMutationIDChanges": {
                          "foo": 42,
                        },
                        "pokeID": "00:02",
                        "rowsPatch": [
                          {
                            "id": {
                              "id": "1",
                            },
                            "op": "put",
                            "rest": {
                              "issueID": "1",
                              "text": "comment 1",
                            },
                            "tableName": "comments",
                          },
                          {
                            "id": {
                              "id": "2",
                            },
                            "op": "put",
                            "rest": {
                              "issueID": "1",
                              "text": "comment 2",
                            },
                            "tableName": "comments",
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
  });
});

describe('pickToken', () => {
  test('previous token is undefined', () => {
    expect(pickToken(undefined, {sub: 'foo', iat: 1})).toEqual({
      sub: 'foo',
      iat: 1,
    });
  });

  test('previous token exists, new token is undefined', () => {
    expect(() => pickToken({sub: 'foo', iat: 1}, undefined)).toThrowError(
      ErrorForClient,
    );
  });

  test('previous token has a subject, new token does not', () => {
    expect(() => pickToken({sub: 'foo'}, {})).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has a different subject', () => {
    expect(() =>
      pickToken({sub: 'foo', iat: 1}, {sub: 'bar', iat: 1}),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has the same subject', () => {
    expect(pickToken({sub: 'foo', iat: 1}, {sub: 'foo', iat: 2})).toEqual({
      sub: 'foo',
      iat: 2,
    });

    expect(pickToken({sub: 'foo', iat: 2}, {sub: 'foo', iat: 1})).toEqual({
      sub: 'foo',
      iat: 2,
    });
  });

  test('previous token has no subject, new token has a subject', () => {
    expect(() => pickToken({sub: 'foo', iat: 123}, {iat: 123})).toThrowError(
      ErrorForClient,
    );
  });

  test('previous token has no subject, new token has no subject', () => {
    expect(pickToken({iat: 1}, {iat: 2})).toEqual({
      iat: 2,
    });
    expect(pickToken({iat: 2}, {iat: 1})).toEqual({
      iat: 2,
    });
  });

  test('previous token has an issued at time, new token does not', () => {
    expect(() => pickToken({sub: 'foo', iat: 1}, {sub: 'foo'})).toThrowError(
      ErrorForClient,
    );
  });

  test('previous token has an issued at time, new token has a greater issued at time', () => {
    expect(pickToken({sub: 'foo', iat: 1}, {sub: 'foo', iat: 2})).toEqual({
      sub: 'foo',
      iat: 2,
    });
  });

  test('previous token has an issued at time, new token has a lesser issued at time', () => {
    expect(pickToken({sub: 'foo', iat: 2}, {sub: 'foo', iat: 1})).toEqual({
      sub: 'foo',
      iat: 2,
    });
  });

  test('previous token has an issued at time, new token has the same issued at time', () => {
    expect(pickToken({sub: 'foo', iat: 2}, {sub: 'foo', iat: 2})).toEqual({
      sub: 'foo',
      iat: 2,
    });
  });

  test('previous token has no issued at time, new token has an issued at time', () => {
    expect(pickToken({sub: 'foo'}, {sub: 'foo', iat: 2})).toEqual({
      sub: 'foo',
      iat: 2,
    });
  });

  test('previous token has no issued at time, new token has no issued at time', () => {
    expect(pickToken({sub: 'foo'}, {sub: 'foo'})).toEqual({
      sub: 'foo',
    });
  });
});
