import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {h128} from '../../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.ts';
import type {ErrorBody} from '../../../../zero-protocol/src/error.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {QueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {PermissionsConfig} from '../../../../zero-schema/src/compiled-permissions.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../../zero-schema/src/permissions.ts';
import type {ExpressionBuilder} from '../../../../zql/src/query/expression.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {LogConfig} from '../../config/zero-config.ts';
import {StatementRunner} from '../../db/statements.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {initChangeLog} from '../replicator/schema/change-log.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  type FakeReplicator,
  ReplicationMessages,
} from '../replicator/test-utils.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRQueryDrivenUpdater} from './cvr.ts';
import {
  type ClientGroupStorage,
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from './database-storage.ts';
import {DrainCoordinator} from './drain-coordinator.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {initViewSyncerSchema} from './schema/init.ts';
import {Snapshotter} from './snapshotter.ts';
import {pickToken, type SyncContext, ViewSyncerService} from './view-syncer.ts';

const SHARD_ID = 'abc';
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

const EXPECTED_LMIDS_AST: AST = {
  schema: '',
  table: 'zero_abc.clients',
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

const issues = table('issues')
  .columns({
    id: string(),
    title: string(),
    owner: string(),
    parent: string(),
    big: number(),
    json: json(),
  })
  .primaryKey('id');
const comments = table('comments')
  .columns({
    id: string(),
    issueID: string(),
    text: string(),
  })
  .primaryKey('id');
const issueLabels = table('issueLabels')
  .columns({
    issueID: string(),
    labelID: string(),
  })
  .primaryKey('issueID', 'labelID');
const labels = table('labels')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const schema = createSchema(1, {
  tables: [issues, comments, issueLabels, labels],
  relationships: [
    relationships(comments, connect => ({
      issue: connect.many({
        sourceField: ['issueID'],
        destField: ['id'],
        destSchema: issues,
      }),
    })),
  ],
});
type Schema = typeof schema;

type AuthData = {
  sub: string;
  role: 'user' | 'admin';
  iat: number;
};
const canSeeIssue = (
  authData: AuthData,
  eb: ExpressionBuilder<Schema, 'issues'>,
) => eb.cmpLit(authData.role, '=', 'admin');
const permissions = await definePermissions<AuthData, typeof schema>(
  schema,
  () => ({
    issues: {
      row: {
        select: [canSeeIssue],
      },
    },
    comments: {
      row: {
        select: [
          (authData, eb: ExpressionBuilder<Schema, 'comments'>) =>
            eb.exists('issue', iq =>
              iq.where(({eb}) => canSeeIssue(authData, eb)),
            ),
        ],
      },
    },
  }),
);

const permissionsAll = await definePermissions<AuthData, typeof schema>(
  schema,
  () => ({
    issues: ANYONE_CAN_DO_ANYTHING,
    comments: ANYONE_CAN_DO_ANYTHING,
    issueLabels: ANYONE_CAN_DO_ANYTHING,
    labels: ANYONE_CAN_DO_ANYTHING,
  }),
);

async function setup(permissions: PermissionsConfig | undefined) {
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
  CREATE TABLE "zero_abc.clients" (
    "clientGroupID"  TEXT,
    "clientID"       TEXT,
    "lastMutationID" INTEGER,
    "userID"         TEXT,
    _0_version       TEXT NOT NULL,
    PRIMARY KEY ("clientGroupID", "clientID")
  );
  CREATE TABLE "zero.schemaVersions" (
    "lock"                INT PRIMARY KEY,
    "minSupportedVersion" INT,
    "maxSupportedVersion" INT,
    _0_version            TEXT NOT NULL
  );
  CREATE TABLE "zero.permissions" (
    "lock"        INT PRIMARY KEY,
    "permissions" JSON,
    "hash"        TEXT,
    _0_version    TEXT NOT NULL
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

  INSERT INTO "zero_abc.clients" ("clientGroupID", "clientID", "lastMutationID", _0_version)
    VALUES ('9876', 'foo', 42, '01');
  INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
    VALUES (1, 2, 3, '01'); 
  INSERT INTO "zero.permissions" ("lock", "permissions", "hash", _0_version)
    VALUES (1, NULL, NULL, '01');

  INSERT INTO users (id, name, _0_version) VALUES ('100', 'Alice', '01');
  INSERT INTO users (id, name, _0_version) VALUES ('101', 'Bob', '01');
  INSERT INTO users (id, name, _0_version) VALUES ('102', 'Candice', '01');

  INSERT INTO issues (id, title, owner, big, _0_version) VALUES ('1', 'parent issue foo', 100, 9007199254740991, '01');
  INSERT INTO issues (id, title, owner, big, _0_version) VALUES ('2', 'parent issue bar', 101, -9007199254740991, '01');
  INSERT INTO issues (id, title, owner, parent, big, _0_version) VALUES ('3', 'foo', 102, 1, 123, '01');
  INSERT INTO issues (id, title, owner, parent, big, _0_version) VALUES ('4', 'bar', 101, 2, 100, '01');
  -- The last row should not match the ISSUES_TITLE_QUERY: "WHERE id IN (1, 2, 3, 4)"
  INSERT INTO issues (id, title, owner, parent, big, json, _0_version) VALUES 
    ('5', 'not matched', 101, 2, 100, '[123,{"foo":456,"bar":789},"baz"]', '01');

  INSERT INTO "issueLabels" (issueID, labelID, _0_version) VALUES ('1', '1', '01');
  INSERT INTO "labels" (id, name, _0_version) VALUES ('1', 'bug', '01');

  INSERT INTO "comments" (id, issueID, text, _0_version) VALUES ('1', '1', 'comment 1', '01');
  INSERT INTO "comments" (id, issueID, text, _0_version) VALUES ('2', '1', 'comment 2', '01');
  `);

  const cvrDB = await testDBs.create('view_syncer_service_test');
  await initViewSyncerSchema(lc, cvrDB, SHARD_ID);

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
      logConfig,
      new Snapshotter(lc, replicaDbFile.path),
      operatorStorage,
      'view-syncer.pg-test.ts',
    ),
    stateChanges,
    drainCoordinator,
  );
  if (permissions) {
    const json = JSON.stringify(permissions);
    replica
      .prepare(`UPDATE "zero.permissions" SET permissions = ?, hash = ?`)
      .run(json, h128(json).toString(16));
  }
  const viewSyncerDone = vs.run();

  function connectWithQueueAndSource(
    ctx: SyncContext,
    desiredQueriesPatch: QueriesPatch,
  ): {queue: Queue<Downstream>; source: Source<Downstream>} {
    const source = vs.initConnection(ctx, [
      'initConnection',
      {desiredQueriesPatch},
    ]);
    const queue = new Queue<Downstream>();

    void (async function () {
      try {
        for await (const msg of source) {
          queue.enqueue(msg);
        }
      } catch (e) {
        queue.enqueueRejection(e);
      }
    })();

    return {queue, source};
  }

  function connect(ctx: SyncContext, desiredQueriesPatch: QueriesPatch) {
    return connectWithQueueAndSource(ctx, desiredQueriesPatch).queue;
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
    connectWithQueueAndSource,
    nextPoke,
    expectNoPokes,
  };
}

const messages = new ReplicationMessages({
  issues: 'id',
  users: 'id',
  issueLabels: ['issueID', 'labelID'],
});
const zeroMessages = new ReplicationMessages(
  {
    schemaVersions: 'lock',
    permissions: 'lock',
  },
  'zero',
);

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
  let connectWithQueueAndSource: (
    ctx: SyncContext,
    desiredQueriesPatch: QueriesPatch,
  ) => {
    queue: Queue<Downstream>;
    source: Source<Downstream>;
  };
  let nextPoke: (client: Queue<Downstream>) => Promise<Downstream[]>;
  let expectNoPokes: (client: Queue<Downstream>) => Promise<void>;

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: undefined,
  };

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
      connectWithQueueAndSource,
      nextPoke,
      expectNoPokes,
    } = await setup(permissionsAll));
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

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD_ID,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'query-hash1': {
          ast: ISSUES_QUERY,
          desiredBy: {foo: {version: {stateVersion: '00', minorVersion: 1}}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', minorVersion: 1},
    });
  });

  test('responds to changeDesiredQueries patch', async () => {
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

    const inactivatedAt = Date.now();
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

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD_ID,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2', 'query-hash1'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: EXPECTED_LMIDS_AST,
          internal: true,
          id: 'lmids',
        },
        'query-hash1': {
          ast: ISSUES_QUERY,
          desiredBy: {
            foo: {
              inactivatedAt: expect.closeTo(inactivatedAt, 10),
              ttl: undefined,
              version: {minorVersion: 2, stateVersion: '00'},
            },
          },
          id: 'query-hash1',
        },
        'query-hash2': {
          ast: USERS_QUERY,
          desiredBy: {foo: {version: {stateVersion: '00', minorVersion: 2}}},
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
            "cookie": "00:01",
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
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr_abc.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "zero_abc.clients",
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
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
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
            "id": "3",
          },
          "rowVersion": "01",
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
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('delete client', async () => {
    const {queue: client1} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    const {queue: client2, source: connectSource2} = connectWithQueueAndSource(
      {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'},
      [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY}],
    );

    await nextPoke(client1);
    await nextPoke(client2);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client1);
    await nextPoke(client1);

    await nextPoke(client2);
    await nextPoke(client2);

    expect(await cvrDB`SELECT * from cvr_abc.clients`).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientGroupID": "9876",
          "clientID": "foo",
          "deleted": false,
          "patchVersion": "00:01",
        },
        {
          "clientGroupID": "9876",
          "clientID": "bar",
          "deleted": false,
          "patchVersion": "00:02",
        },
      ]
    `,
    );

    expect(await cvrDB`SELECT * from cvr_abc.desires`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "clientID": "foo",
          "deleted": false,
          "expiresAt": null,
          "inactivatedAt": null,
          "patchVersion": "00:01",
          "queryHash": "query-hash1",
          "ttl": null,
        },
        {
          "clientGroupID": "9876",
          "clientID": "bar",
          "deleted": false,
          "expiresAt": null,
          "inactivatedAt": null,
          "patchVersion": "00:02",
          "queryHash": "query-hash2",
          "ttl": null,
        },
      ]
    `);

    connectSource2.cancel();

    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

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
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash2",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01:01",
            "pokeID": "01:01",
          },
        ],
      ]
    `);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01:01",
            "cookie": "01:02",
            "pokeID": "01:02",
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
                "hash": "query-hash2",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01:02",
            "pokeID": "01:02",
          },
        ],
      ]
    `);

    expect(await client1.dequeue()).toMatchInlineSnapshot(`
      [
        "deleteClients",
        {
          "clientIDs": [
            "bar",
            "no-such-client",
          ],
        },
      ]
    `);

    expect(await cvrDB`SELECT * from cvr_abc.clients`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "clientID": "foo",
          "deleted": false,
          "patchVersion": "00:01",
        },
      ]
    `);
    expect(await cvrDB`SELECT * from cvr_abc.desires`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "clientID": "foo",
          "deleted": false,
          "expiresAt": null,
          "inactivatedAt": null,
          "patchVersion": "00:01",
          "queryHash": "query-hash1",
          "ttl": null,
        },
      ]
    `);
  });

  test('initial hydration, rows in multiple queries', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      // Test multiple queries that normalize to the same hash.
      {op: 'put', hash: 'query-hash1.1', ast: ISSUES_QUERY},
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
                  "hash": "query-hash1.1",
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
            "cookie": "00:01",
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
                "hash": "query-hash1.1",
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
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "5",
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
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr_abc.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "zero_abc.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
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
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
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
            "cookie": "00:01",
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

  test('process advancements', async () => {
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
            "cookie": "00:01",
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
          "cookie": "01",
          "pokeID": "01",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);

    // Perform an unrelated transaction that does not affect any queries.
    // This should not result in a poke.
    replicator.processTransaction(
      '101',
      messages.insert('users', {
        id: '103',
        name: 'Dude',
      }),
    );
    stateChanges.push({state: 'version-ready'});

    // Then, a relevant change should bump the client from '01' directly to '123'.
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
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr_abc.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "zero_abc.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
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
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "123",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    replicator.processTransaction('124', messages.truncate('issues'));

    stateChanges.push({state: 'version-ready'});

    // Then a poke that deletes issues rows in the CVR.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "123",
            "cookie": "124",
            "pokeID": "124",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "124",
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
            "cookie": "124",
            "pokeID": "124",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from cvr_abc.rows`).toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "zero_abc.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "123",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
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
            "cookie": "00:01",
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
            "cookie": "00:02",
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
          "cookie": "01",
          "pokeID": "01",
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
          "cookie": "01",
          "pokeID": "01",
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
            "baseCookie": "01",
            "cookie": "123",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 3,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
            "cookie": "123",
            "pokeID": "123",
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
            "cookie": "00:01",
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
        cookie: '01',
        pokeID: '01',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction(
      '07',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    stateChanges.push({state: 'version-ready'});

    // The "newColumn" should be arrive in the nextPoke.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "07",
            "pokeID": "07",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "07",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
            "cookie": "07",
            "pokeID": "07",
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
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[2][1] as PokeEndBody;
    expect(preAdvancement).toEqual({
      cookie: '01',
      pokeID: '01',
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
            json: null,
          },
        },
        {
          id: {id: '2'},
          tableName: 'issues',
          op: 'del',
        },
      ],
      pokeID: '123',
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
      pokeID: '123:01',
    });
    expect(response2).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "123:01",
            "pokeID": "123:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
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
            "pokeID": "123:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
            "cookie": "123:01",
            "pokeID": "123:01",
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
            "baseCookie": "123",
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
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
            "pokeID": "123:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
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
      cookie: '01',
      pokeID: '01',
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
            "cookie": "123:01",
            "pokeID": "123:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
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
            "pokeID": "123:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
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
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);
  });

  test('waits for replica to catch up', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '01'.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD_ID,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      REPLICA_VERSION,
    ).flush(lc, true, Date.now(), Date.now());

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
    updateReplicationWatermark(db, '06');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica
      .prepare(`UPDATE issues SET title = 'caught up' where id = '4'`)
      .run();
    updateReplicationWatermark(db, '07'); // Caught up with stateVersion=07, watermark=09.
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
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
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
            "cookie": "07:02",
            "pokeID": "07:02",
          },
        ],
      ]
    `);
  });

  test('sends reset for CVR from older replica version up', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD_ID,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      '1' + REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, true, Date.now(), Date.now());

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
      message: 'Cannot sync from older replica: CVR=101, DB=01',
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
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD_ID,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, Date.now()),
      '07',
      REPLICA_VERSION,
    ).flush(lc, true, Date.now(), Date.now());

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
            "cookie": "123",
            "pokeID": "123",
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
  let replicator: FakeReplicator;

  const SYNC_CONTEXT: SyncContext = {
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
      replicator,
    } = await setup(permissions));
  });

  afterEach(async () => {
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  test('client with user role followed by client with admin role', async () => {
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
            "pokeID": "01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
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
            "cookie": "01:02",
            "pokeID": "01:02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
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
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
            "cookie": "01:02",
            "pokeID": "01:02",
          },
        ],
      ]
    `);
  });

  test('upstream permissions change', async () => {
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
            "pokeID": "01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    // Open permissions
    const relaxed: PermissionsConfig = {
      tables: {
        issues: {
          row: {
            select: [
              [
                'allow',
                {
                  type: 'simple',
                  left: {type: 'literal', value: true},
                  op: '=',
                  right: {type: 'literal', value: true},
                },
              ],
            ],
          },
        },
        comments: {},
      },
    };
    replicator.processTransaction(
      '05',
      zeroMessages.update('permissions', {
        lock: 1,
        permissions: relaxed,
        hash: h128(JSON.stringify(relaxed)).toString(16),
      }),
    );
    stateChanges.push({state: 'version-ready'});

    // Newly visible rows are poked.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "cookie": "05",
            "pokeID": "05",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "05",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
                  "json": null,
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
            "cookie": "05",
            "pokeID": "05",
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
            "pokeID": "01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
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
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "comment 1",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "2",
                  "issueID": "1",
                  "text": "comment 2",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
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
