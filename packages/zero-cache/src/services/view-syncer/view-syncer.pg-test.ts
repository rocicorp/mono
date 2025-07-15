import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type Mock,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {h128} from '../../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {
  TransformResponseBody,
  TransformResponseMessage,
} from '../../../../zero-protocol/src/custom-queries.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import type {ErrorBody} from '../../../../zero-protocol/src/error.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {
  clientSchemaFrom,
  createSchema,
} from '../../../../zero-schema/src/builder/schema-builder.ts';
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
import type {ZeroConfig} from '../../config/zero-config.ts';
import {StatementRunner} from '../../db/statements.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {DataChange} from '../change-source/protocol/current/data.ts';
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
import {ttlClockFromNumber} from './ttl-clock.ts';
import {pickToken, type SyncContext, ViewSyncerService} from './view-syncer.ts';

const APP_ID = 'this_app';
const SHARD_NUM = 2;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

const EXPECTED_LMIDS_AST: AST = {
  schema: '',
  table: 'this_app_2.clients',
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

const queryConfig: ZeroConfig['query'] = {
  url: 'http://my-pull-endpoint.dev/api/zero/pull',
  forwardCookies: true,
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

const ISSUES_QUERY_WITH_EXISTS_AND_RELATED: AST = {
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
        table: 'comments',
        alias: 'exists_comments',
        orderBy: [
          ['issueID', 'asc'],
          ['id', 'asc'],
        ],
        where: {
          type: 'simple',
          left: {
            type: 'column',
            name: 'text',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'foo',
          },
        },
      },
    },
  },
  related: [
    {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [
          ['issueID', 'asc'],
          ['id', 'asc'],
        ],
      },
    },
  ],
};

const ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'NOT EXISTS',
    related: {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'comments',
        alias: 'exists_comments',
        orderBy: [
          ['issueID', 'asc'],
          ['id', 'asc'],
        ],
        where: {
          type: 'simple',
          left: {
            type: 'column',
            name: 'text',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'bar',
          },
        },
      },
    },
  },
  related: [
    {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [
          ['issueID', 'asc'],
          ['id', 'asc'],
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
const users = table('users')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [issues, comments, issueLabels, labels, users],
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

const {clientSchema: defaultClientSchema} = clientSchemaFrom(schema);

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
    users: ANYONE_CAN_DO_ANYTHING,
  }),
);

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

async function nextPokeParts(
  client: Queue<Downstream>,
): Promise<PokePartBody[]> {
  const pokes = await nextPoke(client);
  return pokes
    .filter((msg: Downstream) => msg[0] === 'pokePart')
    .map(([, body]) => body);
}

async function expectNoPokes(client: Queue<Downstream>) {
  // Use the dequeue() API that cancels the dequeue() request after a timeout.
  const timedOut = 'nothing' as unknown as Downstream;
  expect(await client.dequeue(timedOut, 10)).toBe(timedOut);
}

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
  CREATE TABLE "this_app_2.clients" (
    "clientGroupID"  TEXT,
    "clientID"       TEXT,
    "lastMutationID" INTEGER,
    "userID"         TEXT,
    _0_version       TEXT NOT NULL,
    PRIMARY KEY ("clientGroupID", "clientID")
  );
  CREATE TABLE "this_app.schemaVersions" (
    "lock"                INT PRIMARY KEY,
    "minSupportedVersion" INT,
    "maxSupportedVersion" INT,
    _0_version            TEXT NOT NULL
  );
  CREATE TABLE "this_app.permissions" (
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

  INSERT INTO "this_app_2.clients" ("clientGroupID", "clientID", "lastMutationID", _0_version)
    VALUES ('9876', 'foo', 42, '01');
  INSERT INTO "this_app.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
    VALUES (1, 2, 3, '01'); 
  INSERT INTO "this_app.permissions" ("lock", "permissions", "hash", _0_version)
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
  INSERT INTO "comments" (id, issueID, text, _0_version) VALUES ('2', '1', 'bar', '01');
  `);

  const cvrDB = await testDBs.create('view_syncer_service_test');
  await initViewSyncerSchema(lc, cvrDB, SHARD);

  const setTimeoutFn = vi.fn();

  const replicator = fakeReplicator(lc, replica);
  const stateChanges: Subscription<ReplicaState> = Subscription.create();
  const drainCoordinator = new DrainCoordinator();
  const operatorStorage = new DatabaseStorage(
    storageDB,
  ).createClientGroupStorage(serviceID);
  const vs = new ViewSyncerService(
    queryConfig,
    lc,
    SHARD,
    TASK_ID,
    serviceID,
    cvrDB,
    new PipelineDriver(
      lc.withContext('component', 'pipeline-driver'),
      testLogConfig,
      new Snapshotter(lc, replicaDbFile.path, SHARD),
      SHARD,
      operatorStorage,
      'view-syncer.pg-test.ts',
    ),
    stateChanges,
    drainCoordinator,
    100,
    undefined,
    undefined,
    setTimeoutFn,
  );
  if (permissions) {
    const json = JSON.stringify(permissions);
    replica
      .prepare(`UPDATE "this_app.permissions" SET permissions = ?, hash = ?`)
      .run(json, h128(json).toString(16));
  }
  const viewSyncerDone = vs.run();

  function connectWithQueueAndSource(
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema: ClientSchema = defaultClientSchema,
    activeClients?: string[],
  ): {queue: Queue<Downstream>; source: Source<Downstream>} {
    const source = vs.initConnection(ctx, [
      'initConnection',
      {desiredQueriesPatch, clientSchema, activeClients},
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

  function connect(
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
  ) {
    return connectWithQueueAndSource(ctx, desiredQueriesPatch, clientSchema)
      .queue;
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
    setTimeoutFn,
  };
}

const messages = new ReplicationMessages({
  issues: 'id',
  users: 'id',
  issueLabels: ['issueID', 'labelID'],
  comments: 'id',
});
const appMessages = new ReplicationMessages(
  {
    schemaVersions: 'lock',
    permissions: 'lock',
  },
  'this_app',
);

const app2Messages = new ReplicationMessages(
  {
    clients: ['clientGroupID', 'clientID'],
  },
  'this_app_2',
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
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
  ) => Queue<Downstream>;
  let connectWithQueueAndSource: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
    activeClients?: string[],
  ) => {
    queue: Queue<Downstream>;
    source: Source<Downstream>;
  };
  let setTimeoutFn: Mock<typeof setTimeout>;

  function callNextSetTimeout(delta: number) {
    // Sanity check that the system time is the mocked time.
    expect(vi.getRealSystemTime()).not.toBe(vi.getMockedSystemTime());
    vi.setSystemTime(Date.now() + delta);
    const fn = setTimeoutFn.mock.lastCall![0];
    fn();
  }

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: undefined,
    httpCookie: undefined,
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
      setTimeoutFn,
    } = await setup(permissionsAll));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  async function getCVROwner() {
    const [{owner}] = await cvrDB<{owner: string}[]>`
    SELECT owner FROM ${cvrDB(cvrSchema(SHARD))}.instances
       WHERE "clientGroupID" = ${serviceID};
  `;
    return owner;
  }

  test('adds desired queries from initConnectionMessage', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client);

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
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
          type: 'client',
          clientState: {foo: {version: {stateVersion: '00', minorVersion: 1}}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', minorVersion: 1},
    });
  });

  test('responds to changeDesiredQueries patch', async () => {
    const now = Date.UTC(2025, 1, 20);
    vi.setSystemTime(now);
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
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: EXPECTED_LMIDS_AST,
          type: 'internal',
          id: 'lmids',
        },
        'query-hash1': {
          ast: ISSUES_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt,
              ttl: -1,
              version: {minorVersion: 2, stateVersion: '00'},
            },
          },
          id: 'query-hash1',
        },
        'query-hash2': {
          ast: USERS_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt: undefined,
              ttl: -1,
              version: {stateVersion: '00', minorVersion: 2},
            },
          },
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
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
          "table": "this_app_2.clients",
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

  describe('custom queries', () => {
    const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
    beforeEach(() => {
      vi.stubGlobal('fetch', mockFetch);
    });

    afterEach(() => {
      mockFetch.mockClear();
      vi.unstubAllGlobals();
    });

    function mockFetchImpl(queryResponses: TransformResponseBody) {
      mockFetch.mockImplementation(url => {
        if (
          url ===
          'http://my-pull-endpoint.dev/api/zero/pull?schema=this_app_2&appID=this_app'
        ) {
          return Promise.resolve(
            new Response(
              JSON.stringify([
                'transformed',
                queryResponses,
              ] satisfies TransformResponseMessage),
            ),
          );
        }
        return Promise.reject(new Error('Unexpected fetch call ' + url));
      });
    }

    test('initial hydration of a custom query', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query', args: ['thing']},
      ]);
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "custom-1",
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
                  "hash": "custom-1",
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

      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
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
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
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
                "custom-1": 1,
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
                "custom-1": 1,
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
                "custom-1": 1,
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

    test('different custom queries end up with the same query after transformation', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query-1',
        },
        {
          ast: ISSUES_QUERY,
          id: 'custom-2',
          name: 'named-query-2',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
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
                  "hash": "custom-1",
                  "op": "put",
                },
                {
                  "hash": "custom-2",
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
      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
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
            "table": "this_app_2.clients",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "custom-1": 1,
              "custom-2": 1,
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
              "custom-1": 1,
              "custom-2": 1,
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
              "custom-1": 1,
              "custom-2": 1,
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
              "custom-1": 1,
              "custom-2": 1,
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

    // not yet supported: test('a single custom query that returns many queries' () => {});
  });

  test('delete client', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 2, 4));

    const {queue: client1} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    const {queue: client2, source: connectSource2} = connectWithQueueAndSource(
      {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'},
      [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl}],
    );

    await nextPoke(client1);
    await nextPoke(client2);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client1);
    await nextPoke(client1);

    await nextPoke(client2);
    await nextPoke(client2);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
        {
          "clientID": "bar",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    connectSource2.cancel();

    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
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

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 1741046400000,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    callNextSetTimeout(ttl);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hash2",
              "op": "del",
            },
          ],
          "pokeID": "01:02",
          "rowsPatch": [
            {
              "id": {
                "id": "100",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "101",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "102",
              },
              "op": "del",
              "tableName": "users",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 1741046400000,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);
  });

  test('activeClients inactivates queries from inactive clients', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 5, 30));

    // First, connect client A with queries
    const ctxA = {...SYNC_CONTEXT, clientID: 'clientA', wsID: 'wsA'};
    const {source: streamA, queue: clientA} = connectWithQueueAndSource(ctxA, [
      {op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(clientA); // desire query-hashA
    await nextPoke(clientA); // Got query-hashA and rows

    // Now connect client B and C using initConnection
    const ctxB = {...SYNC_CONTEXT, clientID: 'clientB', wsID: 'wsB'};
    const ctxC = {...SYNC_CONTEXT, clientID: 'clientC', wsID: 'wsC'};

    // Connect client B
    const {source: streamB, queue: clientB} = connectWithQueueAndSource(ctxB, [
      {op: 'put', hash: 'query-hashB', ast: USERS_QUERY, ttl},
    ]);

    // Connect client C
    const {source: streamC, queue: clientC} = connectWithQueueAndSource(ctxC, [
      {op: 'put', hash: 'query-hashC', ast: COMMENTS_QUERY, ttl},
    ]);

    await nextPoke(clientA); // desire query-hashB
    await nextPoke(clientA); // Got query-hashB and rows

    await nextPoke(clientA); // desire query-hashC
    await nextPoke(clientA); // Got query-hashC
    await expectNoPokes(clientA);

    await nextPoke(clientB); // Desire and got A & B and rows
    await nextPoke(clientB); // desire query-hashC
    await nextPoke(clientB); // Got query-hashC
    await expectNoPokes(clientB);

    await nextPoke(clientC); // Desire and got A & B and rows
    await nextPoke(clientC); // desire query-hashC
    await nextPoke(clientC); // Got query-hashC
    await expectNoPokes(clientC);

    // Verify all three clients are active and have their queries
    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
        },
        {
          "clientID": "clientB",
        },
        {
          "clientID": "clientC",
        },
      ]
    `);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" from "this_app_2/cvr".desires ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Close client A & C
    streamA.cancel();
    streamC.cancel();

    await expectNoPokes(clientA);
    await expectNoPokes(clientB);
    await expectNoPokes(clientC);

    // Verify that the clients' queries are NOT inactivated
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt"
        FROM "this_app_2/cvr".desires
        ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Simulate the passage of time.
    const ONE_HOUR = 60 * 60 * 1000;
    vi.setSystemTime(Date.now() + ONE_HOUR);

    // Now reconnect client A with activeClients [clientA, clientB]
    // This should inactivate clientC's queries
    const newCtxA = {...ctxA, baseCookie: '01:04', wsID: 'wsA2'};
    const {source: newStreamA, queue: newClientA} = connectWithQueueAndSource(
      newCtxA,
      [{op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl}],
      undefined,
      ['clientA', 'clientB'],
    );

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "desiredQueriesPatches": {
            "clientC": [
              {
                "hash": "query-hashC",
                "op": "del",
              },
            ],
          },
          "pokeID": "01:05",
        },
      ]
    `);

    await expectNoPokes(newClientA);

    await nextPoke(clientB); // desire delete query-hashC
    await expectNoPokes(clientB);

    // Verify that clientC's query remains present but is inactivated.
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" FROM "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": true,
          "inactivatedAt": 1751245200000,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // If we move time forward 5s the inactivated query should be deleted
    callNextSetTimeout(ttl);

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);
    expect(await nextPokeParts(clientB)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);

    // Clean up the streams
    newStreamA.cancel();
    streamB.cancel();

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "hash": "query-hash1.1",
                  "op": "put",
                },
                {
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
                "hash": "query-hash1",
                "op": "put",
              },
              {
                "hash": "query-hash1.1",
                "op": "put",
              },
              {
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

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
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
          "table": "this_app_2.clients",
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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

  test('initial hydration, schema unsupported', async () => {
    const client = connect(
      {...SYNC_CONTEXT, schemaVersion: 1},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      {
        tables: {foo: {columns: {bar: {type: 'string'}}}},
      },
    );
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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
        'The "foo" table does not exist or is not one of the replicated tables: "comments","issueLabels","issues","labels","users".',
    });
  });

  test('initial hydration, schemaVersion unsupported with bad query', async () => {
    vi.setSystemTime(Date.UTC(2025, 6, 14));
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

    let err;
    try {
      // Depending on the ordering of events, the error can happen on
      // the first or second poke.
      await nextPoke(client);
      await nextPoke(client);
    } catch (e) {
      err = e;
    }
    // Make sure it's the SchemaVersionNotSupported error that gets
    // propagated, and not any error related to the bad query.
    expect(err).toBeInstanceOf(ErrorForClient);
    expect((err as ErrorForClient).errorBody).toEqual({
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
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
    await expectNoPokes(client);

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

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
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
          "table": "this_app_2.clients",
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

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
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
          "table": "this_app_2.clients",
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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
            "pokeID": "00:02",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
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
      appMessages.update('schemaVersions', {
        lock: true,
        minSupportedVersion: 3,
        maxSupportedVersion: 3,
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
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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

  test('process advancement with schema change that breaks client support', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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
        pokeID: '01',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction('07', messages.dropColumn('issues', 'owner'));

    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'The "issues"."owner" column does not exist or is not one of the replicated columns: "big","id","json","parent","title".',
    });
  });

  test('process advancement with lmid change, client has no queries.  See https://bugs.rocicorp.dev/issue/3628', async () => {
    const client = connect(SYNC_CONTEXT, []);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
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

    replicator.processTransaction(
      '02',
      app2Messages.update('clients', {
        clientGroupID: serviceID,
        clientID: SYNC_CONTEXT.clientID,
        userID: null,
        lastMutationID: 43,
      }),
    );
    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "lastMutationIDChanges": {
              "foo": 43,
            },
            "pokeID": "02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "02",
            "pokeID": "02",
          },
        ],
      ]
    `);
  });

  test('catchup client', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
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
        httpCookie: undefined,
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
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
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

  test('catchup new client before advancement', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client1);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: '00:01',
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
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
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

  test('waits for replica to catchup', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '01'.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

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
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
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
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      '1' + REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, now, now, ttlClock);

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

  test('initial CVR ownership takeover', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now - 600_000;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // Signal that the replica is ready before any connection
    // message is received.
    stateChanges.push({state: 'version-ready'});

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('deleteClients before init connection initiates takeover', async () => {
    // First simulate a takeover that has happened since the view-syncer
    // was started.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // deleteClients should be considered a new connection and
    // take over the CVR.
    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('sends invalid base cookie if client is ahead of CVR', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

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
    // Bump time forward to verify that the timeout is reset later.
    vi.setSystemTime(now + 3);

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

  test('query with exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the exists condition
    replicator.processTransaction(
      '123',
      messages.update('comments', {
        id: '1',
        text: 'foo',
      }),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
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
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "2",
                  "issueID": "1",
                  "text": "bar",
                },
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

  test('query with not exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the not-exists condition by deleting the comment
    // that matches text='bar'.
    replicator.processTransaction(
      '123',
      messages.delete('comments', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
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
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "comment 1",
                },
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

  describe('expired queries', () => {
    test('expired query is removed', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);

      // Mark query-hash1 as inactive
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);

      // Make sure we do not get a delete of the gotQueriesPatch
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
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
                  "id": "2",
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
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('expired query is readded', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);

      // Mark query-hash1 as inactive
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);

      // Make sure we do not get a delete of the gotQueriesPatch
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl / 2);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
          ],
        },
      ]);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:02",
          },
        ]
      `);

      // No got queries patch since we newer removed.
      await expectNoPokes(client);

      callNextSetTimeout(ttl);

      await expectNoPokes(client);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:03",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(ttl * 2);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:04",
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
                  "id": "2",
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
            ],
          },
        ]
      `);
    });

    test('query is added twice with longer ttl', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);

      // Set the same query again but with 2*ttl
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
          ],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      vi.setSystemTime(Date.now() + ttl * 2);

      // Now delete it and make sure it takes 2 * ttl to get the got delete.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:02",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(2 * ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:03",
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
                  "id": "2",
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
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('query is added twice with shorter ttl', async () => {
      const ttl = 100;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl: ttl * 2},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);

      // Set the same query again but with lower ttl which has no effect
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
          ],
        },
      ]);
      await expectNoPokes(client);

      vi.setSystemTime(Date.now() + 2 * ttl);

      // Now delete it and make sure it takes 2 * ttl to get the got delete.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);
      callNextSetTimeout(2 * ttl);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
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
                  "id": "2",
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
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('expire time is too far in the future', async () => {
      // The timer is limited to 1h... This test sets the expire to 2.5h in the future... so the timer should
      // be fired at 1h, 2h and once again at 2.5h

      const ttl = 2.5 * 60 * 60 * 1000;
      vi.setSystemTime(Date.now());
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);

      // Mark query-hash1 as inactive
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);

      // Make sure we do not get a delete of the gotQueriesPatch
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      callNextSetTimeout(60 * 60 * 1000);

      await expectNoPokes(client);

      callNextSetTimeout(60 * 60 * 1000);

      await expectNoPokes(client);

      callNextSetTimeout(30 * 60 * 1000);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
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
                  "id": "2",
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
            ],
          },
        ]
      `);
    });
  });

  describe('LRU', () => {
    // LMID query has 1 row
    // USERS_QUERY has 3 rows
    // COMMENTS_QUERY has 2 rows
    // ISSUES_QUERY has 4 rows

    test('2 queries', async () => {
      vs.maxRowCount = 4;

      // This test has two queries and together the size is too large. When one becomes inactive
      // we should evict that one.

      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'user-query-hash', ast: USERS_QUERY}, // 3 rows
        {op: 'put', hash: 'comment-query-hash', ast: COMMENTS_QUERY}, // 2 rows
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "put",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "put",
              },
              {
                "hash": "comment-query-hash",
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
                "tableName": "users",
                "value": {
                  "id": "100",
                  "name": "Alice",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "101",
                  "name": "Bob",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "102",
                  "name": "Candice",
                },
              },
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
                  "text": "bar",
                },
              },
            ],
          },
        ]
      `);

      expect(
        (
          await cvrDB`SELECT count(*) from "this_app_2/cvr".rows where "this_app_2/cvr".rows.table != 'this_app_2.clients'`.values()
        )[0][0],
      ).toBe(5n);

      await expectNoPokes(client);

      // We now mark the USERS_QUERY as inactive. Since we are above the desired
      // row count we will evict the USERS_QUERY and get rowsPatch deletes.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'user-query-hash'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "100",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "101",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "102",
                },
                "op": "del",
                "tableName": "users",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);

      // now we mark the COMMENTS_QUERY as inactive. Since we are below the desired
      // row count we should not evict anything.

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'comment-query-hash'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "comment-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:03",
          },
        ]
      `);
      await expectNoPokes(client);
    });

    test('3 queries', async () => {
      vs.maxRowCount = 5;

      // This test is similar to the previous one but we have 3 queries with no ttl.
      // We will inactivate users first, then comments and finally issues.
      // after each, we will check that the oldest query is evicted.
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'user-query-hash', ast: USERS_QUERY}, // 3 rows
        {op: 'put', hash: 'comment-query-hash', ast: COMMENTS_QUERY}, // 2 rows
        {op: 'put', hash: 'issue-query-hash', ast: ISSUES_QUERY}, // 4 rows
      ]);
      stateChanges.push({state: 'version-ready'});

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "put",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "put",
                },
                {
                  "hash": "issue-query-hash",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "put",
              },
              {
                "hash": "comment-query-hash",
                "op": "put",
              },
              {
                "hash": "issue-query-hash",
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
                "tableName": "users",
                "value": {
                  "id": "100",
                  "name": "Alice",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "101",
                  "name": "Bob",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "102",
                  "name": "Candice",
                },
              },
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
                  "text": "bar",
                },
              },
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
        ]
      `);

      expect(
        (
          await cvrDB`SELECT count(*) from "this_app_2/cvr".rows where "this_app_2/cvr".rows.table != 'this_app_2.clients'`.values()
        )[0][0],
      ).toBe(9n);

      await expectNoPokes(client);

      // This is needed because we are using Date.now but real time and we want to ensure
      // that the invalidatedAt is increasing.
      function loopOneMs() {
        const start = Date.now();
        while (Date.now() - start < 1);
      }

      // We now mark the queries as inactive in the order users, comments and
      // then issues moving the time forward after each inactivation. This means
      // that the oldest query will be evicted each time.
      loopOneMs();
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'user-query-hash'}],
        },
      ]);
      loopOneMs();
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'comment-query-hash'}],
        },
      ]);
      loopOneMs();
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'issue-query-hash'}],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "100",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "101",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "102",
                },
                "op": "del",
                "tableName": "users",
              },
            ],
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "comment-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:03",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "comment-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "01:04",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "comments",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "comments",
              },
            ],
          },
        ]
      `);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "issue-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:05",
          },
        ]
      `);
      // Not deleting issue rows because we are now under the limit.
      await expectNoPokes(client);
    });

    test('3 queries, inactivate 2 at the same time', async () => {
      vs.maxRowCount = 5;

      // This test is similar to the previous one but we inactivate two at the same time. Both should be evicted.
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'user-query-hash', ast: USERS_QUERY}, // 3 rows
        {op: 'put', hash: 'comment-query-hash', ast: COMMENTS_QUERY}, // 2 rows
        {op: 'put', hash: 'issue-query-hash', ast: ISSUES_QUERY}, // 4 rows
      ]);
      stateChanges.push({state: 'version-ready'});

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "put",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "put",
                },
                {
                  "hash": "issue-query-hash",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "put",
              },
              {
                "hash": "comment-query-hash",
                "op": "put",
              },
              {
                "hash": "issue-query-hash",
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
                "tableName": "users",
                "value": {
                  "id": "100",
                  "name": "Alice",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "101",
                  "name": "Bob",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "102",
                  "name": "Candice",
                },
              },
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
                  "text": "bar",
                },
              },
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
        ]
      `);

      expect(
        (
          await cvrDB`SELECT count(*) from "this_app_2/cvr".rows where "this_app_2/cvr".rows.table != 'this_app_2.clients'`.values()
        )[0][0],
      ).toBe(9n);

      await expectNoPokes(client);

      // We now mark the queries as inactive in the order users and comments.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'del', hash: 'user-query-hash'},
            {op: 'del', hash: 'comment-query-hash'},
          ],
        },
      ]);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "del",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "01:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "100",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "101",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "102",
                },
                "op": "del",
                "tableName": "users",
              },
            ],
          },
        ]
      `);

      // We continue since we are still above the limit.
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "comment-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "01:03",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "comments",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "comments",
              },
            ],
          },
        ]
      `);
      await expectNoPokes(client);
    });

    test('2 queries, evict due to adding new rows', async () => {
      vs.maxRowCount = 6;

      // This test has two queries and together the size is too large. When one becomes inactive
      // we should evict that one.

      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'user-query-hash', ast: USERS_QUERY}, // 3 rows
        {op: 'put', hash: 'comment-query-hash', ast: COMMENTS_QUERY}, // 2 rows
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "put",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "put",
              },
              {
                "hash": "comment-query-hash",
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
                "tableName": "users",
                "value": {
                  "id": "100",
                  "name": "Alice",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "101",
                  "name": "Bob",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "102",
                  "name": "Candice",
                },
              },
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
                  "text": "bar",
                },
              },
            ],
          },
        ]
      `);

      expect(
        (
          await cvrDB`SELECT count(*) from "this_app_2/cvr".rows where "this_app_2/cvr".rows.table != 'this_app_2.clients'`.values()
        )[0][0],
      ).toBe(5n);

      await expectNoPokes(client);

      // We now mark the USERS_QUERY as inactive. Since we are above the desired
      // row count we will evict the USERS_QUERY and get rowsPatch deletes.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'del', hash: 'user-query-hash'},
            {op: 'del', hash: 'comment-query-hash'},
          ],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "del",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      // Now we add new rows to the users and we should evict the queries.
      replicator.processTransaction(
        '101',
        messages.insert('users', {
          id: '103',
          name: 'Dude',
        }),
      );
      stateChanges.push({state: 'version-ready'});

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "pokeID": "101",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "103",
                  "name": "Dude",
                },
              },
            ],
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "101:01",
            "rowsPatch": [
              {
                "id": {
                  "id": "100",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "101",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "102",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "103",
                },
                "op": "del",
                "tableName": "users",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('3 queries, evict 2 due to adding new rows', async () => {
      vs.maxRowCount = 11;

      // This test has two queries and together the size is too large. When one becomes inactive
      // we should evict that one.

      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'user-query-hash', ast: USERS_QUERY}, // 3 rows
        {op: 'put', hash: 'comment-query-hash', ast: COMMENTS_QUERY}, // 2 rows
        {op: 'put', hash: 'issue-query-hash', ast: ISSUES_QUERY2}, // 5 rows
      ]);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "user-query-hash",
                  "op": "put",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "put",
                },
                {
                  "hash": "issue-query-hash",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "put",
              },
              {
                "hash": "comment-query-hash",
                "op": "put",
              },
              {
                "hash": "issue-query-hash",
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
                "tableName": "users",
                "value": {
                  "id": "100",
                  "name": "Alice",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "101",
                  "name": "Bob",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "102",
                  "name": "Candice",
                },
              },
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
                  "text": "bar",
                },
              },
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
        ]
      `);

      expect(
        (
          await cvrDB`SELECT count(*) from "this_app_2/cvr".rows where "this_app_2/cvr".rows.table != 'this_app_2.clients'`.values()
        )[0][0],
      ).toBe(10n);

      await expectNoPokes(client);

      // We now mark the all the queries as inactive. Since we are above the desired
      // row count we will evict the USERS_QUERY and get rowsPatch deletes.
      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {op: 'del', hash: 'issue-query-hash'},
            {op: 'del', hash: 'user-query-hash'},
            {op: 'del', hash: 'comment-query-hash'},
          ],
        },
      ]);
      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "issue-query-hash",
                  "op": "del",
                },
                {
                  "hash": "user-query-hash",
                  "op": "del",
                },
                {
                  "hash": "comment-query-hash",
                  "op": "del",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ]
      `);

      await expectNoPokes(client);

      // Now we add new rows to the users and we should evict the queries.
      // add 4 users and 3 issues.
      const changes: DataChange[] = [];
      for (let i = 0; i < 5; i++) {
        changes.push(
          messages.insert('users', {
            id: `10x${i}`,
            name: `User ${i}`,
          }),
        );
      }
      for (let i = 0; i < 4; i++) {
        changes.push(
          messages.insert('issues', {
            id: `5${i}`,
            title: `issue ${i}`,
            big: 100,
            json: null,
            owner: '101',
            parent: '2',
          }),
        );
      }
      replicator.processTransaction('101', ...changes);
      stateChanges.push({state: 'version-ready'});

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "pokeID": "101",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "50",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "issue 0",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "51",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "issue 1",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "52",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "issue 2",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "53",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "issue 3",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "10x0",
                  "name": "User 0",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "10x1",
                  "name": "User 1",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "10x2",
                  "name": "User 2",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "10x3",
                  "name": "User 3",
                },
              },
              {
                "op": "put",
                "tableName": "users",
                "value": {
                  "id": "10x4",
                  "name": "User 4",
                },
              },
            ],
          },
        ]
      `);

      // expect(await nextPokeParts(client)).toMatchObject(
      // rowDeletes('users', 10)
      // )

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "user-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "101:01",
            "rowsPatch": [
              {
                "id": {
                  "id": "100",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "101",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "102",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "10x0",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "10x1",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "10x2",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "10x3",
                },
                "op": "del",
                "tableName": "users",
              },
              {
                "id": {
                  "id": "10x4",
                },
                "op": "del",
                "tableName": "users",
              },
            ],
          },
        ]
      `);

      expect(await nextPokeParts(client)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "comment-query-hash",
                "op": "del",
              },
            ],
            "pokeID": "101:02",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "comments",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "comments",
              },
            ],
          },
        ]
      `);

      await expectNoPokes(client);
    });

    test('catchup client', async () => {
      vs.maxRowCount = 5;

      const client1 = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY2}, // 5 rows
      ]);
      expect(await nextPoke(client1)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
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
      const poke = await nextPoke(client1);

      expect(poke[1]).toMatchInlineSnapshot(`
        [
          "pokePart",
          {
            "gotQueriesPatch": [
              {
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
        ]
      `);
      const preAdvancement = poke[2][1] as PokeEndBody;
      expect(preAdvancement).toEqual({
        cookie: '01',
        pokeID: '01',
      });

      replicator.processTransaction(
        '123',
        messages.insert('issues', {
          id: '6',
          title: 'new title 6',
          owner: 100,
          parent: null,
          big: 9007199254740991n,
        }),
        messages.insert('issues', {
          id: '7',
          title: 'new title 7',
          owner: 100,
          parent: null,
          big: 9007199254740991n,
        }),
      );

      stateChanges.push({state: 'version-ready'});
      const advancement = (await nextPoke(client1))[1][1] as PokePartBody;
      expect(advancement).toMatchInlineSnapshot(`
        {
          "pokeID": "123",
          "rowsPatch": [
            {
              "op": "put",
              "tableName": "issues",
              "value": {
                "big": 9007199254740991,
                "id": "6",
                "json": null,
                "owner": "100.0",
                "parent": null,
                "title": "new title 6",
              },
            },
            {
              "op": "put",
              "tableName": "issues",
              "value": {
                "big": 9007199254740991,
                "id": "7",
                "json": null,
                "owner": "100.0",
                "parent": null,
                "title": "new title 7",
              },
            },
          ],
        }
      `);

      await vs.changeDesiredQueries(SYNC_CONTEXT, [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
        },
      ]);
      expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
        [
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "del",
                },
              ],
            },
            "pokeID": "123:01",
          },
        ]
      `);
      expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
        [
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "del",
              },
            ],
            "pokeID": "123:02",
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
                  "id": "2",
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
              {
                "id": {
                  "id": "6",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "7",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ]
      `);

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
          httpCookie: undefined,
        },
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY2}],
      );

      // Response should catch client2 up with the all 7 rows since the query was evicted for client1
      expect(await nextPoke(client2)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "01",
              "pokeID": "123:04",
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
                    "hash": "query-hash1",
                    "op": "put",
                  },
                ],
                "foo": [
                  {
                    "hash": "query-hash1",
                    "op": "del",
                  },
                ],
              },
              "gotQueriesPatch": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
              "pokeID": "123:04",
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
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 9007199254740991,
                    "id": "6",
                    "json": null,
                    "owner": "100.0",
                    "parent": null,
                    "title": "new title 6",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 9007199254740991,
                    "id": "7",
                    "json": null,
                    "owner": "100.0",
                    "parent": null,
                    "title": "new title 7",
                  },
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "123:04",
              "pokeID": "123:04",
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
              "baseCookie": "123:02",
              "pokeID": "123:03",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "bar": [
                  {
                    "hash": "query-hash1",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "123:03",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "123:03",
              "pokeID": "123:03",
            },
          ],
        ]
      `);
    });
  });

  describe('ttlClock', () => {
    test('one client', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect a client with a TTL.
      const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client); // config
      await nextPoke(client); // hydration

      // Before closing, check initial values.
      const result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t0);
      expect(result[0].lastActive).toBe(t0);

      {
        vi.setSystemTime(Date.now() + 500);
        const t1 = Date.now();

        // Close the connection.
        source.cancel();

        await sleep(10);
        vi.setSystemTime(Date.now() + 10);

        // Before closing, check initial values.
        const result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
        expect(result).toEqual([
          {
            ttlClock: t1,
            lastActive: t1,
          },
        ]);
      }
    });

    test('two clients', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect two clients with a TTL.
      const ctx1 = {...SYNC_CONTEXT, clientID: 'foo', wsID: 'ws1'};
      const ctx2 = {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'};
      const {queue: client1, source: source1} = connectWithQueueAndSource(
        ctx1,
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl}],
      );
      const {queue: client2, source: source2} = connectWithQueueAndSource(
        ctx2,
        [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl}],
      );

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client1); // config
      await nextPoke(client1); // hydration
      await nextPoke(client2); // config
      await nextPoke(client2); // hydration

      // Before closing, check initial values.
      let result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t0, lastActive: t0}]);

      vi.setSystemTime(Date.now() + 500);
      // Close the first connection.
      source1.cancel();
      await sleep(10);
      vi.setSystemTime(Date.now() + 10);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t0, lastActive: t0}]);

      // Move time forward and close the second connection.
      vi.setSystemTime(Date.now() + 500);
      const t2 = Date.now();

      source2.cancel();
      await sleep(10);
      vi.setSystemTime(Date.now() + 10);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result).toEqual([{ttlClock: t2, lastActive: t2}]);
    });

    test('one client - disconnect and reconnect', async () => {
      const ttl = 1000;
      vi.setSystemTime(Date.UTC(2025, 5, 4, 24));
      const t0 = Date.now();

      // Connect a client with a TTL.
      const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client); // config
      await nextPoke(client); // hydration

      // Initial values.
      let result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t0);
      expect(result[0].lastActive).toBe(t0);

      // Disconnect.
      vi.setSystemTime(Date.now() + 500);
      const t1 = Date.now();
      source.cancel();
      await sleep(10);

      result = await cvrDB`
        SELECT "ttlClock", "lastActive"
        FROM "this_app_2/cvr".instances
        WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t1);

      // Wait some time while disconnected (should not advance ttlClock).
      vi.setSystemTime(Date.now() + 1000);
      await sleep(100);

      // Reconnect.
      const client2 = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
      ]);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client2);

      // After reconnect, ttlClock and lastActive have not been updated yet.
      const t2 = Date.now();
      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t1);

      // Make a change.
      replicator.processTransaction(
        '123',
        messages.update('issues', {
          id: '1',
          title: 'new title',
          owner: 100,
          parent: null,
          big: 9007199254740991n,
        }),
      );
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client2);

      result = await cvrDB`
          SELECT "ttlClock", "lastActive"
          FROM "this_app_2/cvr".instances
          WHERE "clientGroupID" = ${serviceID}`;
      expect(result[0].ttlClock).toBe(t1);
      expect(result[0].lastActive).toBe(t2);
    });
  });

  test('Query with ttl 10 minutes, disconnect after 5m, reconnect after an hour, should expire after 5 more minutes ', async () => {
    const ttl = 10 * 60 * 1000; // 10 minutes
    vi.setSystemTime(Date.UTC(2025, 5, 4, 12, 0, 0));
    const t0 = Date.now();

    // Connect a client with a TTL.
    const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // config
    await nextPoke(client); // hydration

    // Advance 5 minutes, then remove the query.
    const t1 = t0 + 5 * 60 * 1000;
    vi.setSystemTime(t1);
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
      },
    ]);
    // Should get a desiredQueriesPatches poke, but not expired yet.
    await nextPokeParts(client);

    await expectNoPokes(client);

    // Advance another 5 minutes (total 10m), then disconnect.
    const t2 = t1 + 5 * 60 * 1000;
    vi.setSystemTime(t2);
    source.cancel();
    await sleep(10);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    // Wait an hour while disconnected.
    const t3 = t2 + 60 * 60 * 1000;
    vi.setSystemTime(t3);
    await sleep(10);
    setTimeoutFn.mockClear();

    // Reconnect. The query should still be there but should expire after 5 more minutes.
    const {queue: client2} = connectWithQueueAndSource(SYNC_CONTEXT, []);
    stateChanges.push({state: 'version-ready'});

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    await nextPokeParts(client2);
    await expectNoPokes(client2);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2,
        lastActive: t2,
      },
    ]);

    // Advance 5 minutes
    callNextSetTimeout(5 * 60 * 1000);

    expect((await nextPokeParts(client2))[0].gotQueriesPatch)
      .toMatchInlineSnapshot(`
        [
          {
            "hash": "query-hash1",
            "op": "del",
          },
        ]
      `);
    await expectNoPokes(client2);

    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t2 + 300_000,
        lastActive: t3 + 300_000,
      },
    ]);
  });

  test('TTL eviction based on service active time, not wallclock time', async () => {
    const ttl = 10 * 1000; // 10 seconds for faster test
    vi.setSystemTime(Date.UTC(2025, 6, 1, 12, 0, 0));
    const t0 = Date.now();

    // Connect a client with a TTL query.
    const {queue: client, source} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // config
    await nextPoke(client); // hydration

    // Advance 2 seconds, then inactivate the query.
    const t1 = t0 + 2 * 1000;
    vi.setSystemTime(t1);
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [{op: 'del', hash: 'query-hash1'}],
      },
    ]);
    await nextPokeParts(client); // Should get a desiredQueriesPatches poke

    // Verify the query is inactivated at ttlClock t1.
    expect(
      await cvrDB`
      SELECT "ttlClock", "lastActive"
      FROM "this_app_2/cvr".instances
      WHERE "clientGroupID" = ${serviceID}
    `,
    ).toEqual([
      {
        ttlClock: t1,
        lastActive: t1,
      },
    ]);

    // Simulate service downtime: advance wallclock but keep ttlClock paused.
    source.cancel();
    const t2 = t1 + 60 * 1000; // 1 minute later in wallclock
    vi.setSystemTime(t2);

    // Clear previous setTimeout calls.
    setTimeoutFn.mockClear();

    // Connect a new client to restart the service.
    const client2 = connect(SYNC_CONTEXT, []);
    stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client2);

    // The query should NOT be expired yet because:
    // - inactivatedAt was t1 (when ttlClock was t1)
    // - ttlClock is still t1 (service was down, so no time passed for TTL)
    // - expiry should be at ttlClock = t1 + ttl (t1 + 10 seconds)
    // - current ttlClock is still t1, so 10 seconds remain
    const desires = await cvrDB`
      SELECT "deleted", "inactivatedAt"
      FROM "this_app_2/cvr".desires
      WHERE "clientGroupID" = ${serviceID}
    `;
    expect(desires).toEqual([
      {
        deleted: true,
        inactivatedAt: t1,
      },
    ]);

    // A timer should be scheduled for the remaining TTL (10 seconds).
    expect(setTimeoutFn).toHaveBeenCalledTimes(1);
    const [, delay] = setTimeoutFn.mock.calls[0];
    expect(delay).toBe(ttl); // Full 10 seconds since no service time passed

    // Now simulate the service running for 10 seconds to trigger eviction.
    callNextSetTimeout(ttl);

    // The eviction should trigger a poke to notify clients about the deletion.
    expect(await nextPokeParts(client2)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hash1",
              "op": "del",
            },
          ],
          "pokeID": "01:02",
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
                "id": "2",
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
          ],
        },
      ]
    `);

    await expectNoPokes(client2);

    // The query should now be expired and cleaned up.
    const desiresAfterEviction = await cvrDB`
      SELECT "deleted"
      FROM "this_app_2/cvr".desires
      WHERE "clientGroupID" = ${serviceID}
    `;
    // Query should remain but be marked as deleted (tombstone)
    expect(desiresAfterEviction).toEqual([{deleted: true}]);
  });
});

describe('permissions', () => {
  let stateChanges: Subscription<ReplicaState>;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
  ) => Queue<Downstream>;
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
    httpCookie: undefined,
  };

  beforeEach(async () => {
    ({
      stateChanges,
      connect,
      vs,
      viewSyncerDone,
      cvrDB,
      replicaDbFile,
      replicator,
    } = await setup(permissions));
  });

  afterEach(async () => {
    // Restores fake date if used.
    vi.useRealTimers();
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
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
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
      appMessages.update('permissions', {
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
    const client = connect(SYNC_CONTEXT, [
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
    const client = connect(
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
                  "text": "bar",
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
  const lc = createSilentLogContext();

  test('previous token is undefined', () => {
    expect(
      pickToken(lc, undefined, {decoded: {sub: 'foo', iat: 1}, raw: ''}),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 1,
      },
      raw: '',
    });
  });

  test('previous token exists, new token is undefined', () => {
    expect(() =>
      pickToken(lc, {decoded: {sub: 'foo', iat: 1}, raw: ''}, undefined),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token does not', () => {
    expect(() =>
      pickToken(lc, {decoded: {sub: 'foo'}, raw: ''}, {decoded: {}, raw: ''}),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has a different subject', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'bar', iat: 1}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has the same subject', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });

    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has no subject, new token has a subject', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 123}, raw: ''},
        {decoded: {iat: 123}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has no subject, new token has no subject', () => {
    expect(
      pickToken(lc, {decoded: {iat: 1}, raw: ''}, {decoded: {iat: 2}, raw: ''}),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
    });
    expect(
      pickToken(lc, {decoded: {iat: 2}, raw: ''}, {decoded: {iat: 1}, raw: ''}),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token does not', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has an issued at time, new token has a greater issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token has a lesser issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token has the same issued at time', () => {
    expect(
      pickToken(
        lc,
        {
          decoded: {sub: 'foo', iat: 2},
          raw: '',
        },
        {
          decoded: {sub: 'foo', iat: 2},
          raw: '',
        },
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has no issued at time, new token has an issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo'}, raw: 'no-iat'},
        {decoded: {sub: 'foo', iat: 2}, raw: 'iat'},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: 'iat',
    });
  });

  test('previous token has no issued at time, new token has no issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo'}, raw: ''},
        {decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
      },
      raw: '',
    });
  });
});
