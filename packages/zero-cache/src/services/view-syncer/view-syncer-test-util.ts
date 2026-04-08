import {expect, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {h128} from '../../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {
  TransformRequestMessage,
  TransformResponseBody,
  TransformResponseMessage,
} from '../../../../zero-protocol/src/custom-queries.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {PokePartBody} from '../../../../zero-protocol/src/poke.ts';
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
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {CustomQueryTransformer} from '../../custom-queries/transform-query.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import type {TestDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema} from '../../types/shards.ts';
import {id} from '../../types/sql.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {getMutationsTableDefinition} from '../change-source/pg/schema/shard.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.ts';
import {ConnectionContextManagerImpl} from './connection-context-manager.ts';
import {DrainCoordinator} from './drain-coordinator.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {initViewSyncerSchema} from './schema/init.ts';
import {Snapshotter} from './snapshotter.ts';
import {type SyncContext, ViewSyncerService} from './view-syncer.ts';

export const APP_ID = 'this_app';
export const SHARD_NUM = 2;
export const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};
export const YIELD_THRESHOLD_MS = 200;

export const EXPECTED_LMIDS_AST: AST = {
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

export const ON_FAILURE = (e: unknown) => {
  throw e;
};

export const queryConfig: ZeroConfig['query'] = {
  url: ['http://my-pull-endpoint.dev/api/zero/pull'],
  forwardCookies: true,
};

export const REPLICA_VERSION = '01';
export const TASK_ID = 'foo-task';
export const serviceID = '9876';
export const ISSUES_QUERY: AST = {
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

export const ALL_ISSUES_QUERY: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
};

export const COMMENTS_QUERY: AST = {
  table: 'comments',
  orderBy: [['id', 'asc']],
};

export const ISSUES_QUERY_WITH_EXISTS: AST = {
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

export const ISSUES_QUERY_WITH_RELATED: AST = {
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

export const ISSUES_QUERY_WITH_EXISTS_AND_RELATED: AST = {
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

export const ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED: AST = {
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

export const ISSUES_QUERY_WITH_OWNER: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  related: [
    {
      system: 'client',
      correlation: {
        parentField: ['owner'],
        childField: ['id'],
      },
      subquery: {
        table: 'users',
        alias: 'owner',
        orderBy: [['id', 'asc']],
      },
    },
  ],
};

export const ISSUES_QUERY2: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
};

export const USERS_QUERY: AST = {
  table: 'users',
  orderBy: [['id', 'asc']],
};

export const issues = table('issues')
  .columns({
    id: string(),
    title: string(),
    owner: string(),
    parent: string(),
    big: number(),
    json: json(),
  })
  .primaryKey('id');
export const comments = table('comments')
  .columns({
    id: string(),
    issueID: string(),
    text: string(),
  })
  .primaryKey('id');
export const issueLabels = table('issueLabels')
  .columns({
    issueID: string(),
    labelID: string(),
  })
  .primaryKey('issueID', 'labelID');
export const labels = table('labels')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');
export const users = table('users')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

export const schema = createSchema({
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

export const {clientSchema: defaultClientSchema} = clientSchemaFrom(schema);

export type Schema = typeof schema;

export type AuthData = {
  sub: string;
  role: 'user' | 'admin';
  iat: number;
};
export const canSeeIssue = (
  authData: AuthData,
  eb: ExpressionBuilder<'issues', Schema>,
) => eb.cmpLit(authData.role, '=', 'admin');

export const permissions = await definePermissions<AuthData, typeof schema>(
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
          (authData, eb: ExpressionBuilder<'comments', Schema>) =>
            eb.exists('issue', iq =>
              iq.where(({eb}) => canSeeIssue(authData, eb)),
            ),
        ],
      },
    },
  }),
);

export const permissionsAll = await definePermissions<AuthData, typeof schema>(
  schema,
  () => ({
    issues: ANYONE_CAN_DO_ANYTHING,
    comments: ANYONE_CAN_DO_ANYTHING,
    issueLabels: ANYONE_CAN_DO_ANYTHING,
    labels: ANYONE_CAN_DO_ANYTHING,
    users: ANYONE_CAN_DO_ANYTHING,
  }),
);

export async function nextPoke(
  client: Queue<Downstream>,
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

export async function nextPokeParts(
  client: Queue<Downstream>,
): Promise<PokePartBody[]> {
  const pokes = await nextPoke(client);
  return pokes
    .filter((msg: Downstream) => msg[0] === 'pokePart')
    .map(([, body]) => body);
}

export async function expectNoPokes(client: Queue<Downstream>) {
  // Use the dequeue() API that cancels the dequeue() request after a timeout.
  const timedOut = 'nothing' as unknown as Downstream;
  expect(await client.dequeue(timedOut, 10)).toBe(timedOut);
}

// Higher-level helper functions for TTL tests
export function addQuery(
  vs: ViewSyncerService,
  ctx: SyncContext,
  hash: string,
  ast: AST,
  ttl?: number,
): Promise<void> {
  return changeDesiredQueries(vs, ctx, [{op: 'put', hash, ast, ttl}]);
}

export function removeQuery(
  vs: ViewSyncerService,
  ctx: SyncContext,
  hash: string,
): Promise<void> {
  return changeDesiredQueries(vs, ctx, [{op: 'del', hash}]);
}

function changeDesiredQueries(
  vs: ViewSyncerService,
  ctx: SyncContext,
  desiredQueriesPatch: UpQueriesPatch,
): Promise<void> {
  return vs.changeDesiredQueries(ctx, [
    'changeDesiredQueries',
    {
      desiredQueriesPatch,
    },
  ]);
}

export function inactivateQuery(
  vs: ViewSyncerService,
  ctx: SyncContext,
  hash: string,
) {
  return removeQuery(vs, ctx, hash);
}

export function expectGotPut(
  client: Queue<Downstream>,
  ...queryHash: string[]
) {
  return expectGot(client, 'put', queryHash);
}

export function expectGotDel(
  client: Queue<Downstream>,
  ...queryHash: string[]
) {
  return expectGot(client, 'del', queryHash);
}

async function expectGot(
  client: Queue<Downstream>,
  op: 'put' | 'del',
  queryHashes: string[],
) {
  const pokeParts = await nextPokeParts(client);
  const expectedPatches = queryHashes.map(hash => ({hash, op}));
  expect(pokeParts[0].gotQueriesPatch).toEqual(
    expect.arrayContaining(expectedPatches),
  );
  // Also verify we got exactly the expected number of patches
  expect(pokeParts[0].gotQueriesPatch).toHaveLength(queryHashes.length);
  return pokeParts;
}

export function expectDesiredPut(
  client: Queue<Downstream>,
  clientID: string,
  ...queryHash: string[]
) {
  return expectDesired(client, 'put', clientID, queryHash);
}

export function expectDesiredDel(
  client: Queue<Downstream>,
  clientID: string,
  ...queryHash: string[]
) {
  return expectDesired(client, 'del', clientID, queryHash);
}

async function expectDesired(
  client: Queue<Downstream>,
  op: 'put' | 'del',
  clientID: string,
  queryHash: string[],
) {
  const pokeParts = await nextPokeParts(client);
  expect(pokeParts[0].desiredQueriesPatches?.[clientID]).toEqual(
    expect.arrayContaining(queryHash.map(hash => ({hash, op}))),
  );
  return pokeParts;
}

export const TEST_ADMIN_PASSWORD = 'test-pwd';

type SetupOptions = Readonly<{
  authConfig?: Partial<NormalizedZeroConfig['auth']> | undefined;
  /**
   * Enables a default `/query` stub for PG integration tests that should still
   * exercise real auth-validation code paths without having to model full
   * custom-query transform responses.
   */
  queryFetchMode?: 'none' | 'empty-validation' | undefined;
}>;

export type QueryFetchCall = {
  kind: 'validation' | 'transform';
  url: string;
  headers: Headers;
  request: TransformRequestMessage;
};

type QueryFetchBehavior = (
  call: QueryFetchCall,
) => Response | Promise<Response>;

export type QueryFetchMock = {
  readonly calls: QueryFetchCall[];
  readonly validationCalls: QueryFetchCall[];
  readonly transformCalls: QueryFetchCall[];

  respond(body: TransformResponseBody): void;
  respondOnce(body: TransformResponseBody): void;

  reply(response: Response): void;
  replyOnce(response: Response): void;

  reject(error: unknown): void;
  rejectOnce(error: unknown): void;

  handle(handler: QueryFetchBehavior): void;
  handleOnce(handler: QueryFetchBehavior): void;

  reset(): void;
};

type InternalQueryFetchMock = QueryFetchMock & {
  consumeBehavior(): QueryFetchBehavior | undefined;
};

/** Shared PG view-syncer harness used by integration-style tests in this directory. */
export async function setup(
  testDBs: TestDBs,
  testName: string,
  permissions: PermissionsConfig | undefined,
  options: SetupOptions = {},
) {
  const {authConfig = {}, queryFetchMode = 'none'} = options;
  const effectiveQueryConfig: ZeroConfig['query'] =
    queryFetchMode === 'none' ? {...queryConfig, url: []} : queryConfig;
  const queryFetch = createQueryFetchMock();
  const restoreFetch = installQueryFetchStub(
    queryFetchMode,
    effectiveQueryConfig.url?.[0],
    queryFetch,
  );

  const lc = createSilentLogContext();
  const storageDB = new Database(lc, ':memory:');
  storageDB.prepare(CREATE_STORAGE_TABLE).run();

  const replicaDbFile = new DbFile(testName);
  const replica = replicaDbFile.connect(lc);
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
  CREATE TABLE "this_app_2.mutations" (
    "clientGroupID"  TEXT,
    "clientID"       TEXT,
    "mutationID"     INTEGER,
    "result"         TEXT,
    _0_version       TEXT NOT NULL,
    PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
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

  const [cvrDB, upstreamDb] = await Promise.all([
    testDBs.create(testName),
    testDBs.create(testName + '-upstream'),
  ]);
  const shard = id(upstreamSchema(SHARD));
  await upstreamDb.begin(tx =>
    tx.unsafe(`
          CREATE SCHEMA IF NOT EXISTS ${shard};
          ${getMutationsTableDefinition(shard)}
        `),
  );
  await initViewSyncerSchema(lc, cvrDB, SHARD);

  const setTimeoutFn = vi.fn<typeof setTimeout>();

  const replicator = fakeReplicator(lc, replica);
  const stateChanges: Subscription<ReplicaState> = Subscription.create();
  const drainCoordinator = new DrainCoordinator();
  const operatorStorage = new DatabaseStorage(
    storageDB,
  ).createClientGroupStorage(serviceID);

  const config = {
    auth: {...authConfig},
    query: effectiveQueryConfig,
    adminPassword: TEST_ADMIN_PASSWORD,
    app: {
      id: 'this_app',
    },
    replica: {
      file: replicaDbFile.path,
    },
    log: {
      level: 'error',
    },
  } as NormalizedZeroConfig;

  // Create the custom query transformer if configured
  const {query} = config;
  const queryURLs = query.url ?? [];
  const customQueryTransformer =
    queryURLs.length > 0 ? new CustomQueryTransformer(lc, SHARD) : undefined;

  const inspectorDelegate = new InspectorDelegate(customQueryTransformer);
  const contextManager = new ConnectionContextManagerImpl(
    lc,
    config.auth.revalidateIntervalSeconds,
    config.auth.retransformIntervalSeconds,
    {
      url: query.url,
      apiKey: query.apiKey,
      allowedClientHeaders: query.allowedClientHeaders,
      forwardCookies: query.forwardCookies,
    },
    {
      url: config.push?.url ?? config.mutate?.url,
      apiKey: config.push?.apiKey ?? config.mutate?.apiKey,
      allowedClientHeaders:
        config.push?.allowedClientHeaders ??
        config.mutate?.allowedClientHeaders,
      forwardCookies:
        config.push?.forwardCookies ?? config.mutate?.forwardCookies ?? false,
    },
  );
  const vs = new ViewSyncerService(
    config,
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
      'view-syncer.pg.test.ts',
      inspectorDelegate,
      () => YIELD_THRESHOLD_MS,
    ),
    stateChanges,
    drainCoordinator,
    100,
    inspectorDelegate,
    contextManager,
    customQueryTransformer,
    (_lc, _description, op) => op(),
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
    clientSchema: ClientSchema | null = defaultClientSchema,
    activeClients?: string[],
  ): {queue: Queue<Downstream>; source: Source<Downstream>} {
    const selector = {clientID: ctx.clientID, wsID: ctx.wsID};
    vs.contextManager.registerConnection(
      selector,
      {
        protocolVersion: ctx.protocolVersion,
        clientID: ctx.clientID,
        clientGroupID: serviceID,
        profileID: ctx.profileID,
        baseCookie: ctx.baseCookie,
        timestamp: Date.now(),
        lmID: 0,
        wsID: ctx.wsID,
        debugPerf: false,
        auth: ctx.auth?.raw,
        userID: ctx.userID,
        initConnectionMsg: undefined,
        httpCookie: ctx.httpCookie,
        origin: ctx.origin,
      },
      ctx.auth,
    );
    vs.contextManager.initConnection(selector, {
      desiredQueriesPatch,
      clientSchema: clientSchema ?? undefined,
      activeClients,
    });
    const source = vs.initConnection(selector, [
      'initConnection',
      {
        desiredQueriesPatch,
        clientSchema: clientSchema ?? undefined,
        activeClients,
      },
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
    clientSchema?: ClientSchema | null,
  ) {
    return connectWithQueueAndSource(ctx, desiredQueriesPatch, clientSchema)
      .queue;
  }

  return {
    storageDB,
    replicaDbFile,
    replica,
    cvrDB,
    upstreamDb,
    stateChanges,
    drainCoordinator,
    operatorStorage,
    vs,
    viewSyncerDone,
    replicator,
    connect,
    connectWithQueueAndSource,
    setTimeoutFn,
    inspectorDelegate,
    customQueryTransformer,
    queryFetch,
    clearMocks: () => {
      queryFetch.reset();
      restoreFetch();
    },
  };
}

/**
 * Installs an opt-in default `/query` stub for PG tests.
 *
 * The `empty-validation` mode auto-responds to validation requests and routes
 * non-empty transform requests through the returned `queryFetch` controller.
 */
function installQueryFetchStub(
  mode: SetupOptions['queryFetchMode'],
  queryURL: string | undefined,
  queryFetch: InternalQueryFetchMock,
) {
  if (mode !== 'empty-validation' || !queryURL) {
    return () => {};
  }

  const expected = new URL(queryURL);
  vi.stubGlobal('fetch', (url: RequestInfo | URL, init?: RequestInit) => {
    const actual = new URL(url.toString());
    // We do a simple check to make sure that the fetch is to the query URL.
    if (
      actual.origin === expected.origin &&
      actual.pathname === expected.pathname
    ) {
      const request = parseTransformRequest(init);
      if (request[1].length === 0) {
        queryFetch.validationCalls.push({
          kind: 'validation',
          url: actual.toString(),
          headers: new Headers(init?.headers),
          request,
        });
        queryFetch.calls.push(queryFetch.validationCalls.at(-1)!);
        return Promise.resolve(
          Response.json(['transformed', []] satisfies TransformResponseMessage),
        );
      }

      const call: QueryFetchCall = {
        kind: 'transform',
        url: actual.toString(),
        headers: new Headers(init?.headers),
        request,
      };
      queryFetch.transformCalls.push(call);
      queryFetch.calls.push(call);

      const behavior = queryFetch.consumeBehavior();
      if (!behavior) {
        throw new Error(
          'No query fetch response is configured for a custom-query transform. Use the `queryFetch` controller returned by setup().',
        );
      }
      return behavior(call);
    }
    throw new Error(
      `Unexpected fetch call to ${url.toString()} - the query URL is expected to be ${queryURL}.`,
    );
  });

  return () => {
    vi.unstubAllGlobals();
  };
}

function createQueryFetchMock(): InternalQueryFetchMock {
  const calls: QueryFetchCall[] = [];
  const validationCalls: QueryFetchCall[] = [];
  const transformCalls: QueryFetchCall[] = [];
  const queuedBehaviors: QueryFetchBehavior[] = [];
  let defaultBehavior: QueryFetchBehavior | undefined;

  return {
    calls,
    validationCalls,
    transformCalls,

    respond(body) {
      defaultBehavior = makeTransformedBehavior(body);
    },
    respondOnce(body) {
      queuedBehaviors.push(makeTransformedBehavior(body));
    },

    reply(response) {
      defaultBehavior = () => response.clone();
    },
    replyOnce(response) {
      queuedBehaviors.push(() => response.clone());
    },

    reject(error) {
      defaultBehavior = () => Promise.reject(error);
    },
    rejectOnce(error) {
      queuedBehaviors.push(() => Promise.reject(error));
    },

    handle(handler) {
      defaultBehavior = handler;
    },
    handleOnce(handler) {
      queuedBehaviors.push(handler);
    },

    reset() {
      calls.length = 0;
      validationCalls.length = 0;
      transformCalls.length = 0;
      queuedBehaviors.length = 0;
      defaultBehavior = undefined;
    },
    consumeBehavior() {
      return queuedBehaviors.shift() ?? defaultBehavior;
    },
  };

  function makeTransformedBehavior(
    body: TransformResponseBody,
  ): QueryFetchBehavior {
    return () =>
      Response.json(['transformed', body] satisfies TransformResponseMessage);
  }
}

function parseTransformRequest(init?: RequestInit): TransformRequestMessage {
  const body = init?.body ? JSON.parse(String(init.body)) : undefined;
  if (
    !Array.isArray(body) ||
    body[0] !== 'transform' ||
    !Array.isArray(body[1])
  ) {
    throw new Error(
      `Expected a custom-query transform request body, got: ${JSON.stringify(body)}`,
    );
  }
  return body as TransformRequestMessage;
}

export const messages = new ReplicationMessages({
  issues: 'id',
  users: 'id',
  issueLabels: ['issueID', 'labelID'],
  comments: 'id',
});
export const appMessages = new ReplicationMessages(
  {permissions: 'lock'},
  'this_app',
);

export const app2Messages = new ReplicationMessages(
  {clients: ['clientGroupID', 'clientID']},
  'this_app_2',
);
