import {expect, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {h128} from '../../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
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
import {Database} from '../../../../zqlite/src/db.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {initChangeLog} from '../replicator/schema/change-log.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.ts';
import {CREATE_STORAGE_TABLE, DatabaseStorage} from './database-storage.ts';
import {DrainCoordinator} from './drain-coordinator.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {initViewSyncerSchema} from './schema/init.ts';
import {Snapshotter} from './snapshotter.ts';
import {type SyncContext, ViewSyncerService} from './view-syncer.ts';

export const APP_ID = 'this_app';
export const SHARD_NUM = 2;
export const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

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
  eb: ExpressionBuilder<Schema, 'issues'>,
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
          (authData, eb: ExpressionBuilder<Schema, 'comments'>) =>
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

export async function setup(
  testName: string,
  permissions: PermissionsConfig | undefined,
) {
  const lc = createSilentLogContext();
  const storageDB = new Database(lc, ':memory:');
  storageDB.prepare(CREATE_STORAGE_TABLE).run();

  const replicaDbFile = new DbFile(testName);
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

  const cvrDB = await testDBs.create(testName);
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

export const messages = new ReplicationMessages({
  issues: 'id',
  users: 'id',
  issueLabels: ['issueID', 'labelID'],
  comments: 'id',
});
export const appMessages = new ReplicationMessages(
  {
    schemaVersions: 'lock',
    permissions: 'lock',
  },
  'this_app',
);

export const app2Messages = new ReplicationMessages(
  {
    clients: ['clientGroupID', 'clientID'],
  },
  'this_app_2',
);
