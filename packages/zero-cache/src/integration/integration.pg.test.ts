import websocket from '@fastify/websocket';
import type {LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance, type FastifyRequest} from 'fastify';
import {copyFileSync} from 'fs';
import {afterAll, beforeEach, describe, expect, vi} from 'vitest';
import WebSocket from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import {h128} from '../../../shared/src/hash.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../shared/src/queue.ts';
import {randInt} from '../../../shared/src/rand.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {InitConnectionMessage} from '../../../zero-protocol/src/connect.ts';
import type {PokeStartMessage} from '../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-schema/src/permissions.ts';
import type {ChangeStreamMessage} from '../services/change-source/protocol/current/downstream.ts';
import {
  changeSourceUpstreamSchema,
  type ChangeSourceUpstream,
} from '../services/change-source/protocol/current/upstream.ts';
import {getConnectionURI, test, type PgTest} from '../test/db.ts';
import {DbFile} from '../test/lite.ts';
import type {PostgresDB} from '../types/pg.ts';
import {childWorker, type Worker} from '../types/processes.ts';
import {stream, type Sink} from '../types/streams.ts';
import {PROTOCOL_ERROR} from '../types/ws.ts';

// Adjust to debug.
const LOG_LEVEL: LogLevel = 'error';

const foo = table('foo')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const bar = table('boo.far')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const nopk = table('nopk')
  .columns({
    id: string(),
    val: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [foo, bar, nopk],
});

const permissions = await definePermissions(schema, () => ({
  'foo': ANYONE_CAN_DO_ANYTHING,
  'boo.far': ANYONE_CAN_DO_ANYTHING,
  'nopk': ANYONE_CAN_DO_ANYTHING,
}));

// Note: The NULL unicode character \u0000 is specifically used to verify
//       end-to-end JSON compatibility. In particular, any intermediate
//       JSONB storage of row contents would not be able to handle it.
function initialPGSetup(replicaIdentity = 'DEFAULT') {
  return `
      CREATE TABLE foo(
        id TEXT PRIMARY KEY, 
        far_id TEXT,
        b BOOL,
        j1 JSON,
        j2 JSONB,
        "j.3" JSON,
        j4 JSON
      );
      ALTER TABLE foo REPLICA IDENTITY ${replicaIdentity};
      INSERT INTO foo(id, far_id, b, j1, j2, "j.3", j4) 
        VALUES (
          'bar',
          'baz',
          true,
          '{"foo":"bar\\u0000"}',
          'true',
          '123',
          '"string"');

      CREATE SCHEMA boo;
      CREATE TABLE boo.far(id TEXT PRIMARY KEY);
      INSERT INTO boo.far(id) VALUES ('baz');

      CREATE TABLE nopk(id TEXT NOT NULL, val TEXT);
      INSERT INTO nopk(id, val) VALUES ('foo', 'bar');

      CREATE PUBLICATION zero_all FOR TABLE foo, TABLE boo.far, TABLE nopk;

      CREATE SCHEMA "123";

      CREATE TABLE "123".permissions (
        permissions JSON,
        hash TEXT
      );
      INSERT INTO "123".permissions (permissions, hash) VALUES ('${JSON.stringify(
        permissions,
      )}', '${h128(JSON.stringify(permissions)).toString(16)}');
      `;
}

// Keep this in sync with the initialPGSetup
const INITIAL_CUSTOM_SETUP: ChangeStreamMessage[] = [
  ['begin', {tag: 'begin'}, {commitWatermark: '101'}],
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: 'public',
        name: 'foo',
        columns: {
          id: {pos: 0, dataType: 'text', notNull: true},
          ['far_id']: {pos: 1, dataType: 'text'},
          b: {pos: 2, dataType: 'bool'},
          j1: {pos: 3, dataType: 'json'},
          j2: {pos: 4, dataType: 'jsonb'},
          ['j.3']: {pos: 5, dataType: 'json'},
          j4: {pos: 6, dataType: 'json'},
        },
      },
      metadata: {
        rowKey: {
          columns: ['id'],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-index',
      spec: {
        name: 'foo_key',
        schema: 'public',
        tableName: 'foo',
        columns: {id: 'ASC'},
        unique: true,
      },
    },
  ],
  [
    'data',
    {
      tag: 'insert',
      relation: {
        schema: 'public',
        name: 'foo',
        rowKey: {
          columns: ['id'],
        },
      },
      new: {
        id: 'bar',
        ['far_id']: 'baz',
        b: true,
        j1: {foo: 'bar\u0000'},
        j2: true,
        ['j.3']: 123,
        j4: 'string',
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: 'boo',
        name: 'far',
        columns: {
          id: {pos: 0, dataType: 'text', notNull: true},
        },
      },
      metadata: {
        rowKey: {
          columns: ['id'],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-index',
      spec: {
        name: 'boo_far_key',
        schema: 'boo',
        tableName: 'far',
        columns: {id: 'ASC'},
        unique: true,
      },
    },
  ],
  [
    'data',
    {
      tag: 'insert',
      relation: {
        schema: 'boo',
        name: 'far',
        rowKey: {
          columns: ['id'],
        },
      },
      new: {
        id: 'baz',
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: 'public',
        name: 'nopk',
        columns: {
          id: {pos: 0, dataType: 'text', notNull: true},
          val: {pos: 1, dataType: 'text'},
        },
      },
      metadata: {
        rowKey: {
          columns: [],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'insert',
      relation: {
        schema: 'public',
        name: 'nopk',
        rowKey: {
          columns: [],
        },
      },
      new: {
        id: 'foo',
        val: 'bar',
      },
    },
  ],
  // Required internal tables
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: '123_0',
        name: 'clients',
        primaryKey: ['clientGroupID', 'clientID'],
        columns: {
          clientGroupID: {pos: 0, dataType: 'text', notNull: true},
          clientID: {pos: 1, dataType: 'text', notNull: true},
          lastMutationID: {pos: 2, dataType: 'bigint'},
          userID: {pos: 3, dataType: 'text'},
        },
      },
      metadata: {
        rowKey: {
          columns: ['clientGroupID', 'clientID'],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-index',
      spec: {
        name: '123_clients_key',
        schema: '123_0',
        tableName: 'clients',
        columns: {
          clientGroupID: 'ASC',
          clientID: 'ASC',
        },
        unique: true,
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: '123_0',
        name: 'mutations',
        primaryKey: ['clientGroupID', 'clientID', 'mutationID'],
        columns: {
          clientGroupID: {pos: 0, dataType: 'text', notNull: true},
          clientID: {pos: 1, dataType: 'text', notNull: true},
          mutationID: {pos: 2, dataType: 'bigint', notNull: true},
          mutation: {pos: 3, dataType: 'json'},
        },
      },
      metadata: {
        rowKey: {
          columns: ['clientGroupID', 'clientID', 'mutationID'],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-index',
      spec: {
        name: '123_mutations_key',
        schema: '123_0',
        tableName: 'mutations',
        columns: {
          clientGroupID: 'ASC',
          clientID: 'ASC',
          mutationID: 'ASC',
        },
        unique: true,
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-table',
      spec: {
        schema: '123',
        name: 'permissions',
        primaryKey: ['lock'],
        columns: {
          lock: {pos: 0, dataType: 'bool', notNull: true},
          permissions: {pos: 1, dataType: 'json'},
          hash: {pos: 2, dataType: 'text'},
        },
      },
      metadata: {
        rowKey: {
          columns: ['lock'],
        },
      },
    },
  ],
  [
    'data',
    {
      tag: 'create-index',
      spec: {
        name: '123_permissions_key',
        schema: '123',
        tableName: 'permissions',
        columns: {lock: 'ASC'},
        unique: true,
      },
    },
  ],
  [
    'data',
    {
      tag: 'insert',
      relation: {
        schema: '123',
        name: 'permissions',
        rowKey: {
          columns: ['lock'],
        },
      },
      new: {
        lock: true,
        permissions: permissions as unknown as JSONValue,
        hash: h128(JSON.stringify(permissions)).toString(),
      },
    },
  ],
  ['commit', {tag: 'commit'}, {watermark: '101'}],
];

describe('integration', {timeout: 30000}, () => {
  let upDB: PostgresDB;
  let cvrDB: PostgresDB;
  let changeDB: PostgresDB;
  let replicaDbFile: DbFile;
  let replicaDbFile2: DbFile;
  let env: Record<string, string>;
  let port: number;
  let port2: number;
  let zeros: Worker[];
  let zerosExited: Promise<number>[];
  let customBackend: FastifyInstance;
  let customChangeSourceURI: string;
  let customDownstream: Promise<Sink<ChangeStreamMessage>>;

  const mockExit = vi
    .spyOn(process, 'exit')
    .mockImplementation(() => void 0 as never);

  afterAll(() => {
    mockExit.mockRestore();
  });

  const CHANGE_SOURCE_PATH = '/foo/changes/v0/stream';

  beforeEach<PgTest>(async ({testDBs}) => {
    upDB = await testDBs.create('integration_test_upstream');
    cvrDB = await testDBs.create('integration_test_cvr');
    changeDB = await testDBs.create('integration_test_change');
    replicaDbFile = new DbFile('integration_test_replica');
    replicaDbFile2 = new DbFile('integration_test_replica2');
    zeros = [];
    zerosExited = [];

    customBackend = Fastify();
    await customBackend.register(websocket);

    const {promise, resolve} = resolver<Sink<ChangeStreamMessage>>();
    customDownstream = promise;
    customBackend.get(
      CHANGE_SOURCE_PATH,
      {websocket: true},
      (ws: WebSocket, req: FastifyRequest) => {
        const {outstream} = stream<ChangeSourceUpstream, ChangeStreamMessage>(
          createSilentLogContext(),
          ws,
          changeSourceUpstreamSchema,
        );
        if (req.url.includes('lastWatermark=')) {
          resolve(outstream);
        } else {
          // Initial sync.
          for (const change of INITIAL_CUSTOM_SETUP) {
            outstream.push(change);
          }
        }
      },
    );
    customChangeSourceURI =
      (await customBackend.listen({port: 0})) + CHANGE_SOURCE_PATH;

    port = randInt(5000, 16000);
    port2 = randInt(5000, 16000);

    process.env['SINGLE_PROCESS'] = '1';

    env = {
      ['ZERO_PORT']: String(port),
      ['ZERO_LOG_LEVEL']: LOG_LEVEL,
      ['ZERO_UPSTREAM_DB']: getConnectionURI(upDB),
      ['ZERO_UPSTREAM_MAX_CONNS']: '3',
      ['ZERO_CVR_DB']: getConnectionURI(cvrDB),
      ['ZERO_CVR_MAX_CONNS']: '3',
      ['ZERO_APP_ID']: '123', // Verify that a numeric App ID works end-to-end
      ['ZERO_APP_PUBLICATIONS']: 'zero_all',
      ['ZERO_CHANGE_DB']: getConnectionURI(changeDB),
      ['ZERO_REPLICA_FILE']: replicaDbFile.path,
      ['ZERO_NUM_SYNC_WORKERS']: '1',
      ['ZERO_ADMIN_PASSWORD']: '2p49fqnaivnepr',
    };

    return async () => {
      try {
        zeros.forEach(zero => zero.kill('SIGTERM')); // initiate and await graceful shutdown
        (await Promise.all(zerosExited)).forEach(code => expect(code).toBe(0));
      } finally {
        await testDBs.drop(upDB, cvrDB, changeDB);
        replicaDbFile.delete();
        replicaDbFile2.delete();
      }
    };
  }, 30000);

  const FOO_QUERY: AST = {
    table: 'foo',
    orderBy: [['id', 'asc']],
    related: [
      {
        correlation: {
          parentField: ['far_id'],
          childField: ['id'],
        },
        subquery: {
          table: 'boo.far',
          orderBy: [['id', 'asc']],
          alias: 'far',
        },
      },
    ],
  };

  // One or two zero-caches (i.e. multi-node)
  type Envs = [NodeJS.ProcessEnv] | [NodeJS.ProcessEnv, NodeJS.ProcessEnv];

  async function startZero(envs: Envs) {
    assert(zeros.length === 0);
    assert(zerosExited.length === 0);

    let i = 0;
    for (const env of envs) {
      if (++i === 2) {
        // For multi-node, copy the initially-synced replica file from the
        // replication-manager to the replica file for the view-syncer.
        copyFileSync(replicaDbFile.path, replicaDbFile2.path);
      }
      const {promise: ready, resolve: onReady} = resolver<unknown>();
      const {promise: done, resolve: onClose} = resolver<number>();

      zerosExited.push(done);

      const zero = childWorker(
        new URL('../server/runner/main.ts', import.meta.url),
        env,
      );
      zero.onMessageType('ready', onReady);
      zero.on('close', onClose);
      zeros.push(zero);
      await ready;
    }
  }

  async function expectNoPokes(client: Queue<unknown>) {
    // Use the dequeue() API that cancels the dequeue() request after a timeout.
    const timedOut = 'nothing';
    expect(await client.dequeue(timedOut, 500)).toBe(timedOut);
  }

  async function streamCustomChanges(changes: ChangeStreamMessage[]) {
    const sink = await customDownstream;
    for (const change of changes) {
      sink.push(change);
    }
  }

  const WATERMARK_REGEX = /[0-9a-z]{4,}/;
  const BACKFILL_WATERMARK_REGEX = /[0-9a-z]{4,}\.[0-9a-z]{2,}/;

  test.for([
    ['single-node', 'pg', () => [env], undefined],
    ['replica identity full', 'pg', () => [env], 'FULL'],
    [
      'lazy single-node standalone',
      'pg',
      () => [{...env, ['ZERO_LAZY_STARTUP']: 'true'}],
      undefined,
    ],
    [
      'multi-node (routed)',
      'pg',
      () => [
        // The replication-manager must be started first for initial-sync
        {
          ...env,
          ['ZERO_PORT']: `${port2}`,
          ['ZERO_NUM_SYNC_WORKERS']: '0',
        },
        // startZero() will then copy to replicaDbFile2 for the view-syncer
        {
          ...env,
          ['ZERO_CHANGE_STREAMER_URI']: `http://localhost:${port2 + 1}`,
          ['ZERO_REPLICA_FILE']: replicaDbFile2.path,
        },
      ],
      undefined,
    ],
    [
      'multi-node (auto-discover)',
      'pg',
      () => [
        // The replication-manager must be started first for initial-sync
        {
          ...env,
          ['ZERO_PORT']: `${port2}`,
          ['ZERO_NUM_SYNC_WORKERS']: '0',
          ['ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS']: '0',
        },
        // startZero() will then copy to replicaDbFile2 for the view-syncer
        {
          ...env,
          ['ZERO_CHANGE_STREAMER_MODE']: 'discover',
          ['ZERO_REPLICA_FILE']: replicaDbFile2.path,
        },
      ],
      undefined,
    ],
    [
      'lazy view-syncer multi-node (routed)',
      'pg',
      () => [
        // The replication-manager must be started first for initial-sync
        {
          ...env,
          ['ZERO_PORT']: `${port2}`,
          ['ZERO_NUM_SYNC_WORKERS']: '0',
        },
        // startZero() will then copy to replicaDbFile2 for the view-syncer
        {
          ...env,
          ['ZERO_CHANGE_STREAMER_URI']: `http://localhost:${port2 + 1}`,
          ['ZERO_REPLICA_FILE']: replicaDbFile2.path,
          ['ZERO_LAZY_STARTUP']: 'true',
        },
      ],
      undefined,
    ],
    [
      'lazy view-syncer multi-node (auto-discover)',
      'pg',
      () => [
        // The replication-manager must be started first for initial-sync
        {
          ...env,
          ['ZERO_PORT']: `${port2}`,
          ['ZERO_NUM_SYNC_WORKERS']: '0',
          ['ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS']: '0',
        },
        // startZero() will then copy to replicaDbFile2 for the view-syncer
        {
          ...env,
          ['ZERO_CHANGE_STREAMER_MODE']: 'discover',
          ['ZERO_REPLICA_FILE']: replicaDbFile2.path,
          ['ZERO_LAZY_STARTUP']: 'true',
        },
      ],
      undefined,
    ],
    [
      'single-node',
      'custom',
      () => [
        {
          ...env,
          ['ZERO_UPSTREAM_DB']: customChangeSourceURI,
          ['ZERO_UPSTREAM_TYPE']: 'custom',
        },
      ],
      undefined,
    ],
  ] satisfies [string, 'pg' | 'custom', () => Envs, 'FULL' | undefined][])(
    '%s (%s)',
    async ([_name, backend, makeEnvs, replicaIdentity], {skip}) => {
      if (process.env.CI) {
        // TODO: Track down this unhandled rejection that regularly happens
        //       when the test pg instances are shut down, particularly when
        //       running the test with concurrently pg versions.
        //       Then re-enable the test in the CI.
        //
        // ----⎯⎯⎯⎯⎯⎯⎯⎯⎯ Unhandled Rejection ⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯
        // PostgresError: terminating connection due to administrator command
        // ❯ ErrorResponse node_modules/postgres/src/connection.js:794:26
        // ❯ handle node_modules/postgres/src/connection.js:480:6
        // ❯ Socket.data node_modules/postgres/src/connection.js:315:9
        // ❯ Socket.emit node:events:508:28
        // ❯ addChunk node:internal/streams/readable:559:12
        // ❯ readableAddChunkPushByteMode node:internal/streams/readable:510:3
        // ❯ Socket.Readable.push node:internal/streams/readable:390:5
        // ❯ TCP.onStreamRead node:internal/stream_base_commons:189:23
        skip();
      }
      if (backend === 'pg') {
        await upDB.unsafe(initialPGSetup(replicaIdentity));
      }
      await startZero(makeEnvs());

      const downstream = new Queue<unknown>();
      const ws = new WebSocket(
        `ws://localhost:${port}/zero/sync/v${PROTOCOL_VERSION}/connect` +
          `?clientGroupID=abc&clientID=def&wsid=123&schemaVersion=1&baseCookie=&ts=123456789&lmid=1`,
        encodeURIComponent(btoa('{}')), // auth token
      );
      ws.on('message', data =>
        downstream.enqueue(JSON.parse(data.toString('utf-8'))),
      );
      ws.on('open', () =>
        ws.send(
          JSON.stringify([
            'initConnection',
            {
              desiredQueriesPatch: [
                {op: 'put', hash: 'query-hash1', ast: FOO_QUERY},
              ],
              clientSchema: {
                tables: {
                  foo: {
                    primaryKey: ['id'],
                    columns: {id: {type: 'string'}},
                  },
                  ['boo.far']: {
                    primaryKey: ['id'],
                    columns: {id: {type: 'string'}},
                  },
                },
              },
            },
          ] satisfies InitConnectionMessage),
        ),
      );

      expect(await downstream.dequeue()).toMatchObject([
        'connected',
        {wsid: '123'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeStart',
        {pokeID: '00:01'},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokePart',
        {
          pokeID: '00:01',
          desiredQueriesPatches: {
            def: [{op: 'put', hash: 'query-hash1'}],
          },
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: '00:01'},
      ]);
      const contentPokeStart = (await downstream.dequeue()) as PokeStartMessage;
      expect(contentPokeStart).toMatchObject([
        'pokeStart',
        {pokeID: /[0-9a-z]{2,}/},
      ]);
      const contentPokeID = contentPokeStart[1].pokeID;
      expect(await downstream.dequeue()).toMatchObject([
        'pokePart',
        {
          pokeID: contentPokeID,
          gotQueriesPatch: [{op: 'put', hash: 'query-hash1'}],
          rowsPatch: [
            {
              op: 'put',
              tableName: 'foo',
              value: {
                id: 'bar',
                ['far_id']: 'baz',
                b: true,
                j1: {foo: 'bar\u0000'},
                j2: true,
                ['j.3']: 123,
                j4: 'string',
              },
            },
            {
              op: 'put',
              tableName: 'boo.far',
              value: {
                id: 'baz',
              },
            },
          ],
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: contentPokeID},
      ]);

      // Trigger an upstream change and verify replication.
      if (backend === 'pg') {
        await upDB`
          INSERT INTO foo(id, far_id, b, j1, j2, "j.3", j4) 
            VALUES ('voo', 'doo', null, '"foo"', 'false', '456.789', '{"bar":"baz"}');
          UPDATE foo SET far_id = 'not_baz' WHERE id = 'bar';
        `.simple();
      } else {
        await streamCustomChanges([
          // Unlike initial sync, this transaction uses JSON_STRINGIFIED.
          ['begin', {tag: 'begin', json: 's'}, {commitWatermark: '102'}],
          [
            'data',
            {
              tag: 'insert',
              relation: {
                schema: 'public',
                name: 'foo',
                rowKey: {
                  columns: ['id'],
                },
              },
              new: {
                id: 'voo',
                ['far_id']: 'doo',
                b: null,
                j1: '"foo"',
                j2: 'false',
                ['j.3']: '456.789',
                j4: '{"bar":"baz"}',
              },
            },
          ],
          [
            'data',
            {
              tag: 'update',
              relation: {
                schema: 'public',
                name: 'foo',
                rowKey: {
                  columns: ['id'],
                },
              },
              new: {
                id: 'bar',
                ['far_id']: 'not_baz',
                b: true,
                j1: '{"foo":"bar\\u0000"}',
                j2: 'true',
                ['j.3']: '123',
                j4: '"string"',
              },
              key: null,
            },
          ],
          ['commit', {tag: 'commit'}, {watermark: '102'}],
        ]);
      }

      expect(await downstream.dequeue()).toMatchObject([
        'pokeStart',
        {pokeID: WATERMARK_REGEX},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokePart',
        {
          pokeID: WATERMARK_REGEX,
          rowsPatch: [
            {
              op: 'put',
              tableName: 'foo',
              value: {
                id: 'voo',
                ['far_id']: 'doo',
                b: null,
                j1: 'foo',
                j2: false,
                ['j.3']: 456.789,
                j4: {bar: 'baz'},
              },
            },
            {
              op: 'put',
              tableName: 'foo',
              value: {
                b: true,
                ['far_id']: 'not_baz',
                id: 'bar',
                j1: {
                  foo: 'bar\u0000',
                },
                j2: true,
                ['j.3']: 123,
                j4: 'string',
              },
            },
            // boo.far {id: 'baz'} is no longer referenced by foo {id: 'bar}
            {
              id: {id: 'baz'},
              op: 'del',
              tableName: 'boo.far',
            },
          ],
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: WATERMARK_REGEX},
      ]);

      // Test backfill of a new column
      if (backend === 'pg') {
        await upDB.unsafe(/*sql*/ `
          ALTER TABLE foo ADD COLUMN espresso INT DEFAULT (1 + 2 + 3) * 6;
      `);
        expect(await downstream.dequeue()).toMatchObject([
          'pokeStart',
          {pokeID: BACKFILL_WATERMARK_REGEX},
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'pokePart',
          {
            pokeID: BACKFILL_WATERMARK_REGEX,
            rowsPatch: [
              {
                op: 'put',
                tableName: 'foo',
                value: {id: 'bar', espresso: 36},
              },
              {
                op: 'put',
                tableName: 'foo',
                value: {id: 'voo', espresso: 36},
              },
            ],
          },
        ]);
        expect(await downstream.dequeue()).toMatchObject([
          'pokeEnd',
          {pokeID: BACKFILL_WATERMARK_REGEX},
        ]);
      }

      // Test TRUNCATE
      if (backend === 'pg') {
        await upDB`TRUNCATE TABLE foo RESTART IDENTITY`;
      } else {
        await streamCustomChanges([
          ['begin', {tag: 'begin'}, {commitWatermark: '103'}],
          [
            'data',
            {
              tag: 'truncate',
              relations: [
                {
                  schema: 'public',
                  name: 'foo',
                  rowKey: {
                    columns: ['id'],
                  },
                },
              ],
            },
          ],
          ['commit', {tag: 'commit'}, {watermark: '103'}],
        ]);
      }

      expect(await downstream.dequeue()).toMatchObject([
        'pokeStart',
        {pokeID: WATERMARK_REGEX},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokePart',
        {
          pokeID: WATERMARK_REGEX,
          rowsPatch: [
            {
              op: 'del',
              tableName: 'foo',
              id: {id: 'bar'},
            },
            {
              op: 'del',
              tableName: 'foo',
              id: {id: 'voo'},
            },
          ],
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: WATERMARK_REGEX},
      ]);

      // Test that INSERTs into tables without primary keys are replicated.
      if (backend === 'pg') {
        await upDB.unsafe(`
      INSERT INTO nopk(id, val) VALUES ('bar', 'baz');
      CREATE UNIQUE INDEX nopk_key ON nopk (id);
    `);
      } else {
        await streamCustomChanges([
          ['begin', {tag: 'begin'}, {commitWatermark: '104'}],
          [
            'data',
            {
              tag: 'insert',
              relation: {
                schema: 'public',
                name: 'nopk',
                rowKey: {
                  columns: [],
                },
              },
              new: {
                id: 'bar',
                val: 'baz',
              },
            },
          ],
          [
            'data',
            {
              tag: 'create-index',
              spec: {
                name: 'nopk_now_has_key_yay',
                schema: 'public',
                tableName: 'nopk',
                columns: {id: 'ASC'},
                unique: true,
              },
            },
          ],
          ['commit', {tag: 'commit'}, {watermark: '104'}],
        ]);
      }

      // The schema change should result in resetting the pipelines but no
      // poke will result. expectNoPoke() verifies that nothing arrives for
      // a short interval of time, which also allows the schema change to
      // take effect (which is necessary for the subsequent
      // "changeDesiredQueries" to succeed).
      await expectNoPokes(downstream);

      await testInvalidRequestHandling();
    },
  );

  async function testInvalidRequestHandling() {
    const {
      promise: response,
      resolve: closedWith,
      reject: gotError,
    } = resolver<string>();
    // Make sure an invalid websocket request is properly handled.
    const invalidRequest = new WebSocket(`ws://localhost:${port}/foo-bar`);
    invalidRequest.on('error', gotError);
    invalidRequest.on('close', (code, reason) =>
      closedWith(`${code}: ${reason}`),
    );
    expect(await response).toEqual(
      `${PROTOCOL_ERROR}: Error: Invalid URL: /foo-bar`,
    );
  }
});
