import type {LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {copyFileSync} from 'fs';
import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';
import WebSocket from 'ws';
import {assert} from '../../../shared/src/asserts.js';
import {Queue} from '../../../shared/src/queue.js';
import {randInt} from '../../../shared/src/rand.js';
import type {AST} from '../../../zero-protocol/src/ast.js';
import type {InitConnectionMessage} from '../../../zero-protocol/src/connect.js';
import type {PokeStartMessage} from '../../../zero-protocol/src/poke.js';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.js';
import {getConnectionURI, testDBs} from '../test/db.js';
import {DbFile} from '../test/lite.js';
import type {PostgresDB} from '../types/pg.js';
import {childWorker, type Worker} from '../types/processes.js';

// Adjust to debug.
const LOG_LEVEL: LogLevel = 'error';

describe('integration', () => {
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

  const SCHEMA = {
    permissions: {},
    schema: {
      version: 1,
      tables: {},
    },
  } as const;

  const mockExit = vi
    .spyOn(process, 'exit')
    .mockImplementation(() => void 0 as never);

  afterAll(() => {
    mockExit.mockRestore();
  });

  beforeEach(async () => {
    upDB = await testDBs.create('integration_test_upstream');
    cvrDB = await testDBs.create('integration_test_cvr');
    changeDB = await testDBs.create('integration_test_change');
    replicaDbFile = new DbFile('integration_test_replica');
    replicaDbFile2 = new DbFile('integration_test_replica2');
    zeros = [];
    zerosExited = [];

    await upDB`
      CREATE TABLE foo(
        id TEXT PRIMARY KEY, 
        val TEXT,
        b BOOL,
        j1 JSON,
        j2 JSONB,
        j3 JSON,
        j4 JSON
      );
      INSERT INTO foo(id, val, b, j1, j2, j3, j4) 
        VALUES (
          'bar',
          'baz',
          true,
          '{"foo":"bar"}',
          'true',
          '123',
          '"string"');
    `.simple();

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
      ['ZERO_CHANGE_DB']: getConnectionURI(changeDB),
      ['ZERO_REPLICA_FILE']: replicaDbFile.path,
      ['ZERO_SCHEMA_JSON']: JSON.stringify(SCHEMA),
      ['ZERO_NUM_SYNC_WORKERS']: '1',
    };
  });

  const FOO_QUERY: AST = {
    table: 'foo',
    orderBy: [['id', 'asc']],
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

      const zero = childWorker('./server/multi/main.ts', env);
      zero.onMessageType('ready', onReady);
      zero.on('close', onClose);
      zeros.push(zero);
      await ready;
    }
  }

  afterEach(async () => {
    try {
      zeros.forEach(zero => zero.kill('SIGTERM')); // initiate and await graceful shutdown
      (await Promise.all(zerosExited)).forEach(code => expect(code).toBe(0));
    } finally {
      await testDBs.drop(upDB, cvrDB, changeDB);
      replicaDbFile.delete();
      replicaDbFile2.delete();
    }
  });

  const WATERMARK_REGEX = /[0-9a-z]{4,}/;

  test.each([
    ['single-node standalone', () => [env]],
    [
      'single-node multi-tenant direct-dispatch',
      () => [
        {
          ['ZERO_PORT']: String(port - 3),
          ['ZERO_LOG_LEVEL']: LOG_LEVEL,
          ['ZERO_TENANTS_JSON']: JSON.stringify({
            tenants: [{id: 'tenant', path: '/zero', env}],
          }),
        },
      ],
    ],
    [
      'single-node multi-tenant, double-dispatch',
      () => [
        {
          ['ZERO_PORT']: String(port),
          ['ZERO_LOG_LEVEL']: LOG_LEVEL,
          ['ZERO_TENANTS_JSON']: JSON.stringify({
            tenants: [
              {
                id: 'tenant',
                path: '/zero',
                env: {...env, ['ZERO_PORT']: String(port + 3)},
              },
            ],
          }),
        },
      ],
    ],
    [
      'multi-node standalone',
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
    ],
    [
      'multi-node multi-tenant',
      () => [
        // The replication-manager must be started first for initial-sync
        {
          ['ZERO_PORT']: String(port2),
          ['ZERO_LOG_LEVEL']: LOG_LEVEL,
          ['ZERO_NUM_SYNC_WORKERS']: '0',
          ['ZERO_TENANTS_JSON']: JSON.stringify({
            tenants: [
              {
                id: 'tenant',
                path: '/zero',
                env: {
                  ...env,
                  ['ZERO_PORT']: String(port2 + 3),
                  ['ZERO_NUM_SYNC_WORKERS']: '0',
                },
              },
            ],
          }),
        },
        // startZero() will then copy to replicaDbFile2 for the view-syncer
        {
          ['ZERO_PORT']: String(port),
          ['ZERO_LOG_LEVEL']: LOG_LEVEL,
          ['ZERO_CHANGE_STREAMER_URI']: `http://localhost:${port2 + 1}`,
          ['ZERO_REPLICA_FILE']: replicaDbFile2.path,
          ['ZERO_TENANTS_JSON']: JSON.stringify({
            tenants: [
              {
                id: 'tenant',
                path: '/zero',
                env: {...env, ['ZERO_PORT']: String(port + 3)},
              },
            ],
          }),
        },
      ],
    ],
  ] satisfies [string, () => Envs][])('%s', async (_name, makeEnvs) => {
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
      {pokeID: '00'},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokeEnd',
      {pokeID: '00'},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: '00:01'},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokePart',
      {
        pokeID: '00:01',
        clientsPatch: [{op: 'put', clientID: 'def'}],
        desiredQueriesPatches: {
          def: [{op: 'put', hash: 'query-hash1', ast: FOO_QUERY}],
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
        gotQueriesPatch: [{op: 'put', hash: 'query-hash1', ast: FOO_QUERY}],
        rowsPatch: [
          {
            op: 'put',
            tableName: 'foo',
            value: {
              id: 'bar',
              val: 'baz',
              b: true,
              j1: {foo: 'bar'},
              j2: true,
              j3: 123,
              j4: 'string',
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
    await upDB`
    INSERT INTO foo(id, val, b, j1, j2, j3, j4) 
      VALUES ('voo', 'doo', false, '"foo"', 'false', '456.789', '{"bar":"baz"}')`;

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
              val: 'doo',
              b: false,
              j1: 'foo',
              j2: false,
              j3: 456.789,
              j4: {bar: 'baz'},
            },
          },
        ],
      },
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokeEnd',
      {pokeID: WATERMARK_REGEX},
    ]);

    // Test TRUNCATE
    await upDB`TRUNCATE TABLE foo RESTART IDENTITY`;

    // One canceled poke
    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: WATERMARK_REGEX},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokeEnd',
      {pokeID: WATERMARK_REGEX, cancel: true},
    ]);

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
  });
}, 10000);
