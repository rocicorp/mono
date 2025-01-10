import {resolver} from '@rocicorp/resolver';
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
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.js';
import {getConnectionURI, testDBs} from '../test/db.js';
import {DbFile} from '../test/lite.js';
import type {PostgresDB} from '../types/pg.js';
import {childWorker, type Worker} from '../types/processes.js';

describe('integration', () => {
  let upDB: PostgresDB;
  let cvrDB: PostgresDB;
  let changeDB: PostgresDB;
  let replicaDbFile: DbFile;
  let env: Record<string, string>;
  let port: number;
  let zero: Worker | undefined;
  let zeroExited: Promise<number> | undefined;

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
    zero = undefined;
    zeroExited = undefined;

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

    port = randInt(10000, 16000);

    process.env['SINGLE_PROCESS'] = '1';

    env = {
      ['ZERO_PORT']: String(port),
      ['ZERO_LOG_LEVEL']: 'error',
      ['ZERO_UPSTREAM_DB']: getConnectionURI(upDB),
      ['ZERO_CVR_DB']: getConnectionURI(cvrDB),
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

  async function startZero(module: string, env: NodeJS.ProcessEnv) {
    assert(zero === undefined);
    assert(zeroExited === undefined);
    const {promise: ready, resolve: onReady} = resolver<unknown>();
    const {promise: done, resolve: onClose} = resolver<number>();

    zeroExited = done;
    zero = childWorker(module, env);
    zero.onMessageType('ready', onReady);
    zero.on('close', onClose);
    await ready;
  }

  afterEach(async () => {
    try {
      zero?.kill('SIGTERM'); // initiate and await graceful shutdown
      expect(await zeroExited).toBe(0);
    } finally {
      await testDBs.drop(upDB);
      replicaDbFile.delete();
    }
  });

  test.each([
    ['standalone', './server/multi/main.ts', () => env],
    [
      'multi-tenant, direct-dispatch',
      './server/multi/main.ts',
      () => ({
        ['ZERO_PORT']: String(port - 3),
        ['ZERO_LOG_LEVEL']: 'error',
        ['ZERO_TENANTS_JSON']: JSON.stringify({
          tenants: [{id: 'tenant', path: '/zero', env}],
        }),
      }),
    ],
    [
      'multi-tenant, double-dispatch',
      './server/multi/main.ts',
      () => ({
        ['ZERO_PORT']: String(port),
        ['ZERO_LOG_LEVEL']: 'error',
        ['ZERO_TENANTS_JSON']: JSON.stringify({
          tenants: [
            {
              id: 'tenant',
              path: '/zero',
              env: {...env, ['ZERO_PORT']: String(port + 3)},
            },
          ],
        }),
      }),
    ],
  ])('%s', async (_name, module, makeEnv) => {
    await startZero(module, makeEnv());

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
    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: '00:02'},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokePart',
      {
        pokeID: '00:02',
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
      {pokeID: '00:02'},
    ]);

    // Trigger an upstream change and verify replication.
    await upDB`
    INSERT INTO foo(id, val, b, j1, j2, j3, j4) 
      VALUES ('voo', 'doo', false, '"foo"', 'false', '456.789', '{"bar":"baz"}')`;

    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: expect.any(String)},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokePart',
      {
        pokeID: expect.any(String),
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
      {pokeID: expect.any(String)},
    ]);

    // Test TRUNCATE
    await upDB`TRUNCATE TABLE foo RESTART IDENTITY`;

    // One canceled poke
    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: expect.any(String)},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokeEnd',
      {pokeID: expect.any(String), cancel: true},
    ]);

    expect(await downstream.dequeue()).toMatchObject([
      'pokeStart',
      {pokeID: expect.any(String)},
    ]);
    expect(await downstream.dequeue()).toMatchObject([
      'pokePart',
      {
        pokeID: expect.any(String),
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
      {pokeID: expect.any(String)},
    ]);
  });
}, 10000);
