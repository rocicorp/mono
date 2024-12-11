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
import {Queue} from '../../../shared/src/queue.js';
import {randInt} from '../../../shared/src/rand.js';
import type {AST} from '../../../zero-protocol/src/ast.js';
import type {InitConnectionMessage} from '../../../zero-protocol/src/connect.js';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.js';
import {getConnectionURI, testDBs} from '../test/db.js';
import {DbFile} from '../test/lite.js';
import type {PostgresDB} from '../types/pg.js';
import {childWorker} from '../types/processes.js';

describe('integration', () => {
  let upDB: PostgresDB;
  let cvrDB: PostgresDB;
  let changeDB: PostgresDB;
  let replicaDbFile: DbFile;
  let env: Record<string, string>;
  let port: number;

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

    await upDB`
      CREATE TABLE foo(id TEXT PRIMARY KEY, val TEXT);
      INSERT INTO foo(id, val) VALUES ('bar', 'baz');
    `.simple();

    port = randInt(10000, 16000);

    process.env['SINGLE_PROCESS'] = '1';

    env = {};
    env['ZERO_PORT'] = String(port);
    env['ZERO_LOG_LEVEL'] = 'error';
    env['ZERO_UPSTREAM_DB'] = getConnectionURI(upDB);
    env['ZERO_CVR_DB'] = getConnectionURI(cvrDB);
    env['ZERO_CHANGE_DB'] = getConnectionURI(changeDB);
    env['ZERO_REPLICA_FILE'] = replicaDbFile.path;
    env['ZERO_SCHEMA_JSON'] = JSON.stringify(SCHEMA);
    env['ZERO_NUM_SYNC_WORKERS'] = '1';
  });

  const FOO_QUERY: AST = {
    table: 'foo',
    orderBy: [['id', 'asc']],
  };

  async function startZero(module: string, env: NodeJS.ProcessEnv) {
    const zeroReady = resolver<unknown>();

    const zero = childWorker(module, env);
    zero.onMessageType('ready', zeroReady.resolve);
    await zeroReady.promise;
  }

  afterEach(async () => {
    await testDBs.drop(upDB);
    replicaDbFile.delete();
  });

  test.each([['standalone', './server/main.ts', () => env]])(
    '%s',
    async (_name, module, makeEnv) => {
      await startZero(module, makeEnv());

      const downstream = new Queue<unknown>();
      const ws = new WebSocket(
        `ws://localhost:${port}/zero/sync/v${PROTOCOL_VERSION}/connect` +
          `?clientGroupID=abc&clientID=def&wsid=123&schemaVersion=1&baseCookie=&ts=123456789&lmid=1`,
        encodeURIComponent(btoa('{}')),
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
            {op: 'put', tableName: 'foo', value: {id: 'bar', val: 'baz'}},
          ],
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: '00:02'},
      ]);

      // Trigger an upstream change and verify replication.
      await upDB`INSERT INTO foo(id, val) VALUES ('voo', 'doo')`;

      expect(await downstream.dequeue()).toMatchObject([
        'pokeStart',
        {pokeID: expect.any(String)},
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokePart',
        {
          pokeID: expect.any(String),
          rowsPatch: [
            {op: 'put', tableName: 'foo', value: {id: 'voo', val: 'doo'}},
          ],
        },
      ]);
      expect(await downstream.dequeue()).toMatchObject([
        'pokeEnd',
        {pokeID: expect.any(String)},
      ]);
    },
  );
});
