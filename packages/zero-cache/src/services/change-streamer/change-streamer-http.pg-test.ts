import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  beforeEach,
  describe,
  expect,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import WebSocket from 'ws';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {testDBs} from '../../test/db.ts';
import {type PostgresDB} from '../../types/pg.ts';
import {inProcChannel} from '../../types/processes.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {installWebSocketHandoff} from '../dispatcher/websocket-handoff.ts';
import {HttpService} from '../http-service.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {
  ChangeStreamerHttpClient,
  ChangeStreamerHttpServer,
  getSubscriberContext,
} from './change-streamer-http.ts';
import type {Downstream, SubscriberContext} from './change-streamer.ts';
import {PROTOCOL_VERSION} from './change-streamer.ts';
import {setupCDCTables} from './schema/tables.ts';

const SHARD_ID = {
  appID: 'foo',
  shardNum: 123,
} satisfies ShardID;

describe('change-streamer/http', () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let downstream: Subscription<Downstream>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;
  let serverAddress: string;
  let dispatcherAddress: string;
  let connectionClosed: Promise<Downstream[]>;
  let changeStreamerClient: ChangeStreamerHttpClient;

  beforeEach(async () => {
    lc = createSilentLogContext();

    changeDB = await testDBs.create('change_streamer_http_client');
    await changeDB.begin(tx => setupCDCTables(lc, tx, SHARD_ID));
    await changeDB/*sql*/ `
      INSERT INTO ${changeDB(cdcSchema(SHARD_ID))}."replicationState"
        ${changeDB({lastWatermark: '123'})}
    `;
    changeStreamerClient = new ChangeStreamerHttpClient(lc, SHARD_ID, changeDB);

    const {promise, resolve: cleanup} = resolver<Downstream[]>();
    connectionClosed = promise;
    downstream = Subscription.create({cleanup});
    subscribeFn = vi.fn();

    const [parent, receiver] = inProcChannel();

    const config = {} as unknown as ZeroConfig;

    const dispatcher = new HttpService(
      'dispatcher',
      config,
      lc,
      {port: 0},
      fastify => {
        installWebSocketHandoff(
          lc,
          req => ({payload: getSubscriberContext(req), receiver}),
          fastify.server,
        );
      },
    );

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    const server = new ChangeStreamerHttpServer(
      config,
      lc,
      {subscribe: subscribeFn.mockResolvedValue(downstream)},
      {port: 0},
      parent,
    );

    const [dispatcherURL, serverURL] = await Promise.all([
      dispatcher.start(),
      server.start(),
    ]);
    dispatcherAddress = dispatcherURL.substring('http://'.length);
    serverAddress = serverURL.substring('http://'.length);

    return async () => {
      await Promise.all([dispatcher.stop(), server.stop]);
      await testDBs.drop(changeDB);
    };
  });

  async function setChangeStreamerAddress(addr: string) {
    await changeDB/*sql*/ `
      UPDATE ${changeDB(cdcSchema(SHARD_ID))}."replicationState"
        SET "ownerAddress" = ${addr}
    `;
  }

  async function drain<T>(num: number, sub: Source<T>): Promise<T[]> {
    const drained: T[] = [];
    let i = 0;
    for await (const msg of sub) {
      drained.push(msg);
      if (++i === num) {
        break;
      }
    }
    return drained;
  }

  test('health check', async () => {
    let res = await fetch(`http://${serverAddress}/`);
    expect(res.ok).toBe(true);

    res = await fetch(`http://${serverAddress}/?foo=bar`);
    expect(res.ok).toBe(true);
  });

  describe('request bad requests', () => {
    test.each([
      [
        'invalid querystring - missing id',
        `/replication/v${PROTOCOL_VERSION}/changes`,
      ],
      [
        'invalid querystring - missing watermark',
        `/replication/v${PROTOCOL_VERSION}/changes?id=foo&replicaVersion=bar&initial=true`,
      ],
      [
        // Change the error message as necessary
        `Cannot service client at protocol v3. Supported protocols: [v1 ... v2]`,
        `/replication/v${PROTOCOL_VERSION + 1}/changes` +
          `?id=foo&replicaVersion=bar&watermark=123&initial=true`,
      ],
    ])('%s: %s', async (error, path) => {
      for (const address of [serverAddress, dispatcherAddress]) {
        const {promise: result, resolve} = resolver<unknown>();

        const ws = new WebSocket(new URL(path, `http://${address}/`));
        ws.on('close', (_code, reason) => resolve(reason));

        expect(String(await result)).toEqual(`Error: ${error}`);
      }
    });
  });

  test.each([
    ['hostname', () => serverAddress],
    ['websocket handoff', () => dispatcherAddress],
  ])('basic messages streamed over websocket: %s', async (_name, addr) => {
    const ctx = {
      protocolVersion: PROTOCOL_VERSION,
      id: 'foo',
      mode: 'serving',
      replicaVersion: 'abc',
      watermark: '123',
      initial: true,
    } as const;
    await setChangeStreamerAddress(addr());
    const sub = await changeStreamerClient.subscribe(ctx);

    downstream.push(['begin', {tag: 'begin'}, {commitWatermark: '456'}]);
    downstream.push(['commit', {tag: 'commit'}, {watermark: '456'}]);

    expect(await drain(2, sub)).toEqual([
      ['begin', {tag: 'begin'}, {commitWatermark: '456'}],
      ['commit', {tag: 'commit'}, {watermark: '456'}],
    ]);

    // Draining the client-side subscription should cancel it, closing the
    // websocket, which should cancel the server-side subscription.
    expect(await connectionClosed).toEqual([]);

    expect(subscribeFn).toHaveBeenCalledOnce();
    expect(subscribeFn.mock.calls[0][0]).toEqual(ctx);
  });

  test('bigint fields', async () => {
    await setChangeStreamerAddress(serverAddress);
    const sub = await changeStreamerClient.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      id: 'foo',
      mode: 'serving',
      replicaVersion: 'abc',
      watermark: '123',
      initial: true,
    });

    const messages = new ReplicationMessages({issues: 'id'});
    const insert = messages.insert('issues', {
      id: 'foo',
      big1: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      big2: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
      big3: BigInt(Number.MAX_SAFE_INTEGER) + 3n,
    });

    downstream.push(['data', insert]);
    expect(await drain(1, sub)).toMatchInlineSnapshot(`
      [
        [
          "data",
          {
            "new": {
              "big1": 9007199254740992n,
              "big2": 9007199254740993n,
              "big3": 9007199254740994n,
              "id": "foo",
            },
            "relation": {
              "keyColumns": [
                "id",
              ],
              "name": "issues",
              "replicaIdentity": "default",
              "schema": "public",
              "tag": "relation",
            },
            "tag": "insert",
          },
        ],
      ]
    `);
  });
});
