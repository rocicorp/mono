import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify from 'fastify';
import {beforeEach, describe, expect, type MockedFunction, vi} from 'vitest';
import WebSocket from 'ws';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {getConnectionURI, type PgTest, test} from '../../test/db.ts';
import {type PostgresDB} from '../../types/pg.ts';
import {inProcChannel} from '../../types/processes.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {installWebSocketHandoff} from '../../types/websocket-handoff.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import type {BackupMonitor} from './backup-monitor.ts';
import {
  ChangeStreamerHttpClient,
  ChangeStreamerHttpServer,
} from './change-streamer-http.ts';
import type {Downstream, SubscriberContext} from './change-streamer.ts';
import {PROTOCOL_VERSION} from './change-streamer.ts';
import {setupCDCTables} from './schema/tables.ts';
import {type SnapshotMessage} from './snapshot.ts';

function createTestConfig(overrides?: Partial<ZeroConfig>) {
  return {
    websocketCompression: false,
    websocketCompressionOptions: undefined,
    ...overrides,
  } as ZeroConfig;
}

const SHARD_ID = {
  appID: 'foo',
  shardNum: 123,
} satisfies ShardID;

describe('change-streamer/http', () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let downstream: Subscription<Downstream>;
  let snapshotStream: Subscription<SnapshotMessage>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;
  let snapshotFn: MockedFunction<(id: string) => Subscription<SnapshotMessage>>;
  let endReservationFn: MockedFunction<(id: string) => void>;
  let runFn: MockedFunction<() => Promise<void>>;
  let stopFn: MockedFunction<() => Promise<void>>;

  let serverAddress: string;
  let dispatcherAddress: string;
  let connectionClosed: Promise<Downstream[]>;
  let changeStreamerClient: ChangeStreamerHttpClient;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();

    changeDB = await testDBs.create('change_streamer_http_client');
    await changeDB.begin(tx => setupCDCTables(lc, tx, SHARD_ID));
    await changeDB /*sql*/ `
      INSERT INTO ${changeDB(cdcSchema(SHARD_ID))}."replicationState"
        ${changeDB({lastWatermark: '123'})}
    `;
    changeStreamerClient = new ChangeStreamerHttpClient(
      lc,
      SHARD_ID,
      getConnectionURI(changeDB),
      undefined,
    );

    const {promise, resolve: cleanup} = resolver<Downstream[]>();
    connectionClosed = promise;
    downstream = Subscription.create({cleanup});
    snapshotStream = Subscription.create();
    subscribeFn = vi.fn();
    snapshotFn = vi.fn();
    endReservationFn = vi.fn();
    runFn = vi.fn();
    stopFn = vi.fn();

    const [parent, sender] = inProcChannel();

    const dispatcher = Fastify();
    installWebSocketHandoff(
      lc,
      req => {
        const {pathname} = new URL(req.url ?? '', 'http://unused/');
        const action = pathname.substring(pathname.lastIndexOf('/') + 1);
        return {payload: action, sender};
      },
      dispatcher.server,
    );

    const service = resolver();

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    const server = new ChangeStreamerHttpServer(
      lc,
      createTestConfig(),
      {port: 0, startupDelayMs: 10000},
      parent,
      {
        id: 'change-streamer',
        subscribe: subscribeFn.mockResolvedValue(downstream),
        run: runFn.mockImplementation(() => service.promise),
        stop: stopFn.mockImplementation(() => {
          service.resolve();
          return service.promise;
        }),
      },
      {
        startSnapshotReservation: snapshotFn.mockReturnValue(snapshotStream),
        endReservation: endReservationFn,
      } as unknown as BackupMonitor,
    );

    const [dispatcherURL, serverURL] = await Promise.all([
      dispatcher.listen(),
      server.start(),
    ]);
    dispatcherAddress = dispatcherURL.substring('http://'.length);
    serverAddress = serverURL.substring('http://'.length);

    return async () => {
      await Promise.all([dispatcher.close(), server.stop]);
      await testDBs.drop(changeDB);
    };
  });

  async function setChangeStreamerAddress(addr: string) {
    await changeDB /*sql*/ `
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

  test('health checks and keepalives', async () => {
    const [parent] = inProcChannel();
    const service = resolver();
    const server = new ChangeStreamerHttpServer(
      lc,
      createTestConfig(),
      {
        port: 0,
        startupDelayMs: 10000,
      },
      parent,
      {
        id: 'change-streamer',
        subscribe: subscribeFn.mockResolvedValue(downstream),
        run: runFn.mockImplementation(() => service.promise),
        stop: stopFn.mockImplementation(() => {
          service.resolve();
          return service.promise;
        }),
      },
      null,
    );
    const baseURL = await server.start();

    let res = await fetch(`${baseURL}/`);
    expect(res.ok).toBe(true);

    res = await fetch(`${baseURL}/?foo=bar`);
    expect(res.ok).toBe(true);

    res = await fetch(`${baseURL}/keepalive`);
    expect(res.ok).toBe(true);

    // The ChangeStreamerService should not yet have been started.
    expect(runFn).not.toHaveBeenCalledOnce();

    void server.stop();
  });

  describe('request bad requests', () => {
    test.each([
      [
        'invalid querystring - missing id',
        `/replication/v${PROTOCOL_VERSION}/changes`,
      ],
      [
        'Missing taskID in snapshot request',
        `/replication/v${PROTOCOL_VERSION}/snapshot`,
      ],
      [
        'invalid querystring - missing watermark',
        `/replication/v${PROTOCOL_VERSION}/changes?id=foo&replicaVersion=bar&initial=true`,
      ],
      [
        // Change the error message as necessary
        `Cannot service client at protocol v5. Supported protocols: [v1 ... v4]`,
        `/replication/v${PROTOCOL_VERSION + 1}/changes` +
          `?id=foo&replicaVersion=bar&watermark=123&initial=true`,
      ],
      [
        // Change the error message as necessary
        `Cannot service client at protocol v5. Supported protocols: [v1 ... v4]`,
        `/replication/v${PROTOCOL_VERSION + 1}/snapshot` +
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
    ['hostname', false, () => serverAddress],
    ['websocket handoff', false, () => dispatcherAddress],
    ['hostname auto-discover', true, () => serverAddress],
    ['websocket handoff auto-discover', true, () => dispatcherAddress],
  ])(
    'snapshot status streamed over websocket: %s',
    async (_name, autoDiscover, addr) => {
      await setChangeStreamerAddress(addr());
      const client = autoDiscover
        ? changeStreamerClient
        : new ChangeStreamerHttpClient(
            lc,
            SHARD_ID,
            getConnectionURI(changeDB),
            `http://${addr()}`,
          );
      const sub = await client.reserveSnapshot('foo-bar-id');

      expect(snapshotFn).toHaveBeenCalledWith('foo-bar-id');

      const status = [
        'status',
        {
          tag: 'status',
          backupURL: 's3://foo/bar',
          replicaVersion: '148',
          minWatermark: '188',
        },
      ] satisfies SnapshotMessage;

      snapshotStream.push(status);

      expect(await drain(1, sub)).toEqual([status]);
    },
  );

  test.each([
    ['hostname', false, () => serverAddress],
    ['websocket handoff', false, () => dispatcherAddress],
    ['hostname auto-discover', true, () => serverAddress],
    ['websocket handoff auto-discover', true, () => dispatcherAddress],
  ])(
    'basic changes streamed over websocket: %s',
    async (_name, autoDiscover, addr) => {
      const ctx = {
        protocolVersion: PROTOCOL_VERSION,
        taskID: 'foo-task',
        id: 'foo',
        mode: 'serving',
        replicaVersion: 'abc',
        watermark: '123',
        initial: true,
      } as const;
      await setChangeStreamerAddress(addr());
      const client = autoDiscover
        ? changeStreamerClient
        : new ChangeStreamerHttpClient(
            lc,
            SHARD_ID,
            getConnectionURI(changeDB),
            `http://${addr()}`,
          );
      const sub = await client.subscribe(ctx);

      expect(endReservationFn).toHaveBeenCalledWith('foo-task');

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

      // The ChangeStreamerService should be started when an
      // incoming subscription is received
      expect(runFn).toHaveBeenCalledOnce();
    },
  );

  test('bigint fields', async () => {
    await setChangeStreamerAddress(serverAddress);
    const sub = await changeStreamerClient.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'foo-task',
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
