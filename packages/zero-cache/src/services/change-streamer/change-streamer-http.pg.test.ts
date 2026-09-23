import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify from 'fastify';
import {beforeEach, describe, expect, type MockedFunction, vi} from 'vitest';
import WebSocket from 'ws';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {getConnectionURI, type PgTest, test} from '../../test/db.ts';
import {type PostgresDB} from '../../types/pg.ts';
import {inProcChannel} from '../../types/processes.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import type {PreSerialized, Sized, Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {installWebSocketHandoff} from '../../types/websocket-handoff.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import type {PreSerializedBatch} from './broadcast.ts';
import {
  ChangeStreamerHttpClient,
  ChangeStreamerHttpServer,
} from './change-streamer-http.ts';
import type {SubscriberContext} from './change-streamer.ts';
import {PROTOCOL_VERSION} from './change-streamer.ts';
import {setupCDCTables} from './schema/tables.ts';
import {type SnapshotMessage} from './snapshot.ts';
import {type SubscribeDownstream} from './subscribe.ts';

const SHARD_ID = {
  appID: 'foo',
  shardNum: 123,
} satisfies ShardID;

describe('change-streamer/http', () => {
  let lc: LogContext;
  let changeDB: PostgresDB;
  let downstream: Subscription<string>;
  let snapshotStream: Subscription<SnapshotMessage>;
  // The merged /subscribe handler passes its own `outbound` sink as the second
  // arg; capture it so tests can push changes onto the sink the connection
  // actually serves.
  let capturedSubscribeDownstream:
    | Subscription<string | PreSerializedBatch>
    | undefined;
  let subscribeFn: MockedFunction<
    (
      ctx: SubscriberContext,
      existing?: Subscription<string | PreSerializedBatch>,
    ) => Promise<Source<string | PreSerialized>>
  >;
  let snapshotFn: MockedFunction<
    (id: string) => Promise<Subscription<SnapshotMessage>>
  >;
  let runFn: MockedFunction<() => Promise<void>>;
  let stopFn: MockedFunction<() => Promise<void>>;

  let serverAddress: string;
  let dispatcherAddress: string;
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

    downstream = Subscription.create();
    snapshotStream = Subscription.create();
    subscribeFn = vi.fn();
    snapshotFn = vi.fn();
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
      {port: 0, startupDelayMs: 10000, keepaliveTimeoutMs: undefined},
      parent,
      {
        id: 'change-streamer',
        // Legacy /changes passes no downstream (returns the pre-created one);
        // the merged /subscribe passes its own sink, which we capture and push
        // changes onto, mirroring the real service pushing onto it.
        subscribe: subscribeFn.mockImplementation((_ctx, existing) => {
          if (existing) {
            capturedSubscribeDownstream = existing;
            return Promise.resolve(existing);
          }
          return Promise.resolve(downstream);
        }),
        startSnapshotReservation: snapshotFn.mockResolvedValue(snapshotStream),
        trackBackupWatermark: vi.fn(),
        run: runFn.mockImplementation(() => service.promise),
        stop: stopFn.mockImplementation(() => {
          service.resolve();
          return service.promise;
        }),
      },
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
      {
        port: 0,
        startupDelayMs: 10000,
        keepaliveTimeoutMs: undefined,
      },
      parent,
      {
        id: 'change-streamer',
        subscribe: subscribeFn.mockResolvedValue(downstream),
        startSnapshotReservation: vi.fn(),
        trackBackupWatermark: vi.fn(),
        run: runFn.mockImplementation(() => service.promise),
        stop: stopFn.mockImplementation(() => {
          service.resolve();
          return service.promise;
        }),
      },
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
        'invalid querystring - missing taskID',
        `/replication/v${PROTOCOL_VERSION}/changes?id=foo`,
      ],
      [
        'Missing taskID in snapshot request',
        `/replication/v${PROTOCOL_VERSION}/snapshot`,
      ],
      [
        'invalid querystring - missing watermark',
        `/replication/v${PROTOCOL_VERSION}/changes?id=foo&replicaVersion=bar&initial=true&taskID=foo`,
      ],
      [
        // Change the error message as necessary
        `Cannot service client at protocol v8. Supported protocols: [v4 ... v7]`,
        `/replication/v${PROTOCOL_VERSION + 1}/changes` +
          `?id=foo&replicaVersion=bar&watermark=123&initial=true&id=foo`,
      ],
      [
        // Change the error message as necessary
        `Cannot service client at protocol v8. Supported protocols: [v4 ... v7]`,
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
        wsBatched: true,
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
      // The client's subscribe() now uses the merged /subscribe endpoint in
      // subscribe-only mode: it sends a start-subscription control message and
      // the server serves the change stream over its own `outbound` sink.
      const sub = await client.subscribe(ctx);
      await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalledOnce());
      const serverClosed = resolver<void>();
      must(capturedSubscribeDownstream).addCloseHandler(() =>
        serverClosed.resolve(),
      );

      const begin = JSON.stringify([
        'begin',
        {tag: 'begin'},
        {commitWatermark: '456'},
      ]);
      const commit = JSON.stringify([
        'commit',
        {tag: 'commit'},
        {watermark: '456'},
      ]);
      must(capturedSubscribeDownstream).push(begin);
      must(capturedSubscribeDownstream).push(commit);

      const batchedFrame = `{"id":1,"batch":[${begin},${commit}]}`;
      const batchedSize = Math.round(batchedFrame.length / 2);

      expect(await drain(2, sub)).toEqual([
        {
          data: ['begin', {tag: 'begin'}, {commitWatermark: '456'}],
          size: batchedSize,
        },
        {
          data: ['commit', {tag: 'commit'}, {watermark: '456'}],
          size: batchedSize,
        },
      ]);

      // Draining the client-side subscription should cancel it, closing the
      // websocket, which should cancel the server-side subscription.
      await serverClosed.promise;

      // The server builds the SubscriberContext from the start-subscription
      // message: protocolVersion comes from the path, wsBatched is implicit.
      expect(subscribeFn.mock.calls[0][0]).toEqual(ctx);

      // The ChangeStreamerService should be started when an
      // incoming subscription is received
      expect(runFn).toHaveBeenCalledOnce();
    },
  );

  // Reads an inbound Source into a Queue without cancelling it (breaking a
  // for-await would close the connection).
  function readInto<T>(source: Source<T>): Queue<T> {
    const q = new Queue<T>();
    void (async () => {
      for await (const msg of source) {
        q.enqueue(msg);
      }
    })();
    return q;
  }

  describe('merged v7 subscribe', () => {
    const status = [
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: '148',
        minWatermark: '188',
      },
    ] satisfies SnapshotMessage;

    const ctxPayload = {
      taskID: 'foo-task',
      id: 'foo',
      mode: 'serving',
      replicaVersion: '148',
      watermark: '188',
    } as const;

    const begin = JSON.stringify([
      'begin',
      {tag: 'begin'},
      {commitWatermark: '456'},
    ]);
    const commit = JSON.stringify([
      'commit',
      {tag: 'commit'},
      {watermark: '456'},
    ]);

    test('reserve then start-subscription on one connection', async () => {
      await setChangeStreamerAddress(serverAddress);
      const {instream, outbound} =
        await changeStreamerClient.connect('foo-task');
      const received: Queue<Sized<SubscribeDownstream>> = readInto(instream);

      // Reservation phase.
      outbound.push(['reserve-snapshot', {taskID: 'foo-task'}]);
      await vi.waitFor(() =>
        expect(snapshotFn).toHaveBeenCalledWith('foo-task'),
      );
      snapshotStream.push(status);

      // The reservation status arrives as a distinct 'reserved' message, with
      // the tag rewritten from 'status' to 'snapshot'.
      expect((await received.dequeue()).data).toEqual([
        'reserved',
        {...status[1], tag: 'snapshot'},
      ]);

      // Subscription phase on the SAME connection.
      outbound.push(['start-subscription', ctxPayload]);
      await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalledOnce());
      expect(subscribeFn.mock.calls[0][0]).toEqual({
        protocolVersion: PROTOCOL_VERSION,
        ...ctxPayload,
        wsBatched: true,
      });

      // The subscriber pushes changes directly onto the connection's sink.
      must(capturedSubscribeDownstream).push(begin);
      must(capturedSubscribeDownstream).push(commit);
      expect((await received.dequeue()).data).toEqual([
        'begin',
        {tag: 'begin'},
        {commitWatermark: '456'},
      ]);
      expect((await received.dequeue()).data).toEqual([
        'commit',
        {tag: 'commit'},
        {watermark: '456'},
      ]);

      outbound.cancel();
    });

    test('subscribe-only (reconnect) skips the reservation', async () => {
      await setChangeStreamerAddress(serverAddress);
      const {instream, outbound} =
        await changeStreamerClient.connect('foo-task');
      const received: Queue<Sized<SubscribeDownstream>> = readInto(instream);

      // No reserve-snapshot: go straight to the subscription.
      outbound.push(['start-subscription', ctxPayload]);
      await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalledOnce());
      expect(snapshotFn).not.toHaveBeenCalled();

      must(capturedSubscribeDownstream).push(begin);
      expect((await received.dequeue()).data).toEqual([
        'begin',
        {tag: 'begin'},
        {commitWatermark: '456'},
      ]);

      outbound.cancel();
    });
  });

  test('bigint fields', async () => {
    await setChangeStreamerAddress(serverAddress);
    const sub = await changeStreamerClient.subscribe({
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'foo-task',
      id: 'foo',
      mode: 'serving',
      replicaVersion: 'abc',
      watermark: '123',
    });
    await vi.waitFor(() => expect(subscribeFn).toHaveBeenCalledOnce());

    const messages = new ReplicationMessages({issues: 'id'});
    const insert = messages.insert('issues', {
      id: 'foo',
      big1: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      big2: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
      big3: BigInt(Number.MAX_SAFE_INTEGER) + 3n,
    });

    const json = BigIntJSON.stringify(['data', insert]);
    must(capturedSubscribeDownstream).push(json);
    expect(await drain(1, sub)).toEqual([
      {data: ['data', insert], size: `{"id":1,"msg":${json}}`.length},
    ]);
  });
});
