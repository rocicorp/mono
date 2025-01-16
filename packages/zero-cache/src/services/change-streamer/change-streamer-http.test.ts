import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import WebSocket from 'ws';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {inProcChannel} from '../../types/processes.js';
import type {Source} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {installWebSocketHandoff} from '../dispatcher/websocket-handoff.js';
import {HttpService} from '../http-service.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {
  ChangeStreamerHttpClient,
  ChangeStreamerHttpServer,
  getSubscriberContext,
} from './change-streamer-http.js';
import type {Downstream, SubscriberContext} from './change-streamer.js';

describe('change-streamer/http', () => {
  let lc: LogContext;
  let downstream: Subscription<Downstream>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;
  let serverURL: string;
  let dispatcherURL: string;
  let server: ChangeStreamerHttpServer;
  let dispatcher: HttpService;
  let connectionClosed: Promise<Downstream[]>;

  beforeEach(async () => {
    lc = createSilentLogContext();

    const {promise, resolve: cleanup} = resolver<Downstream[]>();
    connectionClosed = promise;
    downstream = Subscription.create({cleanup});
    subscribeFn = vi.fn();

    const [parent, receiver] = inProcChannel();

    dispatcher = new HttpService('dispatcher', lc, {port: 0}, fastify => {
      installWebSocketHandoff(
        lc,
        req => ({payload: getSubscriberContext(req), receiver}),
        fastify.server,
      );
    });

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    server = new ChangeStreamerHttpServer(
      lc,
      {subscribe: subscribeFn.mockResolvedValue(downstream)},
      {port: 0},
      parent,
    );

    [dispatcherURL, serverURL] = await Promise.all([
      dispatcher.start(),
      server.start(),
    ]);
  });

  afterEach(async () => {
    await Promise.all([dispatcher.stop(), server.stop]);
  });

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
    let res = await fetch(`${serverURL}/`);
    expect(res.ok).toBe(true);

    res = await fetch(`${serverURL}/?foo=bar`);
    expect(res.ok).toBe(true);
  });

  describe('request bad requests', () => {
    test.each([
      ['invalid querystring - missing id', `/api/replication/v0/changes`],
      [
        'invalid querystring - missing watermark',
        `/api/replication/v0/changes?id=foo&replicaVersion=bar&initial=true`,
      ],
    ])('%s: %s', async (error, path) => {
      for (const baseURL of [serverURL, dispatcherURL]) {
        const {promise: result, resolve} = resolver<unknown>();

        const ws = new WebSocket(new URL(path, baseURL));
        ws.on('close', (_code, reason) => resolve(reason));

        expect(String(await result)).toEqual(`Error: ${error}`);
      }
    });
  });

  test.each([
    ['hostname', () => new ChangeStreamerHttpClient(lc, `${serverURL}`)],
    [
      'hostname with slash',
      () => new ChangeStreamerHttpClient(lc, `${serverURL}/`),
    ],
    [
      'hostname with path',
      () => new ChangeStreamerHttpClient(lc, `${serverURL}/tenant-id`),
    ],
    [
      'hostname with path and trailing slash',
      () => new ChangeStreamerHttpClient(lc, `${serverURL}/foo_bar/`),
    ],
    [
      'websocket handoff hostname',
      () => new ChangeStreamerHttpClient(lc, `${dispatcherURL}`),
    ],
    [
      'websocket handoff hostname with slash',
      () => new ChangeStreamerHttpClient(lc, `${dispatcherURL}/`),
    ],
    [
      'websocket handoff hostname with path',
      () => new ChangeStreamerHttpClient(lc, `${dispatcherURL}/tenant-id`),
    ],
    [
      'websocket handoff hostname with path and trailing slash',
      () => new ChangeStreamerHttpClient(lc, `${dispatcherURL}/foo_bar/`),
    ],
  ])('basic messages streamed over websocket: %s', async (_name, client) => {
    const ctx = {
      id: 'foo',
      mode: 'serving',
      replicaVersion: 'abc',
      watermark: '123',
      initial: true,
    } as const;
    const sub = await client().subscribe(ctx);

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
    const sub = await new ChangeStreamerHttpClient(lc, serverURL).subscribe({
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
