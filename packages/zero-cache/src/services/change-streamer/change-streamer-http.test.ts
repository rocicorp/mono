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
import {randInt} from '../../../../shared/src/rand.js';
import type {Source} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {
  ChangeStreamerHttpClient,
  ChangeStreamerHttpServer,
} from './change-streamer-http.js';
import type {
  ChangeStreamer,
  Downstream,
  SubscriberContext,
} from './change-streamer.js';

describe('change-streamer/http', () => {
  let lc: LogContext;
  let downstream: Subscription<Downstream>;
  let subscribeFn: MockedFunction<
    (ctx: SubscriberContext) => Promise<Subscription<Downstream>>
  >;
  let port: number;
  let server: ChangeStreamerHttpServer;
  let client: ChangeStreamer;
  let connectionClosed: Promise<Downstream[]>;

  beforeEach(async () => {
    lc = createSilentLogContext();

    const {promise, resolve: cleanup} = resolver<Downstream[]>();
    connectionClosed = promise;
    downstream = Subscription.create({cleanup});
    subscribeFn = vi.fn();

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    port = 3000 + Math.floor(randInt(0, 5000));
    server = new ChangeStreamerHttpServer(
      lc,
      {subscribe: subscribeFn.mockResolvedValue(downstream)},
      {port},
    );
    await server.start();

    client = new ChangeStreamerHttpClient(lc, port);
  });

  afterEach(async () => {
    await server.stop();
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
    let res = await fetch(`http://localhost:${port}/`);
    expect(res.ok).toBe(true);

    res = await fetch(`http://localhost:${port}/?foo=bar`);
    expect(res.ok).toBe(true);
  });

  describe('request bad requests', () => {
    test.each([
      ['no query', `ws://localhost:%PORT%/api/replication/v0/changes`],
      [
        'missing required query params',
        `ws://localhost:%PORT%/api/replication/v0/changes?id=foo&replicaVersion=bar&initial=true`,
      ],
    ])('%s', async (_, url) => {
      url = url.replace('%PORT%', String(port));
      const {promise: result, resolve} = resolver<unknown>();

      const ws = new WebSocket(url);
      ws.on('upgrade', () => resolve('success'));
      ws.on('error', resolve);

      expect(String(await result)).toEqual(
        'Error: Unexpected server response: 400',
      );
    });
  });

  test('basic messages streamed over websocket', async () => {
    const ctx = {
      id: 'foo',
      mode: 'serving',
      replicaVersion: 'abc',
      watermark: '123',
      initial: true,
    } as const;
    const sub = await client.subscribe(ctx);

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

  test('bigint and non-JSON fields', async () => {
    const sub = await client.subscribe({
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
