import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {getDefaultHighWaterMark} from 'stream';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import WebSocket from 'ws';
import {unreachable} from '../../../shared/src/asserts.ts';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../shared/src/queue.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  type Sink,
  type Source,
  stream,
  streamIn,
  streamOut,
} from './streams.ts';
import {Subscription} from './subscription.ts';

const messageSchema = v.object({
  from: v.number(),
  to: v.number(),
  str: v.string(),
});

type Message = v.Infer<typeof messageSchema>;

describe('streams with flow control', () => {
  let logSink: TestLogSink;
  let lc: LogContext;

  let server: FastifyInstance;
  let serverRequests: Queue<{
    serverIn: Source<Message>;
    serverOut: Sink<Message>;
  }>;
  let ws: WebSocket;
  let wsClosed: Promise<void>;

  beforeEach(async () => {
    logSink = new TestLogSink();
    lc = new LogContext('debug', {}, logSink);

    server = Fastify();
    await server.register(websocket);

    serverRequests = new Queue();
    server.get('/', {websocket: true}, (ws: WebSocket) => {
      const {instream, outstream} = stream<Message, Message>(
        lc,
        ws,
        messageSchema,
      );
      void serverRequests.enqueue({serverIn: instream, serverOut: outstream});
    });
    const url = await server.listen({port: 0});
    lc.info?.(`server running on ${url}`);

    const closed = resolver();
    ws = new WebSocket(url);
    ws.on('close', closed.resolve);
    wsClosed = closed.promise;
  });

  afterEach(async () => {
    await wsClosed;
    await server.close();
  });

  test.each([
    // With a 16k buffer, sending 4 ~8k messages should result in 2 drains.
    [{highWaterMark: 16_384}, 2],
    // With a 64k buffer, sending 4 ~8k messages should not block for any drains.
    [{highWaterMark: 65_536}, 0],
  ])('stream out with back pressure: %o', async (streamOptions, numDrains) => {
    const out = [
      {
        from: 1,
        to: 2,
        str: 'a'.repeat(8192),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        passthrough: true,
      },
      {
        from: 2,
        to: 3,
        str: 'b'.repeat(8192),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
      },
      {
        from: 3,
        to: 4,
        str: 'c'.repeat(8192),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 3n,
      },
      {
        from: 4,
        to: 5,
        str: 'd'.repeat(8192),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 4n,
      },
    ];

    const {outstream} = stream<Message, Message>(
      lc,
      ws,
      messageSchema,
      {},
      {},
      streamOptions,
    );
    // Send a stuff before confirming the server connection.
    for (const msg of out) {
      outstream.push(msg);
    }

    const {serverIn} = await serverRequests.dequeue();
    let i = 0;
    for await (const msg of serverIn) {
      expect(msg).toEqual(out[i++]);
      if (i === out.length) {
        break;
      }
    }

    expect(
      logSink.messages.filter(
        ([level, _ctx, args]) =>
          level === 'debug' && (args[0] as string).match(/drained messages/),
      ),
    ).toHaveLength(numDrains);
  });

  test('stream in', async () => {
    const msgSize = getDefaultHighWaterMark(false) / 2;
    const inMsgs = [
      {
        from: 1,
        to: 2,
        str: 'w'.repeat(msgSize),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        passthrough: true,
      },
      {
        from: 2,
        to: 3,
        str: 'x'.repeat(msgSize),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 2n,
      },
      {
        from: 3,
        to: 4,
        str: 'y'.repeat(msgSize),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 3n,
      },
      {
        from: 4,
        to: 5,
        str: 'z'.repeat(msgSize),
        bigint: BigInt(Number.MAX_SAFE_INTEGER) + 4n,
      },
    ];

    const {serverOut} = await serverRequests.dequeue();

    for (const msg of inMsgs) {
      serverOut.push(msg);
    }

    const {instream} = stream<Message, Message>(lc, ws, messageSchema);
    let i = 0;
    for await (const msg of instream) {
      expect(msg).toEqual(inMsgs[i++]);
      if (i === inMsgs.length) {
        break;
      }
    }

    // Check that back pressure kicked in twice for the four 8K+ messages,
    // as the default watermark is 16k.
    expect(
      logSink.messages.filter(
        ([level, _ctx, args]) =>
          level === 'debug' && (args[0] as string).match(/drained messages/),
      ),
    ).toHaveLength(2);
  });

  test('propagates connection failures', async () => {
    await server.close();

    const {instream} = stream<Message, Message>(lc, ws, messageSchema);

    let result: unknown | undefined;
    try {
      for await (const _ of instream) {
        unreachable();
      }
    } catch (e) {
      result = e;
    }
    expect(String(result)).toMatch(/Error: connect ECONNRE(SET|FUSED)/);
  });
});

describe('streams with internal acks', () => {
  let lc: LogContext;

  let server: FastifyInstance;
  let producer: Subscription<Message>;
  let consumed: Queue<Message>;
  let cleanedUp: Promise<Message[]>;
  let cleanup: (m: Message[]) => void;
  let port: number;

  let ws: WebSocket;

  beforeEach(async () => {
    lc = createSilentLogContext();

    const {promise, resolve} = resolver<Message[]>();
    cleanedUp = promise;
    cleanup = resolve;

    consumed = new Queue();
    producer = Subscription.create({
      consumed: m => consumed.enqueue(m),
      cleanup: resolve,
    });

    server = Fastify();
    await server.register(websocket);
    server.get('/', {websocket: true}, ws => streamOut(lc, producer, ws));

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    port = 3000 + Math.floor(randInt(0, 5000));
    await server.listen({port});
    lc.info?.(`server running on port ${port}`);
  });

  afterEach(async () => {
    expect(ws.readyState).toSatisfy(x => x === ws.CLOSING || x === ws.CLOSED);
    await server.close();
  });

  function startReceiver() {
    ws = new WebSocket(`http://localhost:${port}/`);
    return streamIn(lc, ws, messageSchema);
  }

  test('one at a time', async () => {
    let num = 0;

    producer.push({from: num, to: num + 1, str: 'foo'});

    const consumer = await startReceiver();
    for await (const msg of consumer) {
      if (num > 0) {
        expect(await consumed.dequeue()).toEqual({
          from: num - 1,
          to: num,
          str: 'foo',
        });
      }
      expect(msg).toEqual({from: num, to: num + 1, str: 'foo'});

      if (num === 3) {
        break;
      }
      num++;
      producer.push({from: num, to: num + 1, str: 'foo'});
      expect(consumed.size()).toBe(0);
    }

    expect(await cleanedUp).toEqual([]);
  });

  test('pipelined', async () => {
    producer.push({from: 0, to: 1, str: 'foo'});
    producer.push({from: 1, to: 2, str: 'bar'});
    producer.push({from: 2, to: 3, str: 'baz'});

    const consumer = (await startReceiver()) as Subscription<Message>;

    // Pipelining should send all messages even before they are
    // "consumed" on the receiving end.
    while (consumer.queued < 3) {
      await sleep(1);
    }
    expect(consumed.size()).toBe(0);

    const timedOut = {from: -1, to: -1, str: ''};
    let i = 0;
    for await (const _ of consumer) {
      switch (i++) {
        case 0: {
          expect(await consumed.dequeue(timedOut, 5)).toEqual(timedOut);
          break;
        }
        case 1: {
          expect(await consumed.dequeue()).toEqual({
            from: 0,
            to: 1,
            str: 'foo',
          });
          break;
        }
        case 2: {
          expect(await consumed.dequeue()).toEqual({
            from: 1,
            to: 2,
            str: 'bar',
          });
          break;
        }
      }
      if (i === 3) {
        break;
      }
    }
    expect(await consumed.dequeue()).toEqual({from: 2, to: 3, str: 'baz'});
    expect(await cleanedUp).toEqual([]);
  });

  test('coalesce and cleanup', async () => {
    producer = Subscription.create({
      consumed: m => consumed.enqueue(m),
      coalesce: (curr, prev) => ({
        from: prev.from,
        to: curr.to,
        str: prev.str + curr.str,
      }),
      cleanup,
    });

    producer.push({from: 0, to: 1, str: 'foo'});
    producer.push({from: 1, to: 2, str: 'bar'});
    producer.push({from: 2, to: 3, str: 'baz'});

    let i = 0;
    const consumer = await startReceiver();
    for await (const msg of consumer) {
      switch (i++) {
        case 0:
          expect(msg).toEqual({from: 0, to: 3, str: 'foobarbaz'});
          producer.push({from: 3, to: 4, str: 'foo'});
          producer.push({from: 4, to: 5, str: 'bar'});
          break;
        case 1:
          expect(await consumed.dequeue()).toEqual({
            from: 0,
            to: 3,
            str: 'foobarbaz',
          });
          expect(msg).toEqual({from: 3, to: 5, str: 'foobar'});
          producer.push({from: 5, to: 6, str: 'foo'});
          producer.push({from: 6, to: 7, str: 'boo'});
          producer.push({from: 7, to: 8, str: 'doo'});
          break;
        case 2:
          expect(await consumed.dequeue()).toEqual({
            from: 3,
            to: 5,
            str: 'foobar',
          });
          expect(msg).toEqual({from: 5, to: 8, str: 'fooboodoo'});
          producer.push({from: 8, to: 9, str: 'voo'});
          producer.push({from: 9, to: 10, str: 'doo'});
          ws.terminate(); // Close the websocket abruptly.
          break;
      }
      expect(consumed.size()).toBe(0);
    }

    expect(consumed.size()).toBe(0);
    expect(await cleanedUp).toEqual([{from: 8, to: 10, str: 'voodoo'}]);
  });

  async function drain(
    num: number,
    consumer: Source<Message>,
  ): Promise<Message[]> {
    const drained: Message[] = [];
    let i = 0;
    for await (const msg of consumer) {
      drained.push(msg);
      if (++i === num) {
        break;
      }
    }
    return drained;
  }

  test('passthrough', async () => {
    producer.push({from: 1, to: 2, str: 'foo', extra: 'bar'} as Message);

    const consumer = await startReceiver();
    expect(await drain(1, consumer)).toEqual([
      {from: 1, to: 2, str: 'foo', extra: 'bar'},
    ]);
  });

  test('bigints', async () => {
    producer.push({
      from: 1,
      to: 2,
      str: 'foo',
      extras: [
        Number.MAX_SAFE_INTEGER,
        BigInt(Number.MAX_SAFE_INTEGER) + 1n,
        BigInt(Number.MAX_SAFE_INTEGER) + 2n,
        BigInt(Number.MAX_SAFE_INTEGER) + 3n,
        BigInt(Number.MAX_SAFE_INTEGER) + 4n,
      ],
    } as Message);

    const consumer = await startReceiver();
    expect(await drain(1, consumer)).toEqual([
      {
        from: 1,
        to: 2,
        str: 'foo',
        extras: [
          Number.MAX_SAFE_INTEGER,
          BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          BigInt(Number.MAX_SAFE_INTEGER) + 2n,
          BigInt(Number.MAX_SAFE_INTEGER) + 3n,
          BigInt(Number.MAX_SAFE_INTEGER) + 4n,
        ],
      },
    ]);
  });

  test('propagates connection failures', async () => {
    await server.close();

    let err;
    try {
      await startReceiver();
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(Error);
  });
});
