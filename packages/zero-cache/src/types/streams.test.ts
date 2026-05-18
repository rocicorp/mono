import {getDefaultHighWaterMark} from 'stream';
import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import WebSocket from 'ws';
import {unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/bigint-json.ts';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../shared/src/queue.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  stream,
  streamIn,
  streamInBatches,
  streamOut,
  streamOutStringified,
  type StreamInPayload,
  type Sink,
  type Source,
  type StringifiedStreamPayload,
} from './streams.ts';
import {Subscription, type Result} from './subscription.ts';

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
      serverRequests.enqueue({serverIn: instream, serverOut: outstream});
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
  let stringifiedProducer: Subscription<StringifiedStreamPayload>;
  let consumed: Queue<Message>;
  let cleanedUp: Promise<Message[]>;
  let cleanup: (m: Message[]) => void;
  let ackMessages: number[];
  let streamBatchMessages: number;
  let port: number;

  let ws: WebSocket;

  beforeEach(async () => {
    lc = createSilentLogContext();

    const {promise, resolve} = resolver<Message[]>();
    cleanedUp = promise;
    cleanup = resolve;

    consumed = new Queue();
    ackMessages = [];
    streamBatchMessages = 1;
    producer = Subscription.create({
      consumed: m => consumed.enqueue(m),
      cleanup: resolve,
    });
    stringifiedProducer = Subscription.create();

    server = Fastify();
    await server.register(websocket);
    server.get('/', {websocket: true}, ws => {
      ws.on('message', data => {
        const text = data.toString();
        if (!text.startsWith('{"ack":')) {
          return;
        }
        const {ack} = JSON.parse(text) as {ack?: unknown};
        if (typeof ack === 'number') {
          ackMessages.push(ack);
        }
      });
      const batchOptions =
        streamBatchMessages > 1
          ? {batch: {maxMessages: streamBatchMessages}}
          : undefined;
      void streamOut(lc, producer, ws, batchOptions);
      void streamOutStringified(lc, stringifiedProducer, ws, batchOptions);
    });

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

  async function startReceiver() {
    ws = new WebSocket(`http://localhost:${port}/`);
    return {
      ws,
      consumer: (await streamIn(lc, ws, messageSchema, {
        ack: 'cumulative',
      })) as Subscription<Message>,
    };
  }

  async function startBatchReceiver() {
    ws = new WebSocket(`http://localhost:${port}/`);
    return {
      ws,
      consumer: (await streamInBatches(lc, ws, messageSchema, {
        ack: 'cumulative',
      })) as Subscription<StreamInPayload<Message>>,
    };
  }

  test('one at a time', async () => {
    let num = 0;

    producer.push({from: num, to: num + 1, str: 'foo'});

    const {consumer} = await startReceiver();
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
    const results: Promise<Result>[] = [];
    results.push(producer.push({from: 0, to: 1, str: 'foo'}).result);
    results.push(producer.push({from: 1, to: 2, str: 'bar'}).result);
    results.push(producer.push({from: 2, to: 3, str: 'baz'}).result);

    const {consumer} = await startReceiver();

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
    expect(await Promise.all(results)).toEqual([
      'consumed',
      'consumed',
      'consumed',
    ]);
  });

  test('pipelined cumulative ack batches consumed messages', async () => {
    const messageCount = 96;
    const results: Promise<Result>[] = [];
    for (let i = 0; i < messageCount; i++) {
      results.push(producer.push({from: i, to: i + 1, str: `msg-${i}`}).result);
    }

    const {consumer} = await startReceiver();
    await vi.waitFor(() => expect(consumer.queued).toBe(messageCount));

    const entries = await drainPipeline(messageCount, consumer);
    for (const {consumed} of entries) {
      consumed();
    }

    expect(await Promise.all(results)).toEqual(
      Array<Result>(messageCount).fill('consumed'),
    );
    expect(ackMessages).toEqual([96]);
    consumer.cancel();
    expect(await cleanedUp).toEqual([]);
  });

  test('pipelined outbound stream batches queued messages', async () => {
    streamBatchMessages = 4;
    const messageCount = 10;
    for (let i = 0; i < messageCount; i++) {
      producer.push({from: i, to: i + 1, str: `msg-${i}`});
    }

    const {consumer} = await startReceiver();
    await vi.waitFor(() => expect(consumer.queued).toBe(messageCount));

    const received: Message[] = [];
    for await (const msg of consumer) {
      received.push(msg);
      if (received.length === messageCount) {
        break;
      }
    }

    expect(received).toEqual(
      Array.from({length: messageCount}, (_, i) => ({
        from: i,
        to: i + 1,
        str: `msg-${i}`,
      })),
    );
    expect(await cleanedUp).toEqual([]);
  });

  test('pipelined outbound stream uses batch frames', async () => {
    streamBatchMessages = 4;
    const received: JSONValue[] = [];
    const messageCount = 10;

    ws = new WebSocket(`http://localhost:${port}/`);
    ws.on('message', data =>
      received.push(JSON.parse(data.toString()) as JSONValue),
    );

    for (let i = 0; i < messageCount; i++) {
      producer.push({from: i, to: i + 1, str: `msg-${i}`});
    }

    await vi.waitFor(() => expect(received).toHaveLength(3));
    expect(received).toMatchObject([
      {batch: [{id: 1}, {id: 2}, {id: 3}, {id: 4}]},
      {batch: [{id: 5}, {id: 6}, {id: 7}, {id: 8}]},
      {batch: [{id: 9}, {id: 10}]},
    ]);
    ws.close();
  });

  test('streamInBatches preserves received batch frames', async () => {
    streamBatchMessages = 4;
    const messageCount = 6;
    for (let i = 0; i < messageCount; i++) {
      producer.push({from: i, to: i + 1, str: `msg-${i}`});
    }

    const {consumer} = await startBatchReceiver();

    const received: StreamInPayload<Message>[] = [];
    for await (const payload of consumer) {
      received.push(payload);
      if (
        received.reduce(
          (sum, payload) =>
            sum + ('messages' in payload ? payload.messages.length : 1),
          0,
        ) === messageCount
      ) {
        break;
      }
    }

    expect(received).toEqual([
      {
        tag: 'stream-batch',
        messages: [
          {from: 0, to: 1, str: 'msg-0'},
          {from: 1, to: 2, str: 'msg-1'},
          {from: 2, to: 3, str: 'msg-2'},
          {from: 3, to: 4, str: 'msg-3'},
        ],
      },
      {
        tag: 'stream-batch',
        messages: [
          {from: 4, to: 5, str: 'msg-4'},
          {from: 5, to: 6, str: 'msg-5'},
        ],
      },
    ]);
    await vi.waitFor(() => expect(ackMessages).toEqual([4, 6]));
    expect(await cleanedUp).toEqual([]);
  });

  test('pipelined cumulative ack flushes pending ack on cleanup', async () => {
    const messageCount = 3;
    const results: Promise<Result>[] = [];
    for (let i = 0; i < messageCount; i++) {
      results.push(producer.push({from: i, to: i + 1, str: `msg-${i}`}).result);
    }

    const {consumer} = await startReceiver();
    await vi.waitFor(() => expect(consumer.queued).toBe(messageCount));

    const entries = await drainPipeline(messageCount, consumer);
    for (const {consumed} of entries) {
      consumed();
    }
    expect(ackMessages).toEqual([]);

    consumer.cancel();

    expect(await Promise.all(results)).toEqual(
      Array<Result>(messageCount).fill('consumed'),
    );
    expect(ackMessages).toEqual([3]);
    expect(await cleanedUp).toEqual([]);
  });

  test('pipelined backwards ack closes connection', async () => {
    const received: JSONValue[] = [];
    const closed = resolver<void>();
    const results: Promise<Result>[] = [];

    ws = new WebSocket(`http://localhost:${port}/`);
    ws.on('message', data =>
      received.push(JSON.parse(data.toString()) as JSONValue),
    );
    ws.on('close', () => closed.resolve());

    results.push(producer.push({from: 0, to: 1, str: 'foo'}).result);
    results.push(producer.push({from: 1, to: 2, str: 'bar'}).result);

    await vi.waitFor(() => expect(received).toHaveLength(2));
    ws.send(JSON.stringify({ack: 2}));
    expect(await Promise.all(results)).toEqual(['consumed', 'consumed']);

    ws.send(JSON.stringify({ack: 1}));
    await closed.promise;
  });

  test('pipelined (unconsumed)', async () => {
    const results: Promise<Result>[] = [];
    results.push(producer.push({from: 0, to: 1, str: 'foo'}).result);
    results.push(producer.push({from: 1, to: 2, str: 'bar'}).result);
    results.push(producer.push({from: 2, to: 3, str: 'baz'}).result);

    const {consumer, ws} = await startReceiver();

    // Pipelining should send all messages even before they are
    // "consumed" on the receiving end.
    while (consumer.queued < 3) {
      await sleep(1);
    }
    expect(consumed.size()).toBe(0);

    // Terminate the websocket ungracefully.
    ws.terminate();

    expect(consumed.size()).toBe(0);
    expect(await cleanedUp).toEqual([
      {from: 0, str: 'foo', to: 1},
      {from: 1, str: 'bar', to: 2},
      {from: 2, str: 'baz', to: 3},
    ]);
    expect(await Promise.all(results)).toEqual([
      'unconsumed',
      'unconsumed',
      'unconsumed',
    ]);
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

    // oxlint-disable-next-line no-unused-vars -- Used in switch statement increment
    let i = 0;
    const {consumer} = await startReceiver();
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
        case 3:
          expect(await consumed.dequeue()).toEqual({
            from: 5,
            to: 8,
            str: 'fooboodoo',
          });
          expect(msg).toEqual({from: 8, to: 10, str: 'voodoo'});
          break;
      }
      expect(consumed.size()).toBe(0);
    }

    expect(consumed.size()).toBe(0);
    // In this case, the producer does not get the ack that the last messages
    // were consumed
    expect(await cleanedUp).toEqual([
      {from: 5, to: 8, str: 'fooboodoo'},
      {from: 8, to: 10, str: 'voodoo'},
    ]);
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

  async function drainPipeline(
    num: number,
    consumer: Source<Message>,
  ): Promise<{value: Message; consumed: () => void}[]> {
    const {pipeline} = consumer;
    expect(pipeline).not.toBeUndefined();
    if (!pipeline) {
      unreachable();
    }
    const iterator = pipeline[Symbol.asyncIterator]();
    const drained: {value: Message; consumed: () => void}[] = [];
    for (let i = 0; i < num; i++) {
      const result = await iterator.next();
      expect(result.done).not.toBe(true);
      if (result.done) {
        unreachable();
      }
      drained.push(result.value);
    }
    return drained;
  }

  test('passthrough', async () => {
    producer.push({from: 1, to: 2, str: 'foo', extra: 'bar'} as Message);

    const {consumer} = await startReceiver();
    expect(await drain(1, consumer)).toEqual([
      {from: 1, to: 2, str: 'foo', extra: 'bar'},
    ]);
  });

  test('stringified source', async () => {
    stringifiedProducer.push('{"from":1,"to":2,"str":"foo","extra":"bar"}');

    const {consumer} = await startReceiver();
    expect(await drain(1, consumer)).toEqual([
      {from: 1, to: 2, str: 'foo', extra: 'bar'},
    ]);
  });

  test('stringified source flattens internal message batches', async () => {
    const results = [
      stringifiedProducer.push('{"from":1,"to":2,"str":"one"}').result,
      stringifiedProducer.push([
        '{"from":2,"to":3,"str":"two"}',
        '{"from":3,"to":4,"str":"three"}',
      ]).result,
    ];

    const {consumer} = await startReceiver();
    expect(await drain(3, consumer)).toEqual([
      {from: 1, to: 2, str: 'one'},
      {from: 2, to: 3, str: 'two'},
      {from: 3, to: 4, str: 'three'},
    ]);
    expect(await Promise.all(results)).toEqual(['consumed', 'consumed']);
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

    const {consumer} = await startReceiver();
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

  test('unconsumed array receives pending messages after close', async () => {
    const {consumer} = await startReceiver();

    for (let i = 0; i < 100; i++) {
      producer.push({from: i, to: i + 1, str: 'foo' + 1});
    }

    await vi.waitFor(() => expect(consumer.queued).toBe(100));
    producer.cancel(); // Closes the websocket
    await sleep(10);

    let i = 0;
    for await (const _ of consumer) {
      i++;
    }
    expect(i).toBe(100);
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
