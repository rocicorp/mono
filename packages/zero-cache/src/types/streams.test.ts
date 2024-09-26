import websocket from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {randInt} from 'shared/src/rand.js';
import {sleep} from 'shared/src/sleep.js';
import * as v from 'shared/src/valita.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import WebSocket from 'ws';
import {type Source, streamIn, streamOut} from './streams.js';
import {Subscription} from './subscription.js';

const messageSchema = v.object({
  from: v.number(),
  to: v.number(),
  str: v.string(),
});

type Message = v.Infer<typeof messageSchema>;

describe('streams', () => {
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

    const consumer = startReceiver();
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

    const consumer = startReceiver() as Subscription<Message>;

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
    const consumer = startReceiver();
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

    const consumer = startReceiver();
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

    const consumer = startReceiver();
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
});
