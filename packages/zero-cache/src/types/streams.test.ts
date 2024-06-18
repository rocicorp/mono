import websocket from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {FastifyInstance} from 'fastify';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {Queue} from 'shared/src/queue.js';
import {randInt} from 'shared/src/rand.js';
import * as v from 'shared/src/valita.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import WebSocket from 'ws';
import {CancelableAsyncIterable, streamIn, streamOut} from './streams.js';
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
  let cleanup: Promise<Message[]>;

  let ws: WebSocket;
  let consumer: CancelableAsyncIterable<Message>;

  beforeEach(async () => {
    lc = createSilentLogContext();

    const {promise, resolve} = resolver<Message[]>();
    cleanup = promise;

    consumed = new Queue();
    producer = Subscription.create({
      consumed: m => consumed.enqueue(m),
      coalesce: (curr, prev) => ({
        from: prev.from,
        to: curr.to,
        str: prev.str + curr.str,
      }),
      cleanup: resolve,
    });

    server = Fastify();
    await server.register(websocket);
    server.get('/', {websocket: true}, ws => streamOut(lc, producer, ws));

    // Run the server for real instead of using `injectWS()`, as that has a
    // different behavior for ws.close().
    const port = 3000 + Math.floor(randInt(0, 5000));
    await server.listen({port});
    lc.info?.(`server running on port ${port}`);
    ws = new WebSocket(`http://localhost:${port}/`);

    consumer = streamIn(lc, ws, messageSchema);
  });

  afterEach(async () => {
    expect(ws.readyState).toSatisfy(x => x === ws.CLOSING || x === ws.CLOSED);
    await server.close();
  });

  test('one at a time', async () => {
    let num = 0;

    producer.push({from: num, to: num + 1, str: 'foo'});
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

    expect(await cleanup).toEqual([]);
  });

  test('coalesce and cleanup', async () => {
    producer.push({from: 0, to: 1, str: 'foo'});
    producer.push({from: 1, to: 2, str: 'bar'});
    producer.push({from: 2, to: 3, str: 'baz'});

    let i = 0;
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
    expect(await cleanup).toEqual([{from: 8, to: 10, str: 'voodoo'}]);
  });
});
