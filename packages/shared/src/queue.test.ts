import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {Queue} from './queue.js';

describe('Queue', () => {
  test('dequeues enqueued value', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    const consumed = q.enqueue('foo');
    expect(q.size()).toBe(1);
    const val = await q.dequeue();
    expect(q.size()).toBe(0);
    await consumed;
    expect(val).toBe('foo');
  });

  test('dequeues enqueued rejection', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    const consumed = q.enqueueRejection('bar');
    expect(q.size()).toBe(1);
    let rejection: unknown;
    try {
      await q.dequeue();
    } catch (error) {
      rejection = error;
    }
    expect(q.size()).toBe(0);
    await consumed;
    expect(rejection).toBe('bar');
  });

  test('supports enqueues after dequeue', async () => {
    const q = new Queue<string>();
    const val1 = q.dequeue();
    const val2 = q.dequeue();
    const val3 = q.dequeue();
    expect(q.size()).toBe(0);

    await q.enqueue('a');
    await q.enqueueRejection('b');
    await q.enqueue('c');
    expect(q.size()).toBe(0);

    expect(await val1).toBe('a');
    let rejection: unknown;
    try {
      await val2;
    } catch (error) {
      rejection = error;
    }
    expect(rejection).toBe('b');
    expect(await val3).toBe('c');
  });

  test('supports mixed order', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    const consumed1 = q.enqueue('a');
    expect(q.size()).toBe(1);
    const val1 = q.dequeue();
    expect(q.size()).toBe(0);
    await consumed1;
    const val2 = q.dequeue();
    expect(q.size()).toBe(0);
    await q.enqueue('b');
    expect(q.size()).toBe(0);
    const consumed3 = q.enqueue('c');
    expect(q.size()).toBe(1);
    const val3 = q.dequeue();
    expect(q.size()).toBe(0);
    await consumed3;

    expect(await val1).toBe('a');
    expect(await val2).toBe('b');
    expect(await val3).toBe('c');
  });

  test('async iterator cleanup on break', async () => {
    const {promise: cleanedUp, resolve: cleanup} = resolver<void>();
    const q = new Queue<string>();
    void q.enqueue('foo');
    void q.enqueue('bar');
    void q.enqueue('baz');
    const received = [];
    for await (const snapshot of q.asAsyncIterable(cleanup)) {
      received.push(snapshot);
      if (received.length === 3) {
        break;
      }
    }
    await cleanedUp;
    expect(received).toEqual(['foo', 'bar', 'baz']);
  });

  test('async iterator cleanup on thrown error', async () => {
    const {promise: cleanedUp, resolve: cleanup} = resolver<void>();
    const q = new Queue<string>();
    void q.enqueue('foo');
    void q.enqueue('bar');
    void q.enqueue('baz');
    const received = [];
    let err: unknown;
    try {
      for await (const snapshot of q.asAsyncIterable(cleanup)) {
        received.push(snapshot);
        if (received.length === 3) {
          throw new Error('bonk');
        }
      }
    } catch (e) {
      err = e;
    }
    await cleanedUp;
    expect(received).toEqual(['foo', 'bar', 'baz']);
    expect(String(err)).toBe('Error: bonk');
  });

  test('async iterator cleanup on enqueued rejection error', async () => {
    const {promise: cleanedUp, resolve: cleanup} = resolver<void>();
    const q = new Queue<string>();
    void q.enqueue('foo');
    void q.enqueue('bar');
    void q.enqueueRejection(new Error('bonk'));
    const received = [];
    let err: unknown;
    try {
      for await (const snapshot of q.asAsyncIterable(cleanup)) {
        received.push(snapshot);
      }
    } catch (e) {
      err = e;
    }
    await cleanedUp;
    expect(received).toEqual(['foo', 'bar']);
    expect(String(err)).toBe('Error: bonk');
  });
});
