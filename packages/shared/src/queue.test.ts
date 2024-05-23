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

  test('dequeues timed out value', async () => {
    const q = new Queue<string>();
    const val1 = q.dequeue();
    const val2 = q.dequeue('timed out', 5);
    const val3 = q.dequeue();
    expect(q.size()).toBe(0);

    expect(await val2).toBe('timed out');

    await q.enqueue('a');
    await q.enqueue('b');

    expect(await val1).toBe('a');
    expect(await val3).toBe('b');
    expect(q.size()).toBe(0);
  });

  test('deletes enqueued values', async () => {
    const q = new Queue<string>();
    const c0 = q.enqueue('b');
    const c1 = q.enqueue('a');
    const c2 = q.enqueue('c');
    const c3 = q.enqueue('b');
    const c4 = q.enqueue('b');
    const c5 = q.enqueue('d');
    const c6 = q.enqueue('b');
    expect(q.size()).toBe(7);

    expect(q.delete('b')).toBe(4);
    expect(q.size()).toBe(3);
    expect(q.delete('b')).toBe(0);
    expect(q.size()).toBe(3);

    expect(await q.dequeue()).toBe('a');
    expect(await q.dequeue()).toBe('c');
    expect(await q.dequeue()).toBe('d');
    expect(q.size()).toBe(0);

    await Promise.all([c0, c1, c2, c3, c4, c5, c6]);
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
