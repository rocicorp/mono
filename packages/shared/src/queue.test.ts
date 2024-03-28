import {describe, expect, test} from '@jest/globals';
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
});
