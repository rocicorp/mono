import {describe, test, expect} from '@jest/globals';
import {Queue} from './queue.js';

describe('Queue', () => {
  test('dequeues enqueued value', async () => {
    const q = new Queue<string>();
    const consumed = q.enqueue('foo');
    const val = await q.dequeue();
    await consumed;
    expect(val).toBe('foo');
  });

  test('dequeues enqueued rejection', async () => {
    const q = new Queue<string>();
    const consumed = q.enqueueRejection('bar');
    let rejection: unknown;
    try {
      await q.dequeue();
    } catch (error) {
      rejection = error;
    }
    await consumed;
    expect(rejection).toBe('bar');
  });

  test('supports enqueues after dequeue', async () => {
    const q = new Queue<string>();
    const val1 = q.dequeue();
    const val2 = q.dequeue();
    const val3 = q.dequeue();

    await q.enqueue('a');
    await q.enqueueRejection('b');
    await q.enqueue('c');

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
    const consumed1 = q.enqueue('a');
    const val1 = q.dequeue();
    await consumed1;
    const val2 = q.dequeue();
    await q.enqueue('b');
    const consumed3 = q.enqueue('c');
    const val3 = q.dequeue();
    await consumed3;

    expect(await val1).toBe('a');
    expect(await val2).toBe('b');
    expect(await val3).toBe('c');
  });
});
