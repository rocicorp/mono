import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {Queue} from './queue.ts';

describe('Queue', () => {
  test('dequeues enqueued value', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    q.enqueue('foo');
    expect(q.size()).toBe(1);
    const val = await q.dequeue();
    expect(q.size()).toBe(0);
    expect(val).toBe('foo');
  });

  test('dequeues enqueued rejection', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    q.enqueueRejection('bar');
    expect(q.size()).toBe(1);
    let rejection: unknown;
    try {
      await q.dequeue();
    } catch (error) {
      rejection = error;
    }
    expect(q.size()).toBe(0);
    expect(rejection).toBe('bar');
  });

  test('supports enqueues after dequeue', async () => {
    const q = new Queue<string>();
    const val1 = q.dequeue();
    const val2 = q.dequeue();
    const val3 = q.dequeue();
    expect(q.size()).toBe(0);

    q.enqueue('a');
    q.enqueueRejection('b');
    q.enqueue('c');
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

    q.enqueue('a');
    q.enqueue('b');

    expect(await val1).toBe('a');
    expect(await val3).toBe('b');
    expect(q.size()).toBe(0);
  });

  test('deletes enqueued values', async () => {
    const q = new Queue<string>();
    q.enqueue('b');
    q.enqueue('a');
    q.enqueue('c');
    q.enqueue('b');
    q.enqueue('b');
    q.enqueue('d');
    q.enqueue('b');
    expect(q.size()).toBe(7);

    expect(q.delete('b')).toBe(4);
    expect(q.size()).toBe(3);
    expect(q.delete('b')).toBe(0);
    expect(q.size()).toBe(3);

    expect(await q.dequeue()).toBe('a');
    expect(await q.dequeue()).toBe('c');
    expect(await q.dequeue()).toBe('d');
    expect(q.size()).toBe(0);
  });

  test('supports mixed order', async () => {
    const q = new Queue<string>();
    expect(q.size()).toBe(0);
    q.enqueue('a');
    expect(q.size()).toBe(1);
    const val1 = q.dequeue();
    expect(q.size()).toBe(0);
    const val2 = q.dequeue();
    expect(q.size()).toBe(0);
    q.enqueue('b');
    expect(q.size()).toBe(0);
    q.enqueue('c');
    expect(q.size()).toBe(1);
    const val3 = q.dequeue();
    expect(q.size()).toBe(0);

    expect(await val1).toBe('a');
    expect(await val2).toBe('b');
    expect(await val3).toBe('c');
  });

  test('async iterator cleanup on break', async () => {
    const {promise: cleanedUp, resolve: cleanup} = resolver<void>();
    const q = new Queue<string>();
    q.enqueue('foo');
    q.enqueue('bar');
    q.enqueue('baz');
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
    q.enqueue('foo');
    q.enqueue('bar');
    q.enqueue('baz');
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
    q.enqueue('foo');
    q.enqueue('bar');
    q.enqueueRejection(new Error('bonk'));
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

  test('a consumer blocks until tasks are available', async () => {
    const queue = new Queue<number>();
    const promise = (async () => {
      const head = await queue.dequeue();
      const tasks = queue.drain();
      expect(head).toEqual(1);
      expect(tasks).toEqual([]);
    })();
    queue.enqueue(1);
    await promise;
  });

  test('drain will get all tasks that were accumulated in the prior tick', async () => {
    const queue = new Queue<number>();
    const promise = (async () => {
      const head = await queue.dequeue();
      const tasks = queue.drain();
      expect([head, ...tasks]).toEqual([1, 2, 3]);
    })();
    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    await promise;
  });

  test('a consumer is called if tasks are already available', async () => {
    const queue = new Queue<number>();
    queue.enqueue(1);
    const promise = (async () => {
      const head = await queue.dequeue();
      const tasks = queue.drain();
      expect([head, ...tasks]).toEqual([1]);
    })();
    await promise;
  });

  test('drain will get all tasks that were accumulated in the prior tick | 2', async () => {
    const queue = new Queue<number>();
    queue.enqueue(1);
    const promise = (async () => {
      const head = await queue.dequeue();
      const tasks = queue.drain();
      expect([head, ...tasks]).toEqual([1, 2, 3]);
    })();
    queue.enqueue(2);
    queue.enqueue(3);
    await promise;
  });

  test('large queue drains efficiently (O(n) not O(n^2))', () => {
    // Compare drain times for the same total number of items (k queues of n
    // vs one queue of k*n) rather than asserting an absolute wall-clock
    // bound, which is flaky on slow CI runners. Linear dequeue gives a ratio
    // of ~1; quadratic (e.g. Array.shift()) gives ~k.
    // Prefills `count` queues of `n` items, then times draining them all.
    // Stops early once `budget` ms is exceeded so an O(n^2) regression fails
    // fast instead of hitting the test timeout.
    const drainTime = (count: number, n: number, budget = Infinity) => {
      const queues = Array.from({length: count}, () => {
        const queue = new Queue<number>();
        for (let i = 0; i < n; i++) {
          queue.enqueue(i);
        }
        expect(queue.size()).toBe(n);
        return queue;
      });

      // When items are already enqueued, dequeue() returns T synchronously.
      // This isolates the data structure cost from async/Promise overhead.
      const start = performance.now();
      for (const queue of queues) {
        let sum = 0;
        for (let i = 0; i < n; i++) {
          sum += queue.dequeue() as number;
          if ((i & 0x3ff) === 0 && performance.now() - start > budget) {
            return performance.now() - start;
          }
        }
        // Verify all values were dequeued correctly.
        expect(sum).toBe((n * (n - 1)) / 2);
        expect(queue.size()).toBe(0);
      }
      return performance.now() - start;
    };
    // Average over enough rounds to get past coarse timers (1ms in WebKit)
    // and smooth out JIT/GC noise.
    const minTotalMs = 50;
    const avgTime = (f: () => number) => {
      let total = 0;
      let rounds = 0;
      do {
        total += f();
        rounds++;
      } while (total < minTotalMs);
      return total / rounds;
    };

    const n = 10_000;
    const k = 10;
    const maxRatio = k / 2;
    const small = avgTime(() => drainTime(k, n));
    const large = avgTime(() =>
      drainTime(1, k * n, small * maxRatio + minTotalMs),
    );

    expect(large / small).toBeLessThan(maxRatio);
  });
});
