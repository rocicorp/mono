import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {describe, expect, test, vi} from 'vitest';
import {type Result, Subscription} from './subscription.js';

describe('types/subscription', () => {
  test('cancel', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });
    for (let i = 0; i < 5; i++) {
      const {result} = subscription.push(i);
      results.push(result);
    }

    const received: number[] = [];

    let j = 0;
    for await (const m of subscription) {
      expect(consumed.has(m)).toBe(false);
      for (let i = 0; i < m; i++) {
        expect(consumed.has(i)).toBe(true);
        expect(await results[i]).toBe('consumed');
      }
      received.push(m);
      if (j++ === 2) {
        expect(subscription.active).toBe(true);
        subscription.cancel();
        expect(subscription.active).toBe(false);
      }
    }

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    expect(received).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([3, 4]);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('fail', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });
    for (let i = 0; i < 5; i++) {
      const {result} = subscription.push(i);
      results.push(result);
    }

    const received: number[] = [];
    const failure = new Error('boo');
    let caught;
    let j = 0;
    try {
      for await (const m of subscription) {
        expect(consumed.has(m)).toBe(false);
        for (let i = 0; i < m; i++) {
          expect(consumed.has(i)).toBe(true);
          expect(await results[i]).toBe('consumed');
        }
        received.push(m);
        if (j++ === 2) {
          expect(subscription.active).toBe(true);
          subscription.fail(failure);
          expect(subscription.active).toBe(false);
        }
      }
    } catch (e) {
      caught = e;
    }

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    expect(caught).toBe(failure);
    expect(received).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([3, 4]);
    expect(cleanup.mock.calls[0][1]).toBe(failure);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('iteration break', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });
    for (let i = 0; i < 5; i++) {
      const {result} = subscription.push(i);
      results.push(result);
    }

    const received: number[] = [];
    let j = 0;
    for await (const m of subscription) {
      expect(consumed.has(m)).toBe(false);
      for (let i = 0; i < m; i++) {
        expect(consumed.has(i)).toBe(true);
        expect(await results[i]).toBe('consumed');
      }
      received.push(m);
      if (j++ === 2) {
        expect(subscription.active).toBe(true);
        break;
      }
    }
    expect(subscription.active).toBe(false);

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    expect(received).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([3, 4]);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('iteration throw', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });
    for (let i = 0; i < 5; i++) {
      const {result} = subscription.push(i);
      results.push(result);
    }

    const received: number[] = [];
    const failure = new Error('boo');
    let caught;
    let j = 0;
    try {
      for await (const m of subscription) {
        expect(consumed.has(m)).toBe(false);
        for (let i = 0; i < m; i++) {
          expect(consumed.has(i)).toBe(true);
          expect(await results[i]).toBe('consumed');
        }
        received.push(m);
        if (j++ === 2) {
          expect(subscription.active).toBe(true);
          throw failure;
        }
      }
    } catch (e) {
      expect(subscription.active).toBe(false);
      caught = e;
    }
    expect(subscription.active).toBe(false);

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    expect(caught).toBe(failure);
    expect(received).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([3, 4]);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('pushed while iterating', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });

    // Start the iteration first.
    const received: number[] = [];
    const iteration = (async () => {
      let j = 0;
      for await (const m of subscription) {
        expect(consumed.has(m)).toBe(false);
        for (let i = 0; i < m; i++) {
          expect(consumed.has(i)).toBe(true);
          expect(await results[i]).toBe('consumed');
        }
        received.push(m);
        if (j++ === 2) {
          subscription.cancel();
        }
      }
    })();

    // Now push messages into the subscription.
    for (let i = 0; i < 5; i++) {
      await sleep(2);
      const {result} = subscription.push(i);
      results.push(result);
    }
    await iteration;

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    expect(received).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('coalesce cancel', async () => {
    const consumed = new Set<string>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<string>({
      cleanup,
      consumed: m => consumed.add(m),
      coalesce: (curr, prev) => `${prev},${curr}`,
    });
    results.push(subscription.push('a').result);
    results.push(subscription.push('b').result);

    const received: string[] = [];
    let i = 0;
    for await (const m of subscription) {
      received.push(m);

      if (i++ === 0) {
        expect(consumed.has('a,b')).toBe(false);
        expect(consumed.has('c,d')).toBe(false);
        results.push(subscription.push('c').result);
        results.push(subscription.push('d').result);
      } else {
        expect(consumed.has('a,b')).toBe(true);
        expect(await results[0]).toBe('coalesced');
        expect(await results[1]).toBe('consumed');
        expect(consumed.has('c,d')).toBe(false);
        subscription.cancel();
      }
    }
    expect(await results[2]).toBe('coalesced');
    expect(await results[3]).toBe('consumed');

    expect(received).toEqual(['a,b', 'c,d']);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([]);

    expect(await subscription.push('e').result).toBe('unconsumed');
  });

  test('coalesce break', async () => {
    const consumed = new Set<string>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<string>({
      cleanup,
      consumed: m => consumed.add(m),
      coalesce: (curr, prev) => `${prev},${curr}`,
    });
    results.push(subscription.push('a').result);
    results.push(subscription.push('b').result);

    const received: string[] = [];
    let i = 0;
    for await (const m of subscription) {
      received.push(m);

      if (i++ === 0) {
        expect(consumed.has('a,b')).toBe(false);
        expect(consumed.has('c,d')).toBe(false);
        results.push(subscription.push('c').result);
        results.push(subscription.push('d').result);
      } else {
        expect(consumed.has('a,b')).toBe(true);
        expect(await results[0]).toBe('coalesced');
        expect(await results[1]).toBe('consumed');
        expect(consumed.has('c,d')).toBe(false);
        break;
      }
    }
    expect(await results[2]).toBe('coalesced');
    expect(await results[3]).toBe('consumed');

    expect(received).toEqual(['a,b', 'c,d']);
    expect(consumed).toEqual(new Set(received));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([]);

    expect(await subscription.push('e').result).toBe('unconsumed');
  });

  test('publish different type', async () => {
    type Internal = {foo: number; bar: string};
    type External = {foo: number};

    const consumed = new Set<number>();
    const cleanup = vi.fn();

    const subscription = new Subscription<External, Internal>(
      {
        cleanup,
        consumed: m => consumed.add(m.foo),
      },
      m => ({foo: m.foo}),
    );
    for (let i = 0; i < 5; i++) {
      subscription.push({foo: i, bar: 'internal'});
    }

    const received: External[] = [];

    let j = 0;
    for await (const m of subscription) {
      expect(consumed.has(m.foo)).toBe(false);
      for (let i = 0; i < m.foo; i++) {
        expect(consumed.has(i)).toBe(true);
      }
      received.push(m);
      if (j++ === 2) {
        subscription.cancel();
      }
    }

    expect(received).toEqual([{foo: 0}, {foo: 1}, {foo: 2}]);
    expect(consumed).toEqual(new Set([0, 1, 2]));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([
      {foo: 3, bar: 'internal'},
      {foo: 4, bar: 'internal'},
    ]);
  });

  test('publish: pushed while iterating', async () => {
    type Internal = {foo: number; bar: string};
    type External = {foo: number};

    const consumed = new Set<number>();
    const cleanup = vi.fn();

    const subscription = new Subscription<External, Internal>(
      {
        cleanup,
        consumed: m => consumed.add(m.foo),
      },
      m => ({foo: m.foo}),
    );

    // Start the iteration first.
    const received: External[] = [];
    const iteration = (async () => {
      let j = 0;
      for await (const m of subscription) {
        expect(consumed.has(m.foo)).toBe(false);
        for (let i = 0; i < m.foo; i++) {
          expect(consumed.has(i)).toBe(true);
        }
        received.push(m);
        if (j++ === 2) {
          subscription.cancel();
        }
      }
    })();

    // Now push messages into the subscription.
    for (let i = 0; i < 5; i++) {
      await sleep(2);
      subscription.push({foo: i, bar: 'internal'});
    }
    await iteration;

    expect(received).toEqual([{foo: 0}, {foo: 1}, {foo: 2}]);
    expect(consumed).toEqual(new Set([0, 1, 2]));
    expect(cleanup).toBeCalledTimes(1);
  });

  test('pipelining', async () => {
    const consumed = new Set<number>();
    const cleanup = vi.fn();
    const results: Promise<Result>[] = [];

    const subscription = Subscription.create<number>({
      cleanup,
      consumed: m => consumed.add(m),
    });
    assert(subscription.pipeline);

    for (let i = 0; i < 5; i++) {
      const {result} = subscription.push(i);
      results.push(result);
    }

    const received: {value: number; consumed: () => void}[] = [];
    let j = 0;
    for await (const e of subscription.pipeline) {
      received.push(e);

      if (j++ === 2) {
        for (let i = 0; i < j; i++) {
          expect(consumed.has(i)).toBe(false);
          received[i].consumed();
          expect(consumed.has(i)).toBe(true);
          expect(await results[i]).toBe('consumed');
        }

        expect(subscription.active).toBe(true);
        subscription.cancel();
        expect(subscription.active).toBe(false);
      }
    }

    for (let i = 3; i < 5; i++) {
      expect(await results[i]).toBe('unconsumed');
    }

    const values = received.map(r => r.value);
    expect(values).toEqual([0, 1, 2]);
    expect(consumed).toEqual(new Set(values));
    expect(cleanup).toBeCalledTimes(1);
    expect(cleanup.mock.calls[0][0]).toEqual([3, 4]);

    expect(await subscription.push(6).result).toBe('unconsumed');
  });

  test('pipeline defaults', () => {
    const subNoCoalesce = Subscription.create<string>({});
    expect(subNoCoalesce.pipeline).not.toBeUndefined();

    const subNoPipeline = Subscription.create<string>({pipeline: false});
    expect(subNoPipeline.pipeline).toBeUndefined();

    const subWithCoalesce = Subscription.create<string>({
      coalesce: (curr, prev) => `${prev},${curr}`,
    });
    expect(subWithCoalesce.pipeline).toBeUndefined();

    const subWithCoalesceAndPipeline = Subscription.create<string>({
      coalesce: (curr, prev) => `${prev},${curr}`,
      pipeline: true,
    });
    expect(subWithCoalesceAndPipeline.pipeline).not.toBeUndefined();
  });
});
