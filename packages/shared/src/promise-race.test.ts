import {resolver} from '@rocicorp/resolver';
import {describe, expect, expectTypeOf, test} from 'vitest';
import {assert} from './asserts.ts';
import {promiseOrAbort, promiseRace} from './promise-race.ts';
import {sleep} from './sleep.ts';

/**
 * Wraps `signal`'s addEventListener/removeEventListener to track the number
 * of 'abort' listeners currently registered, without relying on Node's
 * `node:events.getEventListeners` (which isn't available in browser tests).
 */
function trackAbortListenerCount(signal: AbortSignal): () => number {
  let count = 0;
  const nativeAdd = signal.addEventListener.bind(signal);
  const nativeRemove = signal.removeEventListener.bind(signal);

  signal.addEventListener = ((
    type: string,
    listener: EventListenerOrEventListenerObject,
    options?: boolean | AddEventListenerOptions,
  ) => {
    if (type === 'abort') {
      count++;
    }
    nativeAdd(type, listener, options);
  }) as AbortSignal['addEventListener'];

  signal.removeEventListener = ((
    type: string,
    listener: EventListenerOrEventListenerObject,
    options?: boolean | EventListenerOptions,
  ) => {
    if (type === 'abort') {
      count--;
    }
    nativeRemove(type, listener, options);
  }) as AbortSignal['removeEventListener'];

  return () => count;
}

describe('promiseRace with record', () => {
  test('returns key of first settled promise', async () => {
    const result = await promiseRace({slow: sleep(10), fast: sleep(0)});

    expect(result).toEqual({
      key: 'fast',
      status: 'fulfilled',
      result: undefined,
    });
    expectTypeOf(result.key).toEqualTypeOf<'fast' | 'slow'>();
  });

  test('infers key, status, and result types', async () => {
    const result = await promiseRace({
      foo: sleep(10),
      bar: Promise.resolve('life'),
    });

    expectTypeOf(result.key).toEqualTypeOf<'bar' | 'foo'>();
    expectTypeOf(result.status).toEqualTypeOf<'fulfilled'>();
    expectTypeOf(result.result).toEqualTypeOf<void | string>();
    assert(
      result.key === 'bar',
      () => `Expected result.key to be 'bar', got '${result.key}'`,
    );
    // type narrows to string
    expectTypeOf(result.result).toEqualTypeOf<string>();
  });

  test('lets rejection bubble up', async () => {
    const error = new Error('failed');
    const race = promiseRace({
      failing: sleep(0).then(() => {
        throw error;
      }),
      succeeding: sleep(10),
    });

    await expect(race).rejects.toBe(error);
  });

  test('infers large amount of keys', async () => {
    const result = await promiseRace({
      foo: sleep(0),
      bar: sleep(1),
      baz: sleep(1),
      qux: sleep(1),
      quux: sleep(1),
      corge: sleep(1),
      grault: sleep(1),
      garply: sleep(1),
      waldo: sleep(1),
      fred: sleep(1),
      plugh: sleep(1),
      xyzzy: sleep(1),
      thud: sleep(1),
      spam: sleep(1),
      eggs: sleep(1),
      bacon: sleep(1),
      sausage: sleep(1),
      ham: sleep(1),
      pork: sleep(1),
    });

    expect(result.key).toBe('foo');
    expectTypeOf(result.key).toEqualTypeOf<
      | 'foo'
      | 'bar'
      | 'baz'
      | 'qux'
      | 'quux'
      | 'corge'
      | 'grault'
      | 'garply'
      | 'waldo'
      | 'fred'
      | 'plugh'
      | 'xyzzy'
      | 'thud'
      | 'spam'
      | 'eggs'
      | 'bacon'
      | 'sausage'
      | 'ham'
      | 'pork'
    >();
  });

  test('rejects with error for empty record', async () => {
    const result = promiseRace({});

    await expect(result).rejects.toThrow('No promises to race');
  });

  test('rejecting promise beats resolution', async () => {
    const error = new Error('fast reject');
    const race = promiseRace({
      slow: sleep(10),
      fastReject: Promise.reject(error),
    });

    await expect(race).rejects.toBe(error);
  });

  test('handles immediately resolved promises', async () => {
    const result = await promiseRace({
      first: Promise.resolve('value1'),
      second: Promise.resolve('value2'),
      slow: sleep(10),
    });

    expect(['first', 'second']).toContain(result.key);
    expect(['value1', 'value2']).toContain(result.result);
  });
});

describe('promiseOrAbort', () => {
  test('short-lived promise resolves first', async () => {
    const controller = new AbortController();

    const result = await promiseOrAbort(
      Promise.resolve('value'),
      controller.signal,
    );

    expect(result).toBe('value');
  });

  test('short-lived promise rejects first', async () => {
    const error = new Error('boom');
    const controller = new AbortController();

    const raced = promiseOrAbort(Promise.reject(error), controller.signal);

    await expect(raced).rejects.toBe(error);
  });

  test('abort wins over a never-settling short-lived promise', async () => {
    const controller = new AbortController();
    const never = new Promise<string>(() => {});
    const reason = new Error('aborted');

    const raced = promiseOrAbort(never, controller.signal);
    controller.abort(reason);

    await expect(raced).rejects.toBe(reason);
  });

  test('already-aborted signal rejects immediately', async () => {
    const controller = new AbortController();
    const reason = new Error('already aborted');
    controller.abort(reason);

    const never = new Promise<string>(() => {});
    await expect(promiseOrAbort(never, controller.signal)).rejects.toBe(reason);
  });

  test('any of several long-lived signals can win the race', async () => {
    const a = new AbortController();
    const b = new AbortController();
    const c = new AbortController();
    const reason = new Error('b aborted');
    const never = new Promise<string>(() => {});

    const raced = promiseOrAbort(never, a.signal, b.signal, c.signal);
    b.abort(reason);

    await expect(raced).rejects.toBe(reason);
  });

  test('an already-aborted signal among several rejects immediately', async () => {
    const a = new AbortController();
    const b = new AbortController();
    const reason = new Error('a already aborted');
    a.abort(reason);
    const never = new Promise<string>(() => {});

    await expect(promiseOrAbort(never, a.signal, b.signal)).rejects.toBe(
      reason,
    );
  });

  test('removes the abort listener once the short-lived promise wins', async () => {
    const controller = new AbortController();
    const abortListenerCount = trackAbortListenerCount(controller.signal);
    const {promise, resolve} = resolver<string>();

    const raced = promiseOrAbort(promise, controller.signal);
    resolve('value');
    await raced;

    expect(abortListenerCount()).toBe(0);
  });

  test('removes abort listeners on all signals once one wins', async () => {
    const a = new AbortController();
    const b = new AbortController();
    const c = new AbortController();
    const aCount = trackAbortListenerCount(a.signal);
    const bCount = trackAbortListenerCount(b.signal);
    const cCount = trackAbortListenerCount(c.signal);
    const never = new Promise<string>(() => {});

    const raced = promiseOrAbort(never, a.signal, b.signal, c.signal);
    b.abort(new Error('boom'));
    await raced.catch(() => {});

    expect(aCount()).toBe(0);
    expect(bCount()).toBe(0);
    expect(cCount()).toBe(0);
  });

  test('repeated races do not accumulate abort listeners on the shared signal', async () => {
    const controller = new AbortController();
    const abortListenerCount = trackAbortListenerCount(controller.signal);

    // Each race resolves via the short-lived promise; the abort listener
    // must be removed in the `finally` so listeners don't accumulate on
    // the long-lived signal (the whole point of promiseOrAbort over
    // Promise.race with a long-lived AbortSignal promise, see
    // https://github.com/nodejs/node/issues/17469).
    for (let i = 0; i < 100; i++) {
      const {promise, resolve} = resolver<number>();
      const raced = promiseOrAbort(promise, controller.signal);
      resolve(i);
      expect(await raced).toBe(i);
    }

    expect(abortListenerCount()).toBe(0);

    // The signal still terminates the race cleanly afterwards.
    const never = new Promise<number>(() => {});
    const raced = promiseOrAbort(never, controller.signal);
    const reason = new Error('final abort');
    controller.abort(reason);
    await expect(raced).rejects.toBe(reason);
  });
});
