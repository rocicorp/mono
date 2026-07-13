import {resolver} from '@rocicorp/resolver';
import {describe, expect, expectTypeOf, test} from 'vitest';
import {assert} from './asserts.ts';
import {promiseRace, raceWaiterCountForTesting} from './promise-race.ts';
import {sleep} from './sleep.ts';

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

  test('same promise under multiple keys resolves with first key', async () => {
    const {promise, resolve} = resolver<string>();
    const race = promiseRace({a: promise, b: promise});
    resolve('x');

    expect(await race).toEqual({key: 'a', status: 'fulfilled', result: 'x'});
  });
});

describe('promiseRace does not leak reactions on losing promises', () => {
  test('attaches at most one reaction to a repeatedly raced pending promise', async () => {
    let thenCalls = 0;
    const pending = new Promise<never>(() => undefined);
    const loser: PromiseLike<never> = {
      // oxlint-disable-next-line unicorn/no-thenable
      then: (onFulfilled, onRejected) => {
        thenCalls++;
        return pending.then(onFulfilled, onRejected);
      },
    };

    for (let i = 0; i < 50; i++) {
      const result = await promiseRace({loser, winner: Promise.resolve(i)});
      expect(result).toEqual({key: 'winner', status: 'fulfilled', result: i});
      // The settled race must have detached from the pending loser.
      expect(raceWaiterCountForTesting(loser)).toBe(0);
    }

    expect(thenCalls).toBe(1);
  });

  test('losing races stop waiting on long-lived pending promises', async () => {
    const pending = new Promise<never>(() => undefined);

    expect(raceWaiterCountForTesting(pending)).toBe(0);

    const races = [
      promiseRace({pending, a: sleep(1)}),
      promiseRace({pending, b: sleep(1)}),
      promiseRace({pending, c: sleep(1)}),
    ];
    expect(raceWaiterCountForTesting(pending)).toBe(3);

    await Promise.all(races);
    expect(raceWaiterCountForTesting(pending)).toBe(0);
  });

  test('multiple concurrent races resolve when the shared promise settles', async () => {
    const {promise, resolve} = resolver<number>();
    const races = [
      promiseRace({
        shared: promise,
        never: new Promise<never>(() => undefined),
      }),
      promiseRace({
        other: new Promise<never>(() => undefined),
        shared: promise,
      }),
    ];
    expect(raceWaiterCountForTesting(promise)).toBe(2);

    resolve(42);
    expect(await Promise.all(races)).toEqual([
      {key: 'shared', status: 'fulfilled', result: 42},
      {key: 'shared', status: 'fulfilled', result: 42},
    ]);
    expect(raceWaiterCountForTesting(promise)).toBe(0);
  });

  test('a promise that settled while losing earlier races wins later races', async () => {
    const {promise, resolve} = resolver<string>();

    const first = await promiseRace({p: promise, immediate: sleep(0)});
    expect(first.key).toBe('immediate');

    resolve('done');
    await promise;

    const slow = sleep(20);
    const second = await promiseRace({slow, p: promise});
    expect(second).toEqual({key: 'p', status: 'fulfilled', result: 'done'});
    // An already-settled entry wins without registering any waiters.
    expect(raceWaiterCountForTesting(slow)).toBe(0);
    expect(raceWaiterCountForTesting(promise)).toBe(0);
  });

  test('a promise that rejected while losing earlier races rejects later races', async () => {
    const error = new Error('late reject');
    const {promise, reject} = resolver<string>();

    const first = await promiseRace({p: promise, immediate: sleep(0)});
    expect(first.key).toBe('immediate');

    reject(error);
    await expect(promise).rejects.toBe(error);

    await expect(promiseRace({p: promise, slow: sleep(20)})).rejects.toBe(
      error,
    );
    expect(raceWaiterCountForTesting(promise)).toBe(0);
  });

  test('an entry with an observed settlement wins over an earlier key settled this tick', async () => {
    const {promise: cached, resolve} = resolver<string>();
    await promiseRace({cached, quick: sleep(0)});
    resolve('cached-result');
    await cached;

    // Native Promise.race would resolve with `fresh` (first key, also settled
    // at call time); promiseRace picks the entry whose settlement it has
    // already observed. Documented divergence — see the promiseRace jsdoc.
    const result = await promiseRace({
      fresh: Promise.resolve('fresh-result'),
      cached,
    });
    expect(result).toEqual({
      key: 'cached',
      status: 'fulfilled',
      result: 'cached-result',
    });
  });

  test('losers that reject after the race settles are suppressed as handled', async () => {
    const {promise: cached, resolve} = resolver<string>();
    await promiseRace({cached, quick: sleep(0)});
    resolve('cached-result');
    await cached;

    // `cached` wins via the already-settled fast path, so `lateLoser` never
    // registers a waiter — but it must still be subscribed, otherwise its
    // rejection below would surface as an unhandled rejection and fail the
    // test run.
    const {promise: lateLoser, reject} = resolver<never>();
    const result = await promiseRace({cached, lateLoser});
    expect(result.key).toBe('cached');
    expect(raceWaiterCountForTesting(lateLoser)).toBe(0);

    reject(new Error('late loser rejection'));
    await sleep(10);
  });
});
