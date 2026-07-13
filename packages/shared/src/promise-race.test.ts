import {resolver} from '@rocicorp/resolver';
import {describe, expect, expectTypeOf, test} from 'vitest';
import {assert} from './asserts.ts';
import {getSubscriberCountForTesting, promiseRace} from './promise-race.ts';
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

  test('supports the same promise instance under multiple keys', async () => {
    const shared = sleep(0).then(() => 'x');

    const result = await promiseRace({a: shared, b: shared});

    expect(result).toEqual({key: 'a', status: 'fulfilled', result: 'x'});
  });
});

describe('promiseRace reaction reuse', () => {
  // Regression test: promiseRace used to attach a new `.then` reaction to
  // every promise on every call. A reaction on a pending promise is only
  // released when the promise settles, so racing a memoized long-lived
  // pending promise (as zero-client's run loop does on every server message)
  // grew its reaction list without bound.
  test('attaches at most one reaction to a promise raced repeatedly', async () => {
    const {promise, resolve} = resolver<string>();
    let thenCalls = 0;
    const longLived: PromiseLike<string> = {
      // oxlint-disable-next-line unicorn/no-thenable
      then: (onFulfilled, onRejected) => {
        thenCalls++;
        return promise.then(onFulfilled, onRejected);
      },
    };

    for (let i = 0; i < 25; i++) {
      const result = await promiseRace({
        longLived,
        message: Promise.resolve(i),
      });

      expect(result).toEqual({key: 'message', status: 'fulfilled', result: i});
      // The settled race unsubscribed itself from the pending loser.
      expect(getSubscriberCountForTesting(longLived)).toBe(0);
    }

    expect(thenCalls).toBe(1);

    // The single cached reaction still delivers the eventual settlement.
    resolve('done');
    const late = await promiseRace({longLived, slow: sleep(10)});
    expect(late).toEqual({
      key: 'longLived',
      status: 'fulfilled',
      result: 'done',
    });

    // The settlement is cached and replayed to later races.
    const replay = await promiseRace({longLived, other: Promise.resolve('x')});
    expect(replay).toEqual({
      key: 'longLived',
      status: 'fulfilled',
      result: 'done',
    });
    expect(thenCalls).toBe(1);
  });

  test('unsubscribes settled races from pending losers', async () => {
    const {promise: pendingForever} = resolver<void>();
    const {promise: gate, resolve: openGate} = resolver<string>();

    const race1 = promiseRace({pendingForever, gate});
    const race2 = promiseRace({pendingForever, gate});

    expect(getSubscriberCountForTesting(pendingForever)).toBe(2);

    openGate('go');
    expect(await race1).toEqual({
      key: 'gate',
      status: 'fulfilled',
      result: 'go',
    });
    expect(await race2).toEqual({
      key: 'gate',
      status: 'fulfilled',
      result: 'go',
    });

    expect(getSubscriberCountForTesting(pendingForever)).toBe(0);
    expect(getSubscriberCountForTesting(gate)).toBe(0);
  });

  test('replays a cached rejection to later races', async () => {
    const error = new Error('failed later');
    const {promise, reject} = resolver<void>();

    const first = await promiseRace({
      pending: promise,
      winner: Promise.resolve('w'),
    });
    expect(first.key).toBe('winner');

    reject(error);

    await expect(promiseRace({pending: promise, slow: sleep(10)})).rejects.toBe(
      error,
    );
    // Again, once the settlement is cached.
    await expect(promiseRace({pending: promise, slow: sleep(10)})).rejects.toBe(
      error,
    );
  });
});
