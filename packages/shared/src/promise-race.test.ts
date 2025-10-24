import {describe, expect, expectTypeOf, test} from 'vitest';
import {promiseRace} from './promise-race.ts';
import {sleep} from './sleep.ts';
import {assert} from './asserts.ts';

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
    assert(result.key === 'bar');
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
