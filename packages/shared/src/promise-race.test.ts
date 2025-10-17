import {describe, expect, expectTypeOf, test} from 'vitest';
import {promiseRace} from './promise-race.ts';
import {sleep} from './sleep.ts';

describe('promiseRace with array', () => {
  test('returns index of first resolved promise', async () => {
    const result = await promiseRace([sleep(10), sleep(0), sleep(20)]);
    expect(result).toBe(1);
    expectTypeOf(result).toEqualTypeOf<0 | 1 | 2>();
  });

  test('infers large amount of indices', async () => {
    const result = await promiseRace([
      Promise.resolve(0),
      Promise.resolve(1),
      Promise.resolve(2),
      Promise.resolve(3),
      Promise.resolve(4),
      Promise.resolve(5),
      Promise.resolve(6),
      Promise.resolve(7),
      Promise.resolve(8),
      Promise.resolve(9),
      Promise.resolve(10),
      Promise.resolve(11),
      Promise.resolve(12),
      Promise.resolve(13),
      Promise.resolve(14),
      Promise.resolve(15),
      Promise.resolve(16),
      Promise.resolve(17),
      Promise.resolve(18),
      Promise.resolve(19),
      Promise.resolve(20),
    ]);
    expectTypeOf(result).toEqualTypeOf<
      | 0
      | 1
      | 2
      | 3
      | 4
      | 5
      | 6
      | 7
      | 8
      | 9
      | 10
      | 11
      | 12
      | 13
      | 14
      | 15
      | 16
      | 17
      | 18
      | 19
      | 20
    >();
  });

  test('rejects when first promise rejects', async () => {
    const error = new Error('failed');
    const racePromise = promiseRace([
      sleep(0).then(() => {
        throw error;
      }),
      sleep(10),
    ]);
    expectTypeOf(racePromise).toEqualTypeOf<Promise<0 | 1>>();
    await expect(racePromise).rejects.toBe(error);
  });

  test('handles empty array', async () => {
    const promises: Promise<string>[] = [];
    const result = promiseRace(promises);
    expectTypeOf(result).toEqualTypeOf<Promise<number>>();
    await expect(result).rejects.toThrow('No promises to race');
  });

  test('rejects when rejection beats resolution', async () => {
    const error = new Error('fast reject');
    const racePromise = promiseRace([sleep(10), Promise.reject(error)]);
    expectTypeOf(racePromise).toEqualTypeOf<Promise<0 | 1>>();
    await expect(racePromise).rejects.toBe(error);
  });

  test('handles immediately resolved promises', async () => {
    const result = await promiseRace([
      Promise.resolve('first'),
      Promise.resolve('second'),
      sleep(10),
    ]);
    expect(result).toBeOneOf([0, 1]); // Race condition
  });
});

describe('promiseRace with record', () => {
  test('returns key of first resolved promise', async () => {
    const result = await promiseRace({slow: sleep(10), fast: sleep(0)});
    expect(result).toBe('fast');
    expectTypeOf(result).toEqualTypeOf<'fast' | 'slow'>();
  });

  test('rejects when first promise rejects', async () => {
    const error = new Error('failed');
    const racePromise = promiseRace({
      failing: sleep(0).then(() => {
        throw error;
      }),
      succeeding: sleep(10),
    });
    expectTypeOf(racePromise).toEqualTypeOf<Promise<'failing' | 'succeeding'>>();
    await expect(racePromise).rejects.toBe(error);
  });

  test('infers large amount of keys', async () => {
    const result = promiseRace({
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
    expectTypeOf(result).toEqualTypeOf<
      Promise<
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
      >
    >();
    await expect(result).resolves.toBe('foo');
  });

  test('rejects with error for empty record', async () => {
    const result = promiseRace({});
    await expect(result).rejects.toThrow('No promises to race');
    expectTypeOf(result).toEqualTypeOf<Promise<never>>();
  });

  test('rejects when rejection beats resolution', async () => {
    const error = new Error('fast reject');
    const racePromise = promiseRace({
      slow: sleep(10),
      fastReject: Promise.reject(error),
    });
    expectTypeOf(racePromise).toEqualTypeOf<Promise<'slow' | 'fastReject'>>();
    await expect(racePromise).rejects.toBe(error);
  });

  test('handles immediately resolved promises', async () => {
    const result = await promiseRace({
      first: Promise.resolve('value1'),
      second: Promise.resolve('value2'),
      slow: sleep(10),
    });
    expect(result).toBeOneOf(['first', 'second']); // Race condition
  });
});
