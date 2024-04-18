import * as valita from 'shared/src/valita.js';
import {expect, test} from 'vitest';
import {runWithDurableObjectStorage} from '../../../test/do.js';
import {delEntry, getEntries, getEntry, listEntries, putEntry} from './data.js';

// Schema that sometimes produces a normalized value.
const numberToString = valita.union(
  valita.string(),
  valita.number().chain(n => valita.ok(String(n))),
);

test('getEntry', async () => {
  type Case = {
    name: string;
    exists: boolean;
    validSchema: boolean;
  };
  const cases: Case[] = [
    {
      name: 'does not exist',
      exists: false,
      validSchema: true,
    },
    {
      name: 'exists, invalid schema',
      exists: true,
      validSchema: false,
    },
    {
      name: 'exists, valid JSON, valid schema',
      exists: true,
      validSchema: true,
    },
  ];

  await runWithDurableObjectStorage(async storage => {
    for (const c of cases) {
      await storage.delete('foo');
      if (c.exists) {
        await storage.put('foo', c.validSchema ? 42 : {});
      }

      const promise = getEntry(storage, 'foo', numberToString, {});
      let result: string | undefined;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let error: any | undefined;
      await promise.then(
        r => (result = r),
        e => (error = String(e)),
      );
      if (!c.exists) {
        expect(result).toBeUndefined();
        expect(result).toBeUndefined();
        expect(error).toBeUndefined();
      } else if (!c.validSchema) {
        expect(result).toBeUndefined();
        expect(String(error)).toMatch(
          'TypeError: Expected string or number. Got object',
        );
      } else {
        expect(result).toEqual('42');
        expect(error).toBeUndefined();
      }
    }
  });
});

test('getEntry RoundTrip types', async () => {
  await runWithDurableObjectStorage(async storage => {
    await putEntry(storage, 'boolean', true, {});
    await putEntry(storage, 'number', 42, {});
    await putEntry(storage, 'string', 'foo', {});
    await putEntry(storage, 'array', [1, 2, 3], {});
    await putEntry(storage, 'object', {a: 1, b: 2}, {});

    expect(await getEntry(storage, 'boolean', valita.boolean(), {})).toEqual(
      true,
    );
    expect(await getEntry(storage, 'number', valita.number(), {})).toEqual(42);
    expect(await getEntry(storage, 'number', numberToString, {})).toEqual('42');
    expect(await getEntry(storage, 'string', valita.string(), {})).toEqual(
      'foo',
    );
    expect(
      await getEntry(storage, 'array', valita.array(valita.number()), {}),
    ).toEqual([1, 2, 3]);
    expect(
      await getEntry(
        storage,
        'object',
        valita.object({a: valita.number(), b: valita.number()}),
        {},
      ),
    ).toEqual({a: 1, b: 2});
  });
});

test('getEntries', async () => {
  await runWithDurableObjectStorage(async storage => {
    await putEntry(storage, 'a', 'b', {});
    await putEntry(storage, 'c', 'is', {});
    await putEntry(storage, 'easy', 'as', {});
    await putEntry(storage, '1', '2', {});
    await putEntry(storage, '3', '!', {});

    const entries = await getEntries(
      storage,
      ['a', 'b', 'c', 'is', 'easy', 'as', '1', '2', '3'],
      valita.string(),
      {},
    );

    // Note: Also verifies that iteration order is sorted in UTF-8.
    expect([...entries]).toEqual([
      ['1', '2'],
      ['3', '!'],
      ['a', 'b'],
      ['c', 'is'],
      ['easy', 'as'],
    ]);
  });
});

test('getEntries schema chaining', async () => {
  await runWithDurableObjectStorage(async storage => {
    await putEntry(storage, 'a', '1', {});
    // Make normalization apparent midway through the Map to verify
    // that the result still follows iteration order.
    await putEntry(storage, 'b', 2, {});
    await putEntry(storage, 'c', '3', {});

    const entries = await getEntries(
      storage,
      ['a', 'b', 'c', 'is', 'easy', 'as', '1', '2', '3'],
      numberToString,
      {},
    );

    // Note: Also verifies that iteration order is sorted in UTF-8.
    expect([...entries]).toEqual([
      ['a', '1'],
      ['b', '2'],
      ['c', '3'],
    ]);
  });
});

test('listEntries', async () => {
  type Case = {
    name: string;
    exists: boolean;
    validSchema: boolean;
  };
  const cases: Case[] = [
    {
      name: 'empty',
      exists: false,
      validSchema: true,
    },
    {
      name: 'exists, invalid schema',
      exists: true,
      validSchema: false,
    },
    {
      name: 'exists, valid JSON, valid schema',
      exists: true,
      validSchema: true,
    },
  ];

  await runWithDurableObjectStorage(async storage => {
    for (const c of cases) {
      await storage.delete('foos/1');
      await storage.delete('foos/2');
      await storage.delete('foos/3');
      if (c.exists) {
        await storage.put('foos/1', c.validSchema ? '11' : {});
        // Make normalization apparent midway through the Map to verify
        // that the result still follows iteration order.
        await storage.put('foos/2', c.validSchema ? 22 : {});
        await storage.put('foos/3', c.validSchema ? '33' : {});
      }

      let result: Map<string, string> | undefined = undefined;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let error: any | undefined;
      try {
        result = await listEntries(storage, numberToString, {prefix: 'foos/'});
      } catch (e) {
        error = e;
      }
      if (!c.exists) {
        expect(result).toBeDefined();
        if (result === undefined) {
          throw new Error('result should be defined');
        }
        expect(result.size).toEqual(0);
      } else if (!c.validSchema) {
        expect(result).toBeUndefined();
        expect(String(error)).toMatch(
          'TypeError: Expected string or number. Got object',
        );
      } else {
        expect(result).toBeDefined();
        if (result === undefined) {
          throw new Error('result should be defined');
        }
        // Note: Also verifies that iteration order is sorted in UTF-8.
        expect([...result]).toEqual([
          ['foos/1', '11'],
          ['foos/2', '22'],
          ['foos/3', '33'],
        ]);
      }
    }
  });
});

test('listEntries ordering', async () => {
  await runWithDurableObjectStorage(async storage => {
    // Use these keys to test collation: Z,𝙕,Ｚ, from
    // https://github.com/rocicorp/compare-utf8/blob/b0b21f235d3227b42e565708647649c160fabacb/src/index.test.js#L63-L71
    await putEntry(storage, 'Z', 1, {});
    await putEntry(storage, '𝙕', 2, {});
    await putEntry(storage, 'Ｚ', 3, {});

    const entriesMap = await listEntries(storage, valita.number(), {});
    const entries = Array.from(entriesMap);

    expect(entries).toEqual([
      ['Z', 1],
      ['Ｚ', 3],
      ['𝙕', 2],
    ]);
  });
});

test('putEntry', async () => {
  await runWithDurableObjectStorage(async storage => {
    type Case = {
      name: string;
      duplicate: boolean;
    };

    const cases: Case[] = [
      {
        name: 'not duplicate',
        duplicate: false,
      },
      {
        name: 'duplicate',
        duplicate: true,
      },
    ];

    for (const c of cases) {
      await storage.delete('foo');

      let res: Promise<void>;
      if (c.duplicate) {
        await putEntry(storage, 'foo', 41, {});
        res = putEntry(storage, 'foo', 42, {});
      } else {
        res = putEntry(storage, 'foo', 42, {});
      }

      await res.catch(() => ({}));

      const value = await storage.get('foo');
      expect(value).toEqual(42);
    }
  });
});

test('delEntry', async () => {
  await runWithDurableObjectStorage(async storage => {
    type Case = {
      name: string;
      exists: boolean;
    };
    const cases: Case[] = [
      {
        name: 'does not exist',
        exists: false,
      },
      {
        name: 'exists',
        exists: true,
      },
    ];

    for (const c of cases) {
      await storage.delete('foo');
      if (c.exists) {
        await storage.put('foo', 42);
      }

      await delEntry(storage, 'foo', {});
      const value = await storage.get('foo');
      expect(value).toBeUndefined();
    }
  });
});
