import {describe, expect, test} from '@jest/globals';
import * as valita from 'shared/src/valita.js';
import {DurableStorage} from './durable-storage.js';
import type {ListOptions} from './storage.js';
import {randInt} from '../util/rand.js';

describe('list and scan', () => {
  type Case = {
    name: string;
    expected: [string, number][];
    opts?: ListOptions;
  };

  const entries = {
    'user/3': 3,
    'user/1': 1,
    'user/2': 2,

    'product/5': 5,
    'product/4': 4,
    'product/6': 6,
  };

  const cases: Case[] = [
    {
      name: 'prefix option',
      opts: {prefix: 'product/'},
      expected: [
        ['product/4', 4],
        ['product/5', 5],
        ['product/6', 6],
      ],
    },
    {
      name: 'start option inclusive',
      opts: {prefix: 'product/', start: {key: 'product/5'}},
      expected: [
        ['product/5', 5],
        ['product/6', 6],
      ],
    },
    {
      name: 'start option exclusive',
      opts: {
        prefix: 'product/',
        start: {key: 'product/5', exclusive: true},
      },
      expected: [['product/6', 6]],
    },
    {
      name: 'limit option with start',
      opts: {
        limit: 4,
        start: {key: 'product/5'},
      },
      expected: [
        ['product/5', 5],
        ['product/6', 6],
        ['user/1', 1],
        ['user/2', 2],
      ],
    },
    {
      name: 'limit option with start exclusive',
      opts: {
        limit: 4,
        start: {key: 'product/5', exclusive: true},
      },
      expected: [
        ['product/6', 6],
        ['user/1', 1],
        ['user/2', 2],
        ['user/3', 3],
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const {roomDO} = getMiniflareBindings();
      const id = roomDO.newUniqueId();
      const storage = new DurableStorage(
        await getMiniflareDurableObjectStorage(id),
      );

      for (const [k, v] of Object.entries(entries)) {
        await storage.put(k, v);
      }

      const results = [...(await storage.list(c.opts || {}, valita.number()))];
      expect(results).toEqual(c.expected);

      // Test scan()
      const scanResults: [string, number][] = [];
      for await (const entry of storage.scan(c.opts || {}, valita.number())) {
        scanResults.push(entry);
      }
      expect(scanResults).toEqual(c.expected);

      // Test batchScan() with a variety of batch sizes.
      for (const batchSize of [1, 2, 3, 128]) {
        const scanResults: [string, number][] = [];
        for await (const batch of storage.batchScan(
          c.opts || {},
          valita.number(),
          batchSize,
        )) {
          scanResults.push(...batch);
        }
        expect(scanResults).toEqual(c.expected);
      }
    });
  }
});

describe('getEntries', () => {
  for (const num of [0, 10, 128, 129, 300]) {
    test(`get ${num} entries`, async () => {
      const orderedKeys = [...Array(num).keys()].map(key =>
        (100 + key).toString(),
      );
      const entries = new Map(orderedKeys.map(key => [key, `value of ${key}`]));

      const {roomDO} = getMiniflareBindings();
      const id = roomDO.newUniqueId();
      const storage = new DurableStorage(
        await getMiniflareDurableObjectStorage(id),
      );

      for (const [k, v] of entries) {
        await storage.put(k, v);
      }

      const shuffledKeys = orderedKeys
        .map(key => ({key, sort: Math.random()}))
        .sort((a, b) => a.sort - b.sort)
        .map(({key}) => key);

      // Add a couple of non-existent keys that shouldn't affect the results.
      for (let i = 0; i < 10; i++) {
        shuffledKeys.push(randInt(500, 600).toString());
      }

      const gotEntries = await storage.getEntries(
        shuffledKeys,
        valita.string(),
      );

      // Validate order as well as contents.
      expect([...gotEntries]).toEqual([...entries]);
    });
  }
});
