/* eslint-disable @typescript-eslint/naming-convention */
import {expect, test} from '@jest/globals';
import * as valita from 'shared/valita.js';
import {DurableStorage} from './durable-storage.js';
import type {ListOptions} from './storage.js';

test('list', async () => {
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

  const {roomDO} = getMiniflareBindings();
  const id = roomDO.newUniqueId();
  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id),
  );

  for (const [k, v] of Object.entries(entries)) {
    await storage.put(k, v);
  }

  for (const c of cases) {
    const entries = [...(await storage.list(c.opts || {}, valita.number()))];
    expect(entries).toEqual(c.expected);
  }
});
