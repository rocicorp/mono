import {describe, test, expect} from '@jest/globals';
import {DurableStorage} from './durable-storage.js';
import {EntryCache} from './entry-cache.js';
import * as valita from 'shared/valita.js';
import type {ListOptions} from './storage.js';

describe('entry-cache', () => {
  type Case = {
    name: string;
    pendingKeys: string[];
    deletedKeys: string[];
    expected: [string, string][];
    opts?: ListOptions;
  };

  const durableEntryKeys: string[] = ['bar-1', 'baz-1', 'foo-1'];

  const cases: Case[] = [
    {
      name: 'all entries',
      pendingKeys: [],
      deletedKeys: [],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix',
      pendingKeys: [],
      deletedKeys: [],
      opts: {prefix: 'foo'},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'limit',
      pendingKeys: [],
      deletedKeys: [],
      opts: {limit: 2},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
      ],
    },
    {
      name: 'prefix, limit',
      pendingKeys: [],
      deletedKeys: [],
      opts: {prefix: 'foo', limit: 2},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'start',
      pendingKeys: [],
      deletedKeys: [],
      opts: {start: {key: 'baz-1'}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive',
      pendingKeys: [],
      deletedKeys: [],
      opts: {start: {key: 'baz-1', exclusive: true}},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'prefix, limit, start, exclusive',
      pendingKeys: [],
      deletedKeys: [],
      opts: {prefix: 'b', limit: 1, start: {key: 'bar-1', exclusive: true}},
      expected: [['baz-1', 'orig-baz-1']],
    },

    {
      name: 'all entries (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {prefix: 'ba'},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
      ],
    },
    {
      name: 'limit (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {limit: 3},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
      ],
    },
    {
      name: 'prefix, limit (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {prefix: 'baz', limit: 2},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
      ],
    },
    {
      name: 'start (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {start: {key: 'baz-1'}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {start: {key: 'baz-1', exclusive: true}},
      expected: [
        ['baz-2', 'new-baz-2'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      deletedKeys: [],
      opts: {prefix: 'b', limit: 2, start: {key: 'bar-1', exclusive: true}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
      ],
    },

    {
      name: 'all entries (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      deletedKeys: ['baz-1', 'baz-2'],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {prefix: 'ba'},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
      ],
    },
    {
      name: 'limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {limit: 2},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
      ],
    },
    {
      name: 'prefix, limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3', 'baz-4'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {prefix: 'baz', limit: 2},
      expected: [
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
    {
      name: 'start (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {start: {key: 'baz-3'}},
      expected: [
        ['baz-3', 'new-baz-3'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {start: {key: 'baz-3', exclusive: true}},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3', 'baz-4'],
      deletedKeys: ['baz-1', 'baz-2'],
      opts: {prefix: 'b', limit: 2, start: {key: 'bar-1', exclusive: true}},
      expected: [
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const {roomDO} = getMiniflareBindings();
      const id = roomDO.newUniqueId();
      const durable = new DurableStorage(
        await getMiniflareDurableObjectStorage(id),
      );

      for (const k of durableEntryKeys) {
        await durable.put(k, `orig-${k}`);
      }

      const cache = new EntryCache(durable);

      expect(cache.isDirty()).toBe(false);

      for (const k of c.pendingKeys) {
        await cache.put(k, `new-${k}`);
      }

      expect(cache.isDirty()).toBe(c.pendingKeys.length > 0);

      for (const k of c.deletedKeys) {
        await cache.del(k);
      }

      expect(cache.isDirty()).toBe(
        c.pendingKeys.length + c.deletedKeys.length > 0,
      );

      const entries = [...(await cache.list(c.opts || {}, valita.string()))];

      expect(entries).toEqual(c.expected);

      const durableEntriesBeforeFlush = [
        ...(await durable.list({}, valita.string())),
      ];
      expect(durableEntriesBeforeFlush).toEqual(
        durableEntryKeys.map(k => [k, `orig-${k}`]),
      );

      await cache.flush();
      expect(cache.isDirty()).toBe(false);

      const durableEntriesAfterFlush = [
        ...(await durable.list(c.opts || {}, valita.string())),
      ];

      expect(durableEntriesAfterFlush).toEqual(c.expected);
    });
  }
});
