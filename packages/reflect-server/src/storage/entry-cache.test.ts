import {describe, expect, test} from '@jest/globals';
import * as valita from 'shared/src/valita.js';
import {DurableStorage} from './durable-storage.js';
import {EntryCache} from './entry-cache.js';
import type {ListOptions} from './storage.js';

describe('entry-cache', () => {
  type Case = {
    name: string;
    pendingKeys: string[];
    pendingKeysBatch: string[];
    deletedKeys: string[];
    deletedKeysBatch: string[];
    expected: [string, string][];
    opts?: ListOptions;
  };

  const durableEntryKeys: string[] = ['bar-1', 'baz-1', 'foo-1'];

  const cases: Case[] = [
    {
      name: 'all entries',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'foo'},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'limit',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {limit: 2},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
      ],
    },
    {
      name: 'prefix, limit',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'foo', limit: 2},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'start',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {start: {key: 'baz-1'}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {start: {key: 'baz-1', exclusive: true}},
      expected: [['foo-1', 'orig-foo-1']],
    },
    {
      name: 'prefix, limit, start, exclusive',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'b', limit: 1, start: {key: 'bar-1', exclusive: true}},
      expected: [['baz-1', 'orig-baz-1']],
    },

    {
      name: 'all entries (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'foo-1'],
      deletedKeys: [],
      deletedKeysBatch: [],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['foo-1', 'new-foo-1'],
      ],
    },
    {
      name: 'prefix (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'ba'},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
    {
      name: 'limit (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {limit: 4},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
      ],
    },
    {
      name: 'prefix, limit (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'baz', limit: 2},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
      ],
    },
    {
      name: 'start (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {start: {key: 'baz-1'}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {start: {key: 'baz-1', exclusive: true}},
      expected: [
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      opts: {prefix: 'b', limit: 3, start: {key: 'bar-1', exclusive: true}},
      expected: [
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
      ],
    },

    {
      name: 'all entries (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      opts: {prefix: 'ba'},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
    {
      name: 'limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5', 'baz-6', 'baz-7'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-7'],
      opts: {limit: 3},
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
    {
      name: 'prefix, limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5', 'baz-6', 'baz-7'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-7'],
      opts: {prefix: 'baz', limit: 2},
      expected: [
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
      ],
    },
    {
      name: 'start (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      opts: {start: {key: 'baz-3'}},
      expected: [
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      opts: {start: {key: 'baz-3', exclusive: true}},
      expected: [
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
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
      expect(await cache.get('foo-1', valita.string())).toEqual('orig-foo-1');
      expect(cache.isDirty()).toBe(false);

      for (const k of c.pendingKeys) {
        await cache.put(k, `new-${k}`);
      }

      const entriesToPut: Record<string, string> = {};
      for (const k of c.pendingKeysBatch) {
        entriesToPut[k] = `new-${k}`;
      }
      await cache.putEntries(entriesToPut);

      expect(cache.isDirty()).toBe(c.pendingKeys.length > 0);

      for (const k of c.deletedKeys) {
        await cache.del(k);
      }
      await cache.delEntries(c.deletedKeysBatch);

      expect(cache.isDirty()).toBe(
        c.pendingKeys.length + c.deletedKeys.length > 0,
      );

      const entries = [...(await cache.list(c.opts || {}, valita.string()))];
      expect(entries).toEqual(c.expected);

      // Test scan()
      const results: [string, string][] = [];
      for await (const entry of cache.scan(c.opts || {}, valita.string())) {
        results.push(entry);
      }
      expect(results).toEqual(c.expected);

      // Test batchScan() with a variety of batch sizes.
      for (const batchSize of [1, 2, 3, 128]) {
        const results: [string, string][] = [];
        for await (const batch of cache.batchScan(
          c.opts || {},
          valita.string(),
          batchSize,
        )) {
          results.push(...batch);
        }
        expect(results).toEqual(c.expected);
      }

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
