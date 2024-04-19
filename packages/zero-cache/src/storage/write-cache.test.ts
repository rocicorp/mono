import type {Patch} from 'reflect-protocol';
import * as valita from 'shared/src/valita.js';
import {describe, expect, test} from 'vitest';
import {runWithDurableObjectStorage} from '../test/do.js';
import {DurableStorage} from './durable-storage.js';
import type {ListOptions} from './storage.js';
import {EntryCache} from './write-cache.js';

type Case = {
  name: string;
  pendingKeys: string[];
  pendingKeysBatch: string[];
  deletedKeys: string[];
  deletedKeysBatch: string[];
  listAndScanOpts?: {
    opts: ListOptions;
    expected: [string, string][];
  };
  expected: [string, string][];
  expectedPending: Patch;
};
describe('entry-cache', () => {
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
      expectedPending: [],
    },
    {
      name: 'prefix',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {prefix: 'foo'},
        expected: [['foo-1', 'orig-foo-1']],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
    },
    {
      name: 'limit',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {limit: 2},
        expected: [
          ['bar-1', 'orig-bar-1'],
          ['baz-1', 'orig-baz-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
    },
    {
      name: 'prefix, limit',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {prefix: 'foo', limit: 2},
        expected: [['foo-1', 'orig-foo-1']],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
    },
    {
      name: 'start',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {start: {key: 'baz-1'}},
        expected: [
          ['baz-1', 'orig-baz-1'],
          ['foo-1', 'orig-foo-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
    },
    {
      name: 'start, exclusive',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {start: {key: 'baz-1', exclusive: true}},
        expected: [['foo-1', 'orig-foo-1']],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
    },
    {
      name: 'prefix, limit, start, exclusive',
      pendingKeys: [],
      pendingKeysBatch: [],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {
          prefix: 'b',
          limit: 1,
          start: {key: 'bar-1', exclusive: true},
        },
        expected: [['baz-1', 'orig-baz-1']],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [],
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
      expectedPending: [
        {op: 'put', key: 'foo-1', value: 'new-foo-1'},
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
      ],
    },
    {
      name: 'prefix (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {prefix: 'ba'},
        expected: [
          ['bar-1', 'orig-bar-1'],
          ['baz-1', 'orig-baz-1'],
          ['baz-2', 'new-baz-2'],
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
      ],
    },
    {
      name: 'limit (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {limit: 4},
        expected: [
          ['bar-1', 'orig-bar-1'],
          ['baz-1', 'orig-baz-1'],
          ['baz-2', 'new-baz-2'],
          ['baz-3', 'new-baz-3'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
      ],
    },
    {
      name: 'prefix, limit (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {prefix: 'baz', limit: 2},
        expected: [
          ['baz-1', 'orig-baz-1'],
          ['baz-2', 'new-baz-2'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
      ],
    },
    {
      name: 'start (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {start: {key: 'baz-1'}},
        expected: [
          ['baz-1', 'orig-baz-1'],
          ['baz-2', 'new-baz-2'],
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
          ['foo-1', 'orig-foo-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
      ],
    },
    {
      name: 'start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {start: {key: 'baz-1', exclusive: true}},
        expected: [
          ['baz-2', 'new-baz-2'],
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
          ['foo-1', 'orig-foo-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
      ],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts)',
      pendingKeys: ['baz-2'],
      pendingKeysBatch: ['baz-3', 'baz-4'],
      deletedKeys: [],
      deletedKeysBatch: [],
      listAndScanOpts: {
        opts: {
          prefix: 'b',
          limit: 3,
          start: {key: 'bar-1', exclusive: true},
        },
        expected: [
          ['baz-1', 'orig-baz-1'],
          ['baz-2', 'new-baz-2'],
          ['baz-3', 'new-baz-3'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-1', 'orig-baz-1'],
        ['baz-2', 'new-baz-2'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'put', key: 'baz-2', value: 'new-baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
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
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'del', key: 'baz-1'},
        {op: 'del', key: 'baz-6'},
      ],
    },
    {
      name: 'prefix (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      listAndScanOpts: {
        opts: {prefix: 'ba'},
        expected: [
          ['bar-1', 'orig-bar-1'],
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'del', key: 'baz-1'},
        {op: 'del', key: 'baz-6'},
      ],
    },
    {
      name: 'limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5', 'baz-6', 'baz-7'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-7'],
      listAndScanOpts: {
        opts: {limit: 3},
        expected: [
          ['bar-1', 'orig-bar-1'],
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['baz-6', 'new-baz-6'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'put', key: 'baz-6', value: 'new-baz-6'},
        {op: 'del', key: 'baz-7'},
        {op: 'del', key: 'baz-1'},
      ],
    },
    {
      name: 'prefix, limit (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5', 'baz-6', 'baz-7'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-7'],
      listAndScanOpts: {
        opts: {prefix: 'baz', limit: 2},
        expected: [
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['baz-6', 'new-baz-6'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'put', key: 'baz-6', value: 'new-baz-6'},
        {op: 'del', key: 'baz-7'},
        {op: 'del', key: 'baz-1'},
      ],
    },
    {
      name: 'start (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      listAndScanOpts: {
        opts: {start: {key: 'baz-3'}},
        expected: [
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
          ['foo-1', 'orig-foo-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'del', key: 'baz-1'},
        {op: 'del', key: 'baz-6'},
      ],
    },
    {
      name: 'start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      listAndScanOpts: {
        opts: {start: {key: 'baz-3', exclusive: true}},
        expected: [
          ['baz-4', 'new-baz-4'],
          ['foo-1', 'orig-foo-1'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'del', key: 'baz-1'},
        {op: 'del', key: 'baz-6'},
      ],
    },
    {
      name: 'prefix, limit, start, exclusive (with pending puts and dels)',
      pendingKeys: ['baz-2', 'baz-3'],
      pendingKeysBatch: ['baz-4', 'baz-5'],
      deletedKeys: ['baz-1', 'baz-2'],
      deletedKeysBatch: ['baz-5', 'baz-6'],
      listAndScanOpts: {
        opts: {
          prefix: 'b',
          limit: 2,
          start: {key: 'bar-1', exclusive: true},
        },
        expected: [
          ['baz-3', 'new-baz-3'],
          ['baz-4', 'new-baz-4'],
        ],
      },
      expected: [
        ['bar-1', 'orig-bar-1'],
        ['baz-3', 'new-baz-3'],
        ['baz-4', 'new-baz-4'],
        ['foo-1', 'orig-foo-1'],
      ],
      expectedPending: [
        {op: 'del', key: 'baz-2'},
        {op: 'put', key: 'baz-3', value: 'new-baz-3'},
        {op: 'put', key: 'baz-4', value: 'new-baz-4'},
        {op: 'del', key: 'baz-5'},
        {op: 'del', key: 'baz-1'},
        {op: 'del', key: 'baz-6'},
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await runWithDurableObjectStorage(async storage => {
        const durable = new DurableStorage(storage);

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

        const entries = [...(await cache.list({}, valita.string()))];
        expect(entries).toEqual(c.expected);
        if (c.listAndScanOpts) {
          const entries = [
            ...(await cache.list(c.listAndScanOpts.opts, valita.string())),
          ];
          expect(entries).toEqual(c.listAndScanOpts.expected);
        }

        // Test scan()
        const results: [string, string][] = [];
        for await (const entry of cache.scan({}, valita.string())) {
          results.push(entry);
        }
        expect(results).toEqual(c.expected);

        if (c.listAndScanOpts) {
          const results: [string, string][] = [];
          for await (const entry of cache.scan(
            c.listAndScanOpts.opts,
            valita.string(),
          )) {
            results.push(entry);
          }
          expect(results).toEqual(c.listAndScanOpts.expected);
        }

        // Test batchScan() with a variety of batch sizes.
        for (const batchSize of [1, 2, 3, 128]) {
          const results: [string, string][] = [];
          for await (const batch of cache.batchScan(
            {},
            valita.string(),
            batchSize,
          )) {
            results.push(...batch);
          }
          expect(results).toEqual(c.expected);
        }
        if (c.listAndScanOpts) {
          for (const batchSize of [1, 2, 3, 128]) {
            const results: [string, string][] = [];
            for await (const batch of cache.batchScan(
              c.listAndScanOpts.opts,
              valita.string(),
              batchSize,
            )) {
              results.push(...batch);
            }
            expect(results).toEqual(c.listAndScanOpts.expected);
          }
        }

        // Test getEntries
        await testGetEntries(durableEntryKeys, cache, c);
        await testGetEntries([...durableEntryKeys, ...c.pendingKeys], cache, c);

        const durableEntriesBeforeFlush = [
          ...(await durable.list({}, valita.string())),
        ];
        expect(durableEntriesBeforeFlush).toEqual(
          durableEntryKeys.map(k => [k, `orig-${k}`]),
        );

        expect(cache.pending()).toEqual(c.expectedPending);
        const expectedCounts = {
          delCount: 0,
          putCount: 0,
        };
        for (const {op} of c.expectedPending) {
          if (op === 'del') {
            expectedCounts.delCount++;
          }
          if (op === 'put') {
            expectedCounts.putCount++;
          }
        }
        expect(cache.pendingCounts()).toEqual(expectedCounts);

        await cache.flush();
        expect(cache.isDirty()).toBe(false);

        const durableEntriesAfterFlush = [
          ...(await durable.list({}, valita.string())),
        ];

        expect(durableEntriesAfterFlush).toEqual(c.expected);
      });
    });
  }
});

async function testGetEntries(keys: string[], cache: EntryCache, c: Case) {
  const compareEntries = (
    [k1, _1]: [string, string],
    [k2, _2]: [string, string],
  ) => {
    if (k1 === k2) {
      return 0;
    } else if (k1 < k2) {
      return -1;
    }
    return 1;
  };
  const sortedResultEntries = [
    ...(await cache.getEntries(keys, valita.string())).entries(),
  ].sort(compareEntries);
  const sortedExpectedEntries = c.expected
    .filter(([k]) => keys.includes(k))
    .sort(compareEntries);
  expect(sortedResultEntries).toEqual(sortedExpectedEntries);
}
