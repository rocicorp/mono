import {test, expect} from '@jest/globals';
import type {PatchOperation} from 'replicache';
import {getPatch} from '../../src/ff/get-patch.js';
import type {Version} from '../../src/types/version.js';
import {ReplicacheTransaction} from '../../src/storage/replicache-transaction.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('getPatch', async () => {
  type Case = {
    name: string;
    // undefined value means delete
    muts?: {key: string; value?: number; version: number}[];
    fromCookie: Version;
    expected: PatchOperation[];
  };

  const cases: Case[] = [
    {
      name: 'add a, diff from null',
      muts: [{key: 'a', value: 1, version: 2}],
      fromCookie: 0,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 1,
        },
      ],
    },
    {
      name: 'add a, diff from 1',
      fromCookie: 1,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 1,
        },
      ],
    },
    {
      name: 'add a, diff from 2',
      fromCookie: 2,
      expected: [],
    },
    {
      name: 'add a + b, diff from null',
      muts: [{key: 'b', value: 2, version: 3}],
      fromCookie: 0,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 1,
        },
        {
          op: 'put',
          key: 'b',
          value: 2,
        },
      ],
    },
    {
      name: 'add a + b, diff from 2',
      muts: [],
      fromCookie: 2,
      expected: [
        {
          op: 'put',
          key: 'b',
          value: 2,
        },
      ],
    },
    {
      name: 'add a + b, diff from 3',
      muts: [],
      fromCookie: 3,
      expected: [],
    },
    {
      name: 'add a + b, diff from 4',
      muts: [],
      fromCookie: 4,
      expected: [],
    },
    {
      name: 'del a, diff from 3',
      muts: [{key: 'a', version: 4}],
      fromCookie: 3,
      expected: [
        {
          op: 'del',
          key: 'a',
        },
      ],
    },
    {
      name: 'del a, diff from 4',
      fromCookie: 4,
      expected: [],
    },
  ];

  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id),
  );

  for (const c of cases) {
    for (const p of c.muts || []) {
      const tx = new ReplicacheTransaction(storage, 'c1', p.version);
      if (p.value !== undefined) {
        await tx.put(p.key, p.value);
      } else {
        await tx.del(p.key);
      }
    }
    const patch = await getPatch(storage, c.fromCookie);
    expect(patch).toEqual(c.expected);
  }
});
