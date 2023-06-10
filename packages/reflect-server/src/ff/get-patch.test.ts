import {expect, describe, test} from '@jest/globals';
import type {NullableVersion} from 'reflect-protocol';
import type {PatchOperation} from 'replicache';
import {getPatch} from '../../src/ff/get-patch.js';
import {DurableStorage} from '../../src/storage/durable-storage.js';
import {ReplicacheTransaction} from '../../src/storage/replicache-transaction.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

describe('getPatch', () => {
  type Case = {
    name: string;
    // undefined value means delete
    muts?: {key: string; value?: string; mutationID: number; version: number}[];
    fromCookie: NullableVersion;
    expected: PatchOperation[];
  };

  const cases: Case[] = [
    {
      name: 'add a+b, diff from null',
      muts: [
        {key: 'a', mutationID: 1, value: 'a1', version: 2},
        {key: 'b', mutationID: 1, value: 'b1', version: 2},
      ],
      fromCookie: null,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 'a1',
        },
        {
          op: 'put',
          key: 'b',
          value: 'b1',
        },
      ],
    },
    {
      name: 'del b, diff from null',
      muts: [{key: 'b', mutationID: 2, version: 3}],
      fromCookie: null,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 'a1',
        },
        // no delete for b because diff is from null
      ],
    },
    {
      name: 'diff from 1',
      fromCookie: 1,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 'a1',
        },
        {
          op: 'del',
          key: 'b',
        },
      ],
    },
    {
      name: 'diff from 2',
      fromCookie: 2,
      expected: [
        {
          op: 'del',
          key: 'b',
        },
      ],
    },
    {
      name: 'add b, diff from null',
      muts: [{key: 'b', mutationID: 3, value: 'b2', version: 4}],
      fromCookie: 0,
      expected: [
        {
          op: 'put',
          key: 'a',
          value: 'a1',
        },
        {
          op: 'put',
          key: 'b',
          value: 'b2',
        },
      ],
    },
    {
      name: 'diff from 2 after b re-added',
      muts: [],
      fromCookie: 2,
      expected: [
        {
          op: 'put',
          key: 'b',
          value: 'b2',
        },
      ],
    },
    {
      name: 'diff from 3',
      muts: [],
      fromCookie: 3,
      expected: [
        {
          op: 'put',
          key: 'b',
          value: 'b2',
        },
      ],
    },
    {
      name: 'diff from 4',
      muts: [],
      fromCookie: 4,
      expected: [],
    },
    {
      name: 'del a, diff from 4',
      muts: [{key: 'a', mutationID: 4, version: 5}],
      fromCookie: 4,
      expected: [
        {
          op: 'del',
          key: 'a',
        },
      ],
    },
    {
      name: 'diff from 5',
      fromCookie: 5,
      expected: [],
    },
  ];

  for (let i = 0; i < cases.length; i++) {
    const c = cases[i];
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      await durable.deleteAll();
      const storage = new DurableStorage(durable);
      // The cases build on each other, apply mutations from
      // all previous cases, and this case.
      for (const p of cases.slice(0, i + 1).flatMap(c => c.muts || [])) {
        const tx = new ReplicacheTransaction(
          storage,
          'c1',
          p.mutationID,
          p.version,
          undefined,
        );
        if (p.value !== undefined) {
          await tx.put(p.key, p.value);
        } else {
          await tx.del(p.key);
        }
      }
      const patch = await getPatch(storage, c.fromCookie);
      expect(patch).toEqual(c.expected);
    });
  }
});
