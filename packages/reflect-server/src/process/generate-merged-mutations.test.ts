import {test, expect} from '@jest/globals';
import {generateMergedMutations} from '../../src/process/generate-merged-mutations.js';
import {pendingMutationsEntry, mutation} from '../util/test-utils.js';
import type {Mutation} from '../protocol/push.js';
import type {PendingMutationMap} from '../types/mutation.js';

test('generateMergedMutations', () => {
  type Case = {
    name: string;
    pendingMutations: PendingMutationMap;
    expected: Mutation[];
  };
  const cases: Case[] = [
    {
      name: 'empty',
      pendingMutations: new Map(),
      expected: [],
    },
    {
      name: 'one mutation',
      pendingMutations: new Map([
        pendingMutationsEntry('cg1', mutation('c1', 1)),
      ]),
      expected: [mutation('c1', 1)],
    },
    {
      name: 'multiple mutations across client groups in order',
      pendingMutations: new Map([
        pendingMutationsEntry(
          'cg1',
          mutation('c1', 1, 'a', null, 1),
          mutation('c2', 2, 'a', null, 2),
          mutation('c1', 2, 'a', null, 4),
        ),
        pendingMutationsEntry(
          'cg2',
          mutation('c3', 4, 'a', null, 3),
          mutation('c3', 5, 'a', null, 5),
          mutation('c4', 2, 'a', null, 6),
        ),
      ]),
      expected: [
        mutation('c1', 1, 'a', null, 1),
        mutation('c2', 2, 'a', null, 2),
        mutation('c3', 4, 'a', null, 3),
        mutation('c1', 2, 'a', null, 4),
        mutation('c3', 5, 'a', null, 5),
        mutation('c4', 2, 'a', null, 6),
      ],
    },
    {
      name: 'ooo timestamps',
      pendingMutations: new Map([
        pendingMutationsEntry(
          'cg1',
          mutation('c1', 1, 'a', null, 5),
          mutation('c2', 2, 'a', null, 2),
          mutation('c1', 2, 'a', null, 3),
        ),
        pendingMutationsEntry(
          'cg2',
          mutation('c3', 4, 'a', null, 1),
          mutation('c3', 5, 'a', null, 6),
          mutation('c4', 2, 'a', null, 4),
        ),
      ]),
      expected: [
        mutation('c3', 4, 'a', null, 1),
        mutation('c1', 1, 'a', null, 5),
        mutation('c2', 2, 'a', null, 2),
        mutation('c1', 2, 'a', null, 3),
        mutation('c3', 5, 'a', null, 6),
        mutation('c4', 2, 'a', null, 4),
      ],
    },
  ];
  for (const c of cases) {
    const gen = generateMergedMutations(c.pendingMutations);
    for (const [, m] of c.expected.entries()) {
      expect(gen.next().value).toEqual(m);
    }
    expect(gen.next().done);
  }
});
