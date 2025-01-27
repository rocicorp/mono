import {expect, test} from 'vitest';
import type {InternalDiff} from '../btree/node.ts';
import {TestStore} from '../dag/test-store.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {IndexDefinitions} from '../index-defs.ts';
import {testSubscriptionsManagerOptions} from '../test-util.ts';
import {withRead} from '../with-transactions.ts';
import {diff} from './diff.ts';

type DiffsRecord = Record<string, InternalDiff>;

test('db diff dd31', async () => {
  const clientID = 'client-id-1';

  const t = async ({
    iOld,
    iNew,
    expectedDiff,
    indexDefinitions,
    setupChain,
  }: {
    iOld: number;
    iNew: number;
    expectedDiff: DiffsRecord;
    indexDefinitions?: IndexDefinitions;
    setupChain?: (b: ChainBuilder) => Promise<void>;
  }) => {
    const formatVersion = FormatVersion.Latest;
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID, indexDefinitions);
    await b.addLocal(clientID, [['a', 'a2']]);
    await b.addLocal(clientID, [['b', 'b1']]);
    await setupChain?.(b);

    await withRead(store, async read => {
      const diffsMap = await diff(
        b.chain[iOld].chunk.hash,
        b.chain[iNew].chunk.hash,
        read,
        testSubscriptionsManagerOptions,
        formatVersion,
      );
      expect(Object.fromEntries(diffsMap)).to.deep.equal(expectedDiff);
    });
  };

  await t({
    iOld: 0,
    iNew: 1,
    expectedDiff: {
      '': [
        {
          key: 'a',
          newValue: 'a2',
          op: 'add',
        },
      ],
    },
  });

  await t({
    iOld: 0,
    iNew: 2,
    expectedDiff: {
      '': [
        {
          key: 'a',
          newValue: 'a2',
          op: 'add',
        },
        {key: 'b', newValue: 'b1', op: 'add'},
      ],
    },
  });

  await t({
    iOld: 1,
    iNew: 2,
    expectedDiff: {
      '': [{key: 'b', newValue: 'b1', op: 'add'}],
    },
  });

  await t({iOld: 0, iNew: 0, expectedDiff: {}});

  await t({
    iOld: 2,
    iNew: 3,
    expectedDiff: {
      '': [
        {
          key: 'c1',
          newValue: 'c1',
          op: 'add',
        },
        {
          key: 'c2',
          newValue: 'c2',
          op: 'add',
        },
      ],
      'index-c': [
        {
          key: '\u0000c1\u0000c1',
          newValue: 'c1',
          op: 'add',
        },
        {
          key: '\u0000c2\u0000c2',
          newValue: 'c2',
          op: 'add',
        },
      ],
    },
    indexDefinitions: {
      'index-c': {prefix: 'c', jsonPointer: ''},
    },
    setupChain: async b => {
      await b.addLocal(clientID, [
        ['c1', 'c1'],
        ['c2', 'c2'],
      ]);
    },
  });

  await t({
    iOld: 3,
    iNew: 4,
    expectedDiff: {
      '': [
        {
          key: 'c1',
          newValue: 'c1-new',
          oldValue: 'c1',
          op: 'change',
        },
      ],
      'index-c': [
        {
          key: '\u0000c1\u0000c1',
          oldValue: 'c1',
          op: 'del',
        },
        {
          key: '\u0000c1-new\u0000c1',
          newValue: 'c1-new',
          op: 'add',
        },
      ],
    },
    indexDefinitions: {
      'index-c': {prefix: 'c', jsonPointer: ''},
    },
    setupChain: async b => {
      await b.addLocal(clientID, [
        ['c1', 'c1'],
        ['c2', 'c2'],
      ]);
      await b.addLocal(clientID, [['c1', 'c1-new']]);
    },
  });

  await t({
    iOld: 2,
    iNew: 4,
    expectedDiff: {
      '': [
        {
          key: 'c1',
          newValue: 'c1-new',
          op: 'add',
        },
        {
          key: 'c2',
          newValue: 'c2',
          op: 'add',
        },
      ],
      'index-c': [
        {
          key: '\u0000c1-new\u0000c1',
          newValue: 'c1-new',
          op: 'add',
        },
        {
          key: '\u0000c2\u0000c2',
          newValue: 'c2',
          op: 'add',
        },
      ],
    },
    indexDefinitions: {
      'index-c': {prefix: 'c', jsonPointer: ''},
    },
    setupChain: async b => {
      await b.addLocal(clientID, [
        ['c1', 'c1'],
        ['c2', 'c2'],
      ]);
      await b.addLocal(clientID, [['c1', 'c1-new']]);
    },
  });
});
