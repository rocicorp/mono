import {expect} from '@esm-bundle/chai';
import type {InternalDiff} from '../btree/node.js';
import * as dag from '../dag/mod';
import {diff} from './diff.js';
import {ChainBuilder} from '../db/test-helpers';
import {testSubscriptionsManagerOptions} from '../test-util.js';

type DiffsRecord = Record<string, InternalDiff>;

test('db diff', async () => {
  const store = new dag.TestStore();
  const clientID = 'client-id-1';
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID);
  await b.addLocal(clientID, [['a', 'a2']]);
  await b.addLocal(clientID, [['b', 'b1']]);

  const t = async (iOld: number, iNew: number, expectedDiff: DiffsRecord) => {
    await store.withRead(async read => {
      const diffsMap = await diff(
        b.chain[iOld].chunk.hash,
        b.chain[iNew].chunk.hash,
        read,
        testSubscriptionsManagerOptions,
      );
      expect(Object.fromEntries(diffsMap)).to.deep.equal(expectedDiff);
    });
  };

  await t(0, 1, {
    '': [
      {
        key: 'a',
        newValue: 'a2',
        op: 'add',
      },
    ],
  });

  await t(0, 2, {
    '': [
      {
        key: 'a',
        newValue: 'a2',
        op: 'add',
      },
      {key: 'b', newValue: 'b1', op: 'add'},
    ],
  });

  await t(1, 2, {
    '': [{key: 'b', newValue: 'b1', op: 'add'}],
  });

  await t(0, 0, {});

  if (DD31) {
    await b.addSnapshot([], clientID, undefined, undefined, {
      'index-c': {prefix: 'c', jsonPointer: ''},
    });
    await b.addLocal(clientID, [
      ['c1', 'c1'],
      ['c2', 'c2'],
    ]);

    await t(b.chain.length - 2, b.chain.length - 1, {
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
    });
  } else {
    await b.addSnapshot(
      [
        ['c1', 'c1'],
        ['c2', 'c2'],
      ],
      clientID,
    );
    await b.addIndexChange(clientID, 'index-c', {
      prefix: 'c',
      jsonPointer: '',
    });

    await t(b.chain.length - 2, b.chain.length - 1, {
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
    });
  }

  await b.addLocal(clientID, [['c1', 'c1-new']]);
  await t(b.chain.length - 2, b.chain.length - 1, {
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
  });

  if (DD31) {
    await t(b.chain.length - 3, b.chain.length - 1, {
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
    });
  } else {
    await t(b.chain.length - 3, b.chain.length - 1, {
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
    });
  }
});
