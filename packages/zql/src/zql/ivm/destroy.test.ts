import {expect, test} from 'vitest';
import {ArrayView} from './array-view.js';
import {Filter} from './filter.js';
import {Join} from './join.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Snitch} from './snitch.js';

test('destroy source connections', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}},
    ['a'],
  );
  const connection1 = ms.connect([['a', 'asc']]);
  const connection2 = ms.connect([['a', 'asc']]);

  const snitch1 = new Snitch(connection1, 'snitch1');
  const snitch2 = new Snitch(connection2, 'snitch2');

  const msg1 = {
    type: 'add',
    row: {a: 3},
  } as const;
  ms.push(msg1);

  expect(snitch1.log).toEqual([['snitch1', 'push', msg1]]);
  expect(snitch2.log).toEqual([['snitch2', 'push', msg1]]);

  snitch1.destroy();

  const msg2 = {
    type: 'add',
    row: {a: 2},
  } as const;
  ms.push(msg2);

  // snitch1 was destroyed. No new events should
  // be received.
  expect(snitch1.log).toEqual([['snitch1', 'push', msg1]]);
  // snitch 2 is a separate connection and should not
  // have been destroyed
  expect(snitch2.log).toEqual([
    ['snitch2', 'push', msg1],
    ['snitch2', 'push', msg2],
  ]);

  snitch2.destroy();
  const msg3 = {
    type: 'add',
    row: {a: 1},
  } as const;
  ms.push(msg3);
  expect(snitch2.log).toEqual([
    ['snitch2', 'push', msg1],
    ['snitch2', 'push', msg2],
  ]);
});

test('destroy a pipeline from the view', () => {
  // two sources
  // filtered
  // joined
  const source1 = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}},
    ['b'],
  );
  const source2 = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}},
    ['b'],
  );

  const filter1 = new Filter(
    source1.connect([['b', 'asc']]),
    'all',
    () => true,
  );
  const filter2 = new Filter(
    source2.connect([['b', 'asc']]),
    'all',
    () => true,
  );
  const join = new Join({
    parent: filter1,
    child: filter2,
    storage: new MemoryStorage(),
    parentKey: 'a',
    childKey: 'a',
    relationshipName: 'stuff',
    hidden: false,
  });

  const view = new ArrayView(join, {
    singular: false,
    relationships: {stuff: {singular: false, relationships: {}}},
  });
  let data: unknown;
  view.addListener(d => {
    data = structuredClone(d);
  });

  source1.push({
    type: 'add',
    row: {
      a: 'a',
      b: 'b-source-1',
    },
  });
  source2.push({
    type: 'add',
    row: {
      a: 'a',
      b: 'b-source-2',
    },
  });
  view.flush();

  const expected = [
    {
      a: 'a',
      b: 'b-source-1',
      stuff: [
        {
          a: 'a',
          b: 'b-source-2',
        },
      ],
    },
  ];
  expect(data).toEqual(expected);

  view.destroy();

  source2.push({
    type: 'remove',
    row: {
      a: 'a',
      b: 'b-source-2',
    },
  });
  view.flush();

  // view was destroyed before last push so data is unchanged
  expect(data).toEqual(expected);
});
