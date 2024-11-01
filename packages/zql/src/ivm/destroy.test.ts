import {expect, test} from 'vitest';
import {MemorySource} from './memory-source.js';
import {Snitch} from './snitch.js';
import {Filter} from './filter.js';
import {FanOut} from './fan-out.js';
import {FanIn} from './fan-in.js';
import {Catch} from './catch.js';

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

test('destroy a pipeline that has forking', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = ms.connect([['a', 'asc']]);
  const fanOut = new FanOut(connector);
  const filter1 = new Filter(fanOut, 'all', () => true);
  const filter2 = new Filter(fanOut, 'all', () => true);
  const filter3 = new Filter(fanOut, 'all', () => true);
  const fanIn = new FanIn(fanOut, [filter1, filter2, filter3]);
  const out = new Catch(fanIn);

  ms.push({type: 'add', row: {a: 1, b: 'foo'}});

  const expected = [
    {
      node: {
        relationships: {},
        row: {
          a: 1,
          b: 'foo',
        },
      },
      type: 'add',
    },
  ];
  expect(out.pushes).toEqual(expected);

  out.destroy();
  ms.push({type: 'add', row: {a: 2, b: 'foo'}});

  // The pipeline was destroyed. No new events should
  // be received.
  expect(out.pushes).toEqual(expected);

  expect(() => out.destroy()).toThrow(
    'FanOut already destroyed once for each output',
  );
});
