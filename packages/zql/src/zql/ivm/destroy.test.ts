import {expect, test} from 'vitest';
import {MemorySource} from './memory-source.js';
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
