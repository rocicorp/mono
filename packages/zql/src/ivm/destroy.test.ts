import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import {Catch} from './catch.ts';
import {FanIn} from './fan-in.ts';
import {FanOut} from './fan-out.ts';
import {buildFilterPipeline} from './filter-operators.ts';
import {Filter} from './filter.ts';
import {Snitch} from './snitch.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

import {makeSourceChangeAdd} from './source.ts';
const lc = createSilentLogContext();

test('destroy source connections', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'string'}, b: {type: 'string'}},
    ['a'],
  );
  const connection1 = ms.connect([['a', 'asc']]);
  const connection2 = ms.connect([['a', 'asc']]);

  const snitch1 = new Snitch(connection1, 'snitch1');
  const snitch2 = new Snitch(connection2, 'snitch2');

  const msg1 = makeSourceChangeAdd({a: 3});
  consume(ms.push(msg1));

  const record1 = {type: 'add', row: {a: 3}};
  expect(snitch1.log).toEqual([['snitch1', 'push', record1]]);
  expect(snitch2.log).toEqual([['snitch2', 'push', record1]]);

  snitch1.destroy();

  const msg2 = makeSourceChangeAdd({a: 2});
  consume(ms.push(msg2));

  // snitch1 was destroyed. No new events should
  // be received.
  const record2 = {type: 'add', row: {a: 2}};
  expect(snitch1.log).toEqual([['snitch1', 'push', record1]]);
  // snitch 2 is a separate connection and should not
  // have been destroyed
  expect(snitch2.log).toEqual([
    ['snitch2', 'push', record1],
    ['snitch2', 'push', record2],
  ]);

  snitch2.destroy();
  const msg3 = makeSourceChangeAdd({a: 1});
  consume(ms.push(msg3));
  expect(snitch2.log).toEqual([
    ['snitch2', 'push', record1],
    ['snitch2', 'push', record2],
  ]);
});

test('destroy a pipeline that has forking', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = ms.connect([['a', 'asc']]);
  const pipeline = buildFilterPipeline(
    connector,
    {
      addEdge() {},
    } as unknown as BuilderDelegate,
    filterInput => {
      const fanOut = new FanOut(filterInput);
      const filter1 = new Filter(fanOut, () => true);
      const filter2 = new Filter(fanOut, () => true);
      const filter3 = new Filter(fanOut, () => true);
      const fanIn = new FanIn(fanOut, [filter1, filter2, filter3]);
      fanOut.setFanIn(fanIn);
      return fanIn;
    },
  );

  const out = new Catch(pipeline);

  consume(ms.push(makeSourceChangeAdd({a: 1, b: 'foo'})));

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
  consume(ms.push(makeSourceChangeAdd({a: 2, b: 'foo'})));

  // The pipeline was destroyed. No new events should
  // be received.
  expect(out.pushes).toEqual(expected);

  expect(() => out.destroy()).toThrow(
    'FanOut already destroyed once for each output',
  );
});
