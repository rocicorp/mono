import {expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Catch} from './catch.ts';
import {FanIn} from './fan-in.ts';
import {FanOut} from './fan-out.ts';
import {
  buildFilterPipeline,
  FilterEnd,
  FilterStart,
} from './filter-operators.ts';
import {Filter} from './filter.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

test('fan-out pushes along all paths', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = s.connect([['a', 'asc']]);

  const filterStart = new FilterStart(connector);
  const fanOut = new FanOut(filterStart);
  const catch1 = new Catch(new FilterEnd(filterStart, fanOut));
  const catch2 = new Catch(new FilterEnd(filterStart, fanOut));
  const catch3 = new Catch(new FilterEnd(filterStart, fanOut));

  // dummy fan-in for invariant in fan-out
  const fanIn = new FanIn(fanOut, []);
  fanOut.setFanIn(fanIn);

  s.push({type: 'add', row: {a: 1, b: 'foo'}});
  s.push({type: 'edit', oldRow: {a: 1, b: 'foo'}, row: {a: 1, b: 'bar'}});
  s.push({type: 'remove', row: {a: 1, b: 'bar'}});

  expect(catch1.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "oldRow": {
          "a": 1,
          "b": "foo",
        },
        "row": {
          "a": 1,
          "b": "bar",
        },
        "type": "edit",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "bar",
          },
        },
        "type": "remove",
      },
    ]
  `);
  expect(catch2.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "oldRow": {
          "a": 1,
          "b": "foo",
        },
        "row": {
          "a": 1,
          "b": "bar",
        },
        "type": "edit",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "bar",
          },
        },
        "type": "remove",
      },
    ]
  `);
  expect(catch3.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "oldRow": {
          "a": 1,
          "b": "foo",
        },
        "row": {
          "a": 1,
          "b": "bar",
        },
        "type": "edit",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "bar",
          },
        },
        "type": "remove",
      },
    ]
  `);
});

test('fan-out,fan-in pairing does not duplicate pushes', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  const connector = s.connect([['a', 'asc']]);
  const pipeline = buildFilterPipeline(connector, filterInput => {
    const fanOut = new FanOut(filterInput);
    const filter1 = new Filter(fanOut, () => true);
    const filter2 = new Filter(fanOut, () => true);
    const filter3 = new Filter(fanOut, () => true);

    const fanIn = new FanIn(fanOut, [filter1, filter2, filter3]);
    fanOut.setFanIn(fanIn);
    return fanIn;
  });
  const out = new Catch(pipeline);

  s.push({type: 'add', row: {a: 1, b: 'foo'}});
  s.push({type: 'add', row: {a: 2, b: 'foo'}});
  s.push({type: 'add', row: {a: 3, b: 'foo'}});

  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 1,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 2,
            "b": "foo",
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 3,
            "b": "foo",
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('fan-in fetch', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'boolean'}, b: {type: 'boolean'}},
    ['a', 'b'],
  );

  s.push({type: 'add', row: {a: false, b: false}});
  s.push({type: 'add', row: {a: false, b: true}});
  s.push({type: 'add', row: {a: true, b: false}});
  s.push({type: 'add', row: {a: true, b: true}});

  const connector = s.connect([
    ['a', 'asc'],
    ['b', 'asc'],
  ]);

  const pipeline = buildFilterPipeline(connector, filterInput => {
    const fanOut = new FanOut(filterInput);

    const filter1 = new Filter(fanOut, row => row.a === true);
    const filter2 = new Filter(fanOut, row => row.b === true);
    const filter3 = new Filter(
      fanOut,
      row => row.a === true && row.b === false,
    ); // duplicates a row of filter1
    const filter4 = new Filter(fanOut, row => row.a === true && row.b === true); // duplicates a row of filter1 and filter2

    return new FanIn(fanOut, [filter1, filter2, filter3, filter4]);
  });

  const out = new Catch(pipeline);
  const result = out.fetch();
  expect(result).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": false,
          "b": true,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": true,
          "b": false,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": true,
          "b": true,
        },
      },
    ]
  `);
});

test('cleanup forwards too all branches', () => {
  const s = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  s.push({type: 'add', row: {a: 1, b: 'foo'}});

  const connector = s.connect([['a', 'asc']]);
  const filterStart = new FilterStart(connector);
  const fanOut = new FanOut(filterStart);
  const filter1 = new Filter(fanOut, () => false);
  const filter2 = new Filter(fanOut, () => true);
  const filter3 = new Filter(fanOut, () => true);

  const fanIn = new FanIn(fanOut, [filter1, filter2, filter3]);
  fanOut.setFanIn(fanIn);
  const out = new Catch(new FilterEnd(filterStart, fanIn));

  const filterSpy1 = vi.spyOn(filter1, 'filter');
  const filterSpy2 = vi.spyOn(filter2, 'filter');
  const filterSpy3 = vi.spyOn(filter3, 'filter');

  const result = out.cleanup();
  expect(result).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "b": "foo",
        },
      },
    ]
  `);

  expect(filterSpy1).toHaveBeenCalledExactlyOnceWith(
    {relationships: {}, row: {a: 1, b: 'foo'}},
    true,
  );
  expect(filterSpy2).toHaveBeenCalledExactlyOnceWith(
    {relationships: {}, row: {a: 1, b: 'foo'}},
    true,
  );
  expect(filterSpy3).toHaveBeenCalledExactlyOnceWith(
    {relationships: {}, row: {a: 1, b: 'foo'}},
    true,
  );
});
