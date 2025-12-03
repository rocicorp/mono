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
import {consume} from './stream.ts';
import type {BuilderDelegate} from '../builder/builder.ts';

const lc = createSilentLogContext();
const mockDelegate = {
  addEdge() {},
} as unknown as BuilderDelegate;

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

  consume(s.push({type: 'add', row: {a: 1, b: 'foo'}}));
  consume(
    s.push({type: 'edit', oldRow: {a: 1, b: 'foo'}, row: {a: 1, b: 'bar'}}),
  );
  consume(s.push({type: 'remove', row: {a: 1, b: 'bar'}}));

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
  const pipeline = buildFilterPipeline(connector, mockDelegate, filterInput => {
    const fanOut = new FanOut(filterInput);
    const filter1 = new Filter(fanOut, () => true);
    const filter2 = new Filter(fanOut, () => true);
    const filter3 = new Filter(fanOut, () => true);

    const fanIn = new FanIn(fanOut, [filter1, filter2, filter3]);
    fanOut.setFanIn(fanIn);
    return fanIn;
  });
  const out = new Catch(pipeline);

  consume(s.push({type: 'add', row: {a: 1, b: 'foo'}}));
  consume(s.push({type: 'add', row: {a: 2, b: 'foo'}}));
  consume(s.push({type: 'add', row: {a: 3, b: 'foo'}}));

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

  consume(s.push({type: 'add', row: {a: false, b: false}}));
  consume(s.push({type: 'add', row: {a: false, b: true}}));
  consume(s.push({type: 'add', row: {a: true, b: false}}));
  consume(s.push({type: 'add', row: {a: true, b: true}}));

  const connector = s.connect([
    ['a', 'asc'],
    ['b', 'asc'],
  ]);

  const pipeline = buildFilterPipeline(connector, mockDelegate, filterInput => {
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

test('FanOut forwards beginFilter/endFilter to all outputs', () => {
  const mockInput = {
    setFilterOutput: vi.fn(),
    getSchema: vi.fn(),
    destroy: vi.fn(),
  };

  const fanOut = new FanOut(mockInput);
  const mockOutput1 = {
    push: vi.fn(),
    filter: vi.fn(),
    beginFilter: vi.fn(),
    endFilter: vi.fn(),
  };
  const mockOutput2 = {
    push: vi.fn(),
    filter: vi.fn(),
    beginFilter: vi.fn(),
    endFilter: vi.fn(),
  };

  fanOut.setFilterOutput(mockOutput1);
  fanOut.setFilterOutput(mockOutput2);

  fanOut.beginFilter();
  expect(mockOutput1.beginFilter).toHaveBeenCalled();
  expect(mockOutput2.beginFilter).toHaveBeenCalled();

  fanOut.endFilter();
  expect(mockOutput1.endFilter).toHaveBeenCalled();
  expect(mockOutput2.endFilter).toHaveBeenCalled();
});

test('FanIn forwards beginFilter/endFilter to output', () => {
  const mockFanOut = {
    getSchema: vi.fn(),
  } as unknown as FanOut;
  const mockInput = {
    setFilterOutput: vi.fn(),
    getSchema: vi.fn(),
    destroy: vi.fn(),
  };

  const fanIn = new FanIn(mockFanOut, [mockInput]);
  const mockOutput = {
    push: vi.fn(),
    filter: vi.fn(),
    beginFilter: vi.fn(),
    endFilter: vi.fn(),
  };

  fanIn.setFilterOutput(mockOutput);

  fanIn.beginFilter();
  expect(mockOutput.beginFilter).toHaveBeenCalled();

  fanIn.endFilter();
  expect(mockOutput.endFilter).toHaveBeenCalled();
});
