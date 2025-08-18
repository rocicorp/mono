import {expect, test} from 'vitest';
import {Catch} from './catch.ts';
import {Filter} from './filter.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {buildFilterPipeline} from './filter-operators.ts';
import type {BuilderDelegate} from '../builder/builder.ts';

const lc = createSilentLogContext();
const mockDelegate = {
  addEdge() {},
} as unknown as BuilderDelegate;

test('basics', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, b: {type: 'string'}},
    ['a'],
  );
  ms.push({type: 'add', row: {a: 3, b: 'foo'}});
  ms.push({type: 'add', row: {a: 2, b: 'bar'}});
  ms.push({type: 'add', row: {a: 1, b: 'foo'}});

  const connector = ms.connect([['a', 'asc']]);
  const filter = buildFilterPipeline(
    connector,
    mockDelegate,
    filterInput => new Filter(filterInput, row => row.b === 'foo'),
  );

  const out = new Catch(filter);

  expect(out.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "b": "foo",
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 3,
          "b": "foo",
        },
      },
    ]
  `);
  ms.push({type: 'add', row: {a: 4, b: 'bar'}});
  ms.push({type: 'add', row: {a: 5, b: 'foo'}});
  ms.push({type: 'remove', row: {a: 3, b: 'foo'}});
  ms.push({type: 'remove', row: {a: 2, b: 'bar'}});

  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 5,
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
        "type": "remove",
      },
    ]
  `);

  expect(out.cleanup({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 1,
          "b": "foo",
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 5,
          "b": "foo",
        },
      },
    ]
  `);
});

test('edit', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'number'}, x: {type: 'number'}},
    ['a'],
  );
  for (const row of [
    {a: 1, x: 1},
    {a: 2, x: 2},
    {a: 3, x: 3},
  ]) {
    ms.push({type: 'add', row});
  }

  const connector = ms.connect([['a', 'asc']]);
  const filter = buildFilterPipeline(
    connector,
    mockDelegate,
    filterInput => new Filter(filterInput, row => (row.x as number) % 2 === 0),
  );
  const out = new Catch(filter);

  expect(out.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 2,
          "x": 2,
        },
      },
    ]
  `);

  ms.push({type: 'add', row: {a: 4, x: 4}});
  ms.push({type: 'edit', oldRow: {a: 3, x: 3}, row: {a: 3, x: 6}});

  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 4,
            "x": 4,
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 3,
            "x": 6,
          },
        },
        "type": "add",
      },
    ]
  `);
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 2,
          "x": 2,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 3,
          "x": 6,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 4,
          "x": 4,
        },
      },
    ]
  `);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 3, x: 6}, row: {a: 3, x: 5}});
  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "a": 3,
            "x": 6,
          },
        },
        "type": "remove",
      },
    ]
  `);
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 2,
          "x": 2,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 4,
          "x": 4,
        },
      },
    ]
  `);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 3, x: 5}, row: {a: 3, x: 7}});
  expect(out.pushes).toMatchInlineSnapshot(`[]`);
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 2,
          "x": 2,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 4,
          "x": 4,
        },
      },
    ]
  `);

  out.pushes.length = 0;
  ms.push({type: 'edit', oldRow: {a: 2, x: 2}, row: {a: 2, x: 4}});
  expect(out.pushes).toMatchInlineSnapshot(`
    [
      {
        "oldRow": {
          "a": 2,
          "x": 2,
        },
        "row": {
          "a": 2,
          "x": 4,
        },
        "type": "edit",
      },
    ]
  `);
  expect(out.fetch({})).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": 2,
          "x": 4,
        },
      },
      {
        "relationships": {},
        "row": {
          "a": 4,
          "x": 4,
        },
      },
    ]
  `);
});
