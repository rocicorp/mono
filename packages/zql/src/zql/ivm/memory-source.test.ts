import {describe, expect, test} from 'vitest';
import {Ordering} from '../ast/ast.js';
import {Catch} from './catch.js';
import {Row} from './data.js';
import {compareRowsTest} from './data.test.js';
import {
  generateWithOverlayInner,
  MemorySource,
  Overlay,
  overlayForConstraintForTest,
  overlayForStartAtForTest,
} from './memory-source.js';
import type {PrimaryKey, SchemaValue} from './schema.js';
import {runCases} from './test/source-cases.js';

runCases(
  (
    name: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
  ) => new MemorySource(name, columns, primaryKey),
);

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = new MemorySource('table', {a: {type: 'string'}}, ['a']);
    return ms.connect(order).getSchema().compareRows;
  });
});

test('indexes get cleaned up when not needed', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);

  const conn1 = ms.connect([
    ['a', 'asc'],
    ['b', 'asc'],
  ]);
  const c1 = new Catch(conn1);
  c1.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([
      ['a', 'asc'],
      ['b', 'asc'],
    ]),
  ]);

  const conn2 = ms.connect([
    ['a', 'asc'],
    ['b', 'asc'],
  ]);
  const c2 = new Catch(conn2);
  c2.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([
      ['a', 'asc'],
      ['b', 'asc'],
    ]),
  ]);

  const conn3 = ms.connect([
    ['a', 'asc'],
    ['c', 'asc'],
  ]);
  const c3 = new Catch(conn3);
  c3.fetch();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([
      ['a', 'asc'],
      ['b', 'asc'],
    ]),
    JSON.stringify([
      ['a', 'asc'],
      ['c', 'asc'],
    ]),
  ]);

  conn3.destroy();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([
      ['a', 'asc'],
      ['b', 'asc'],
    ]),
  ]);

  conn2.destroy();
  expect(ms.getIndexKeys()).toEqual([
    JSON.stringify([['a', 'asc']]),
    JSON.stringify([
      ['a', 'asc'],
      ['b', 'asc'],
    ]),
  ]);

  conn1.destroy();
  expect(ms.getIndexKeys()).toEqual([JSON.stringify([['a', 'asc']])]);
});

test('push edit change', () => {
  const ms = new MemorySource(
    'table',
    {a: {type: 'string'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );

  ms.push({
    type: 'add',
    row: {a: 'a', b: 'b', c: 'c'},
  });

  const conn = ms.connect([['a', 'asc']]);
  const c = new Catch(conn);

  ms.push({
    type: 'edit',
    oldRow: {a: 'a', b: 'b', c: 'c'},
    row: {a: 'a', b: 'b2', c: 'c2'},
  });
  expect(c.pushes).toEqual([
    {
      type: 'edit',
      row: {a: 'a', b: 'b2', c: 'c2'},
      oldRow: {a: 'a', b: 'b', c: 'c'},
    },
  ]);
  expect(c.fetch()).toEqual([
    {
      row: {a: 'a', b: 'b2', c: 'c2'},
      relationships: {},
    },
  ]);

  conn.destroy();
});

describe('generateWithOverlayInner', () => {
  const rows = [
    {id: 1, s: 'a', n: 11},
    {id: 2, s: 'b', n: 22},
    {id: 3, s: 'c', n: 33},
  ];

  const compare = (a: Row, b: Row) => (a.id as number) - (b.id as number);

  test.each([
    {
      name: 'No overlay',
      changes: [undefined],
      expected: rows,
    },

    {
      name: 'Add overlay before start',
      changes: [{type: 'add', row: {id: 0, s: 'd', n: 0}}],
      expected: [{id: 0, s: 'd', n: 0}, ...rows],
    },
    {
      name: 'Add overlay at end',
      changes: [{type: 'add', row: {id: 4, s: 'd', n: 44}}],
      expected: [...rows, {id: 4, s: 'd', n: 44}],
    },
    {
      name: 'Add overlay middle',
      changes: [{type: 'add', row: {id: 2.5, s: 'b2', n: 225}}],
      expected: [rows[0], rows[1], {id: 2.5, s: 'b2', n: 225}, rows[2]],
    },
    {
      name: 'Add overlay replace',
      changes: [{type: 'add', row: {id: 2, s: 'b2', n: 225}}],
      expected: [rows[0], rows[1], {id: 2, s: 'b2', n: 225}, rows[2]],
    },

    {
      name: 'Remove overlay before start',
      changes: [{type: 'remove', row: {id: 0, s: 'z', n: -1}}],
      expected: rows,
    },
    {
      name: 'Remove overlay start',
      changes: [{type: 'remove', row: {id: 1, s: 'a', n: 11}}],
      expected: rows.slice(1),
    },
    {
      name: 'Remove overlay at end',
      changes: [{type: 'remove', row: {id: 3, s: 'c', n: 33}}],
      expected: rows.slice(0, -1),
    },
    {
      name: 'Remove overlay middle',
      changes: [{type: 'remove', row: {id: 2, s: 'b', n: 22}}],
      expected: [rows[0], rows[2]],
    },
    {
      name: 'Remove overlay after end',
      changes: [{type: 'remove', row: {id: 4, s: 'd', n: 44}}],
      expected: rows,
    },

    // Two overlays
    {
      name: 'Basic edit',
      changes: [
        {type: 'remove', row: {id: 2, s: 'b', n: 22}},
        {type: 'add', row: {id: 2, s: 'b2', n: 225}},
      ],
      expected: [rows[0], {id: 2, s: 'b2', n: 225}, rows[2]],
    },
    {
      name: 'Edit first, still first',
      changes: [
        {type: 'add', row: {id: 0, s: 'a0', n: 0}},
        {type: 'remove', row: {id: 1, s: 'a', n: 11}},
      ],
      expected: [{id: 0, s: 'a0', n: 0}, rows[1], rows[2]],
    },
    {
      name: 'Edit first, now second',
      changes: [
        {type: 'remove', row: {id: 1, s: 'a', n: 11}},
        {type: 'add', row: {id: 2.5, s: 'a', n: 11}},
      ],
      expected: [rows[1], {id: 2.5, s: 'a', n: 11}, rows[2]],
    },
    {
      name: 'Edit first, now last',
      changes: [
        {type: 'remove', row: {id: 1, s: 'a', n: 11}},
        {type: 'add', row: {id: 3.5, s: 'a', n: 11}},
      ],
      expected: [rows[1], rows[2], {id: 3.5, s: 'a', n: 11}],
    },

    {
      name: 'Edit second, now first',
      changes: [
        {type: 'add', row: {id: 0, s: 'b', n: 22}},
        {type: 'remove', row: {id: 2, s: 'b', n: 22}},
      ],
      expected: [{id: 0, s: 'b', n: 22}, rows[0], rows[2]],
    },
    {
      name: 'Edit second, still second',
      changes: [
        {type: 'remove', row: {id: 2, s: 'b', n: 22}},
        {type: 'add', row: {id: 2.5, s: 'b', n: 22}},
      ],
      expected: [rows[0], {id: 2.5, s: 'b', n: 22}, rows[2]],
    },
    {
      name: 'Edit second, still second',
      changes: [
        {type: 'add', row: {id: 1.5, s: 'b', n: 22}},
        {type: 'remove', row: {id: 2, s: 'b', n: 22}},
      ],
      expected: [rows[0], {id: 1.5, s: 'b', n: 22}, rows[2]],
    },
    {
      name: 'Edit second, now last',
      changes: [
        {type: 'remove', row: {id: 1, s: 'b', n: 22}},
        {type: 'add', row: {id: 3.5, s: 'b', n: 22}},
      ],
      expected: [rows[1], rows[2], {id: 3.5, s: 'b', n: 22}],
    },

    {
      name: 'Edit last, now first',
      changes: [
        {type: 'add', row: {id: 0, s: 'c', n: 33}},
        {type: 'remove', row: {id: 3, s: 'c', n: 33}},
      ],
      expected: [{id: 0, s: 'c', n: 33}, rows[0], rows[1]],
    },
    {
      name: 'Edit last, now second',
      changes: [
        {type: 'add', row: {id: 1.5, s: 'c', n: 33}},
        {type: 'remove', row: {id: 3, s: 'c', n: 33}},
      ],
      expected: [rows[0], {id: 1.5, s: 'c', n: 33}, rows[1]],
    },
    {
      name: 'Edit last, still last',
      changes: [
        {type: 'remove', row: {id: 3, s: 'c', n: 33}},
        {type: 'add', row: {id: 3.5, s: 'c', n: 33}},
      ],
      expected: [rows[0], rows[1], {id: 3.5, s: 'c', n: 33}],
    },
    {
      name: 'Edit last, still last',
      changes: [
        {type: 'add', row: {id: 2.5, s: 'c', n: 33}},
        {type: 'remove', row: {id: 3, s: 'c', n: 33}},
      ],
      expected: [rows[0], rows[1], {id: 2.5, s: 'c', n: 33}],
    },
  ] as const)('$name', ({changes, expected}) => {
    const actual = generateWithOverlayInner(
      rows,
      changes.map(change => change && {change, outputIndex: 0}) as [
        Overlay | undefined,
        Overlay | undefined,
      ],
      compare,
    );
    expect([...actual].map(({row}) => row)).toEqual(expected);
  });
});

test('overlayForConstraint', () => {
  expect(
    overlayForConstraintForTest(undefined, {key: 'a', value: 'b'}),
  ).toEqual(undefined);

  expect(
    overlayForConstraintForTest(
      {outputIndex: 0, change: {type: 'add', row: {a: 'b'}}},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({outputIndex: 0, change: {type: 'add', row: {a: 'b'}}});

  expect(
    overlayForConstraintForTest(
      {outputIndex: 0, change: {type: 'add', row: {a: 'c'}}},
      {key: 'a', value: 'b'},
    ),
  ).toEqual(undefined);
});

test('overlayForStartAt', () => {
  const compare = (a: Row, b: Row) => (a.id as number) - (b.id as number);
  expect(overlayForStartAtForTest(undefined, {id: 1}, compare)).toEqual(
    undefined,
  );
  expect(
    overlayForStartAtForTest(
      {
        outputIndex: 0,
        change: {type: 'add', row: {id: 1}},
      },
      {id: 1},
      compare,
    ),
  ).toEqual({
    outputIndex: 0,
    change: {type: 'add', row: {id: 1}},
  });
  expect(
    overlayForStartAtForTest(
      {
        outputIndex: 0,
        change: {type: 'add', row: {id: 1}},
      },
      {id: 0},
      compare,
    ),
  ).toEqual({
    outputIndex: 0,
    change: {type: 'add', row: {id: 1}},
  });
  expect(
    overlayForStartAtForTest(
      {
        outputIndex: 0,
        change: {type: 'add', row: {id: 1}},
      },
      {id: 2},
      compare,
    ),
  ).toEqual(undefined);
});
