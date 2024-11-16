import {describe, expect, test} from 'vitest';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import {Catch} from './catch.js';
import {compareRowsTest} from './data.test.js';
import {
  filterOptionalFilters,
  generateWithOverlayInner,
  MemorySource,
  overlaysForConstraintForTest,
  overlaysForStartAtForTest,
} from './memory-source.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {runCases} from './test/source-cases.js';
import type {Change} from './change.js';

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

test('fetch during push edit change', () => {
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
  let fetchDuringPush = undefined;
  conn.setOutput({
    push(change: Change): void {
      expect(change).toEqual({
        type: 'edit',
        oldRow: {a: 'a', b: 'b', c: 'c'},
        row: {a: 'a', b: 'b2', c: 'c2'},
      });
      fetchDuringPush = [...conn.fetch({})];
    },
  });

  ms.push({
    type: 'edit',
    oldRow: {a: 'a', b: 'b', c: 'c'},
    row: {a: 'a', b: 'b2', c: 'c2'},
  });
  expect(fetchDuringPush).toEqual([
    {
      row: {a: 'a', b: 'b2', c: 'c2'},
      relationships: {},
    },
  ]);
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
      overlays: {
        add: undefined,
        remove: undefined,
      },
      expected: rows,
    },

    {
      name: 'Add overlay before start',
      overlays: {
        add: {id: 0, s: 'd', n: 0},
        remove: undefined,
      },
      expected: [{id: 0, s: 'd', n: 0}, ...rows],
    },
    {
      name: 'Add overlay at end',
      overlays: {
        add: {id: 4, s: 'd', n: 44},
        remove: undefined,
      },
      expected: [...rows, {id: 4, s: 'd', n: 44}],
    },
    {
      name: 'Add overlay middle',
      overlays: {
        add: {id: 2.5, s: 'b2', n: 225},
        remove: undefined,
      },
      expected: [rows[0], rows[1], {id: 2.5, s: 'b2', n: 225}, rows[2]],
    },
    {
      name: 'Add overlay replace',
      overlays: {
        add: {id: 2, s: 'b2', n: 225},
        remove: undefined,
      },
      expected: [rows[0], rows[1], {id: 2, s: 'b2', n: 225}, rows[2]],
    },

    {
      name: 'Remove overlay before start',
      overlays: {
        add: undefined,
        remove: {id: 0, s: 'z', n: -1},
      },
      expected: rows,
    },
    {
      name: 'Remove overlay start',
      overlays: {
        add: undefined,
        remove: {id: 1, s: 'a', n: 11},
      },
      expected: rows.slice(1),
    },
    {
      name: 'Remove overlay at end',
      overlays: {
        add: undefined,
        remove: {id: 3, s: 'c', n: 33},
      },
      expected: rows.slice(0, -1),
    },
    {
      name: 'Remove overlay middle',
      overlays: {
        add: undefined,
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [rows[0], rows[2]],
    },
    {
      name: 'Remove overlay after end',
      overlays: {
        add: undefined,
        remove: {id: 4, s: 'd', n: 44},
      },
      expected: rows,
    },

    // Two overlays
    {
      name: 'Basic edit',
      overlays: {
        add: {id: 2, s: 'b2', n: 225},
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [rows[0], {id: 2, s: 'b2', n: 225}, rows[2]],
    },
    {
      name: 'Edit first, still first',
      overlays: {
        add: {id: 0, s: 'a0', n: 0},
        remove: {id: 1, s: 'a', n: 11},
      },
      expected: [{id: 0, s: 'a0', n: 0}, rows[1], rows[2]],
    },
    {
      name: 'Edit first, now second',
      overlays: {
        add: {id: 2.5, s: 'a', n: 11},
        remove: {id: 1, s: 'a', n: 11},
      },
      expected: [rows[1], {id: 2.5, s: 'a', n: 11}, rows[2]],
    },
    {
      name: 'Edit first, now last',
      overlays: {
        add: {id: 3.5, s: 'a', n: 11},
        remove: {id: 1, s: 'a', n: 11},
      },
      expected: [rows[1], rows[2], {id: 3.5, s: 'a', n: 11}],
    },

    {
      name: 'Edit second, now first',
      overlays: {
        add: {id: 0, s: 'b', n: 22},
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [{id: 0, s: 'b', n: 22}, rows[0], rows[2]],
    },
    {
      name: 'Edit second, still second',
      overlays: {
        add: {id: 2.5, s: 'b', n: 22},
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [rows[0], {id: 2.5, s: 'b', n: 22}, rows[2]],
    },
    {
      name: 'Edit second, still second',
      overlays: {
        add: {id: 1.5, s: 'b', n: 22},
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [rows[0], {id: 1.5, s: 'b', n: 22}, rows[2]],
    },
    {
      name: 'Edit second, now last',
      overlays: {
        add: {id: 3.5, s: 'b', n: 22},
        remove: {id: 1, s: 'b', n: 22},
      },
      expected: [rows[1], rows[2], {id: 3.5, s: 'b', n: 22}],
    },

    {
      name: 'Edit last, now first',
      overlays: {
        add: {id: 0, s: 'c', n: 33},
        remove: {id: 3, s: 'c', n: 33},
      },
      expected: [{id: 0, s: 'c', n: 33}, rows[0], rows[1]],
    },
    {
      name: 'Edit last, now second',
      overlays: {
        add: {id: 1.5, s: 'c', n: 33},
        remove: {id: 3, s: 'c', n: 33},
      },
      expected: [rows[0], {id: 1.5, s: 'c', n: 33}, rows[1]],
    },
    {
      name: 'Edit last, still last',
      overlays: {
        add: {id: 3.5, s: 'c', n: 33},
        remove: {id: 3, s: 'c', n: 33},
      },
      expected: [rows[0], rows[1], {id: 3.5, s: 'c', n: 33}],
    },
    {
      name: 'Edit last, still last',
      overlays: {
        add: {id: 2.5, s: 'c', n: 33},
        remove: {id: 3, s: 'c', n: 33},
      },
      expected: [rows[0], rows[1], {id: 2.5, s: 'c', n: 33}],
    },
  ] as const)('$name', ({overlays, expected}) => {
    const actual = generateWithOverlayInner(rows, overlays, compare);
    expect([...actual].map(({row}) => row)).toEqual(expected);
  });
});

test('overlaysForConstraint', () => {
  expect(
    overlaysForConstraintForTest(
      {add: undefined, remove: undefined},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({add: undefined, remove: undefined});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b'}, remove: undefined},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({add: {a: 'b'}, remove: undefined});

  expect(
    overlaysForConstraintForTest(
      {add: undefined, remove: {a: 'b'}},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({add: undefined, remove: {a: 'b'}});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'c', b: '2'}, remove: {a: 'c', b: '1'}},
      {key: 'a', value: 'b'},
    ),
  ).toEqual({add: undefined, remove: undefined});
});

test('overlaysForStartAt', () => {
  const compare = (a: Row, b: Row) => (a.id as number) - (b.id as number);
  expect(
    overlaysForStartAtForTest(
      {add: undefined, remove: undefined},
      {id: 1},
      compare,
    ),
  ).toEqual({add: undefined, remove: undefined});
  expect(
    overlaysForStartAtForTest(
      {add: {id: 1}, remove: undefined},
      {id: 1},
      compare,
    ),
  ).toEqual({add: {id: 1}, remove: undefined});
  expect(
    overlaysForStartAtForTest(
      {add: {id: 1}, remove: undefined},
      {id: 0},
      compare,
    ),
  ).toEqual({add: {id: 1}, remove: undefined});
  expect(
    overlaysForStartAtForTest(
      {add: {id: 1}, remove: undefined},
      {id: 2},
      compare,
    ),
  ).toEqual({add: undefined, remove: undefined});
});

describe('filterOptionalFilters', () => {
  test('no filters', () => {
    expect(filterOptionalFilters(undefined).allApplied).toBe(true);
  });
  test('one simple filter', () => {
    expect(
      filterOptionalFilters({
        type: 'simple',
        field: 'a',
        value: 'b',
        op: '=',
      }).allApplied,
    ).toBe(true);
  });
  test('anded simple filters', () => {
    expect(
      filterOptionalFilters({
        type: 'and',
        conditions: [
          {type: 'simple', field: 'a', value: 'b', op: '='},
          {type: 'simple', field: 'c', value: 'd', op: '='},
        ],
      }).allApplied,
    ).toBe(true);
  });
  test('or with one simple filter', () => {
    expect(
      filterOptionalFilters({
        type: 'or',
        conditions: [{type: 'simple', field: 'a', value: 'b', op: '='}],
      }).allApplied,
    ).toBe(true);
  });
  test('or with anded simple filters', () => {
    expect(
      filterOptionalFilters({
        type: 'or',
        conditions: [
          {
            type: 'and',
            conditions: [
              {type: 'simple', field: 'a', value: 'b', op: '='},
              {type: 'simple', field: 'c', value: 'd', op: '='},
            ],
          },
        ],
      }).allApplied,
    ).toBe(true);
  });
  test('many ors', () => {
    expect(
      filterOptionalFilters({
        type: 'or',
        conditions: [
          {type: 'simple', field: 'a', value: 'b', op: '='},
          {type: 'simple', field: 'c', value: 'd', op: '='},
        ],
      }).allApplied,
    ).toBe(false);
  });
  test('anded ors', () => {
    expect(
      filterOptionalFilters({
        type: 'and',
        conditions: [
          {
            type: 'or',
            conditions: [
              {type: 'simple', field: 'a', value: 'b', op: '='},
              {type: 'simple', field: 'c', value: 'd', op: '='},
            ],
          },
        ],
      }).allApplied,
    ).toBe(false);
  });
});
