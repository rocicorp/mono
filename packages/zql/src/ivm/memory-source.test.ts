import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Catch} from './catch.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {
  generateWithOverlay,
  generateWithOverlayInner,
  generateWithOverlayInnerUnordered,
  generateWithOverlayUnordered,
  MemorySource,
  mergeSortedStreams,
  overlaysForConstraintForTest,
  overlaysForMultiConstraintForTest,
  overlaysForStartAtForTest,
  type Overlay,
} from './memory-source.ts';
import type {MultiConstraint} from './operator.ts';
import type {Stream} from './stream.ts';
import {consume} from './stream.ts';
import {compareRowsTest} from './test/compare-rows-test.ts';
import {createSource} from './test/source-factory.ts';

import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
const lc = createSilentLogContext();

test('schema', () => {
  compareRowsTest((order: Ordering) => {
    const ms = createSource(lc, testLogConfig, 'table', {a: {type: 'string'}}, [
      'a',
    ]);
    return ms.connect(order).getSchema().compareRows;
  });
});

test('indexes remain after not needed', () => {
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
    JSON.stringify([
      ['a', 'asc'],
      ['c', 'asc'],
    ]),
  ]);

  conn2.destroy();
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

  conn1.destroy();
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
});

test('push edit change', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'string'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );

  consume(ms.push(makeSourceChangeAdd({a: 'a', b: 'b', c: 'c'})));

  const conn = ms.connect([['a', 'asc']]);
  const c = new Catch(conn);

  consume(
    ms.push(
      makeSourceChangeEdit(
        {a: 'a', b: 'b2', c: 'c2'},
        {a: 'a', b: 'b', c: 'c'},
      ),
    ),
  );
  expect(c.pushes).toMatchInlineSnapshot(`
    [
      {
        "oldRow": {
          "a": "a",
          "b": "b",
          "c": "c",
        },
        "row": {
          "a": "a",
          "b": "b2",
          "c": "c2",
        },
        "type": "edit",
      },
    ]
  `);
  expect(c.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": "a",
          "b": "b2",
          "c": "c2",
        },
      },
    ]
  `);

  conn.destroy();
});

test('fetch during push edit change', () => {
  const ms = createSource(
    lc,
    testLogConfig,
    'table',
    {a: {type: 'string'}, b: {type: 'string'}, c: {type: 'string'}},
    ['a'],
  );

  consume(ms.push(makeSourceChangeAdd({a: 'a', b: 'b', c: 'c'})));

  const conn = ms.connect([['a', 'asc']]);
  let fetchDuringPush = undefined;
  conn.setOutput({
    push(change: Change) {
      expect(change[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
      expect(change[ChangeIndex.NODE]).toEqual({
        row: {a: 'a', b: 'b2', c: 'c2'},
        relationships: {},
      });
      expect(change[ChangeIndex.OLD_NODE]).toEqual({
        row: {a: 'a', b: 'b', c: 'c'},
        relationships: {},
      });
      fetchDuringPush = [...conn.fetch({})];
      return emptyArray;
    },
  });

  consume(
    ms.push(
      makeSourceChangeEdit(
        {a: 'a', b: 'b2', c: 'c2'},
        {a: 'a', b: 'b', c: 'c'},
      ),
    ),
  );
  expect(fetchDuringPush).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "a": "a",
          "b": "b2",
          "c": "c2",
        },
      },
    ]
  `);
});

describe('fetch with req.filter', () => {
  test('non-PK filter returns only matching rows', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    consume(ms.push(makeSourceChangeAdd({a: 'a1', b: 'x'})));
    consume(ms.push(makeSourceChangeAdd({a: 'a2', b: 'y'})));
    consume(ms.push(makeSourceChangeAdd({a: 'a3', b: 'x'})));

    const conn = ms.connect([['a', 'asc']]);
    const rows = [
      ...conn.fetch({
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'b'},
          right: {type: 'literal', value: 'x'},
        },
      }),
    ].filter(n => n !== 'yield');

    expect(rows.map(n => n.row)).toEqual([
      {a: 'a1', b: 'x'},
      {a: 'a3', b: 'x'},
    ]);
    conn.destroy();
  });

  test('PK-equality filter drives indexed PK lookup', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {a: {type: 'string'}, b: {type: 'string'}},
      ['a'],
    );
    for (let i = 0; i < 100; i++) {
      consume(ms.push(makeSourceChangeAdd({a: `id-${i}`, b: `val-${i}`})));
    }

    const conn = ms.connect([['a', 'asc']]);
    const rows = [
      ...conn.fetch({
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'a'},
          right: {type: 'literal', value: 'id-42'},
        },
      }),
    ].filter(n => n !== 'yield');

    expect(rows.map(n => n.row)).toEqual([{a: 'id-42', b: 'val-42'}]);
    conn.destroy();
  });

  test('req.filter is ANDed with connection-time filter', () => {
    const ms = createSource(
      lc,
      testLogConfig,
      'table',
      {
        a: {type: 'string'},
        b: {type: 'string'},
        c: {type: 'string'},
      },
      ['a'],
    );
    consume(ms.push(makeSourceChangeAdd({a: '1', b: 'x', c: 'p'})));
    consume(ms.push(makeSourceChangeAdd({a: '2', b: 'x', c: 'q'})));
    consume(ms.push(makeSourceChangeAdd({a: '3', b: 'y', c: 'p'})));

    const conn = ms.connect([['a', 'asc']], {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'b'},
      right: {type: 'literal', value: 'x'},
    });

    const rows = [
      ...conn.fetch({
        filter: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'c'},
          right: {type: 'literal', value: 'p'},
        },
      }),
    ].filter(n => n !== 'yield');

    expect(rows.map(n => n.row)).toEqual([{a: '1', b: 'x', c: 'p'}]);
    conn.destroy();
  });
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
    expect(Array.from(actual, ({row}) => row)).toEqual(expected);
  });
});

test('overlaysForConstraint', () => {
  expect(
    overlaysForConstraintForTest({add: undefined, remove: undefined}, {a: 'b'}),
  ).toEqual({add: undefined, remove: undefined});

  expect(
    overlaysForConstraintForTest({add: {a: 'b'}, remove: undefined}, {a: 'b'}),
  ).toEqual({add: {a: 'b'}, remove: undefined});

  expect(
    overlaysForConstraintForTest({add: undefined, remove: {a: 'b'}}, {a: 'b'}),
  ).toEqual({add: undefined, remove: {a: 'b'}});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}},
      {a: 'b'},
    ),
  ).toEqual({add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'c', b: '2'}, remove: {a: 'c', b: '1'}},
      {a: 'b'},
    ),
  ).toEqual({add: undefined, remove: undefined});

  // Compound key constraints
  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}},
      {a: 'b', b: '2'},
    ),
  ).toEqual({add: {a: 'b', b: '2'}, remove: undefined});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}},
      {a: 'b', b: '1'},
    ),
  ).toEqual({add: undefined, remove: {a: 'b', b: '1'}});

  expect(
    overlaysForConstraintForTest(
      {add: {a: 'b', b: '2'}, remove: {a: 'b', b: '1'}},
      {a: 'b', b: '3'},
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

describe('overlaysForMultiConstraint', () => {
  test('add overlay matching one IN entry is kept', () => {
    expect(
      overlaysForMultiConstraintForTest({add: {id: 'i2'}, remove: undefined}, [
        {id: 'i1'},
        {id: 'i2'},
        {id: 'i3'},
      ]),
    ).toEqual({add: {id: 'i2'}, remove: undefined});
  });

  test('add overlay matching no IN entry is dropped', () => {
    expect(
      overlaysForMultiConstraintForTest({add: {id: 'i9'}, remove: undefined}, [
        {id: 'i1'},
        {id: 'i2'},
      ]),
    ).toEqual({add: undefined, remove: undefined});
  });

  test('remove overlay matching one IN entry is kept', () => {
    expect(
      overlaysForMultiConstraintForTest({add: undefined, remove: {id: 'i1'}}, [
        {id: 'i1'},
        {id: 'i2'},
      ]),
    ).toEqual({add: undefined, remove: {id: 'i1'}});
  });

  test('remove overlay matching no IN entry is dropped', () => {
    expect(
      overlaysForMultiConstraintForTest({add: undefined, remove: {id: 'i9'}}, [
        {id: 'i1'},
        {id: 'i2'},
      ]),
    ).toEqual({add: undefined, remove: undefined});
  });

  test('add and remove are filtered independently', () => {
    // add matches, remove does not
    expect(
      overlaysForMultiConstraintForTest({add: {id: 'i1'}, remove: {id: 'i9'}}, [
        {id: 'i1'},
        {id: 'i2'},
      ]),
    ).toEqual({add: {id: 'i1'}, remove: undefined});

    // remove matches, add does not
    expect(
      overlaysForMultiConstraintForTest({add: {id: 'i9'}, remove: {id: 'i2'}}, [
        {id: 'i1'},
        {id: 'i2'},
      ]),
    ).toEqual({add: undefined, remove: {id: 'i2'}});
  });

  test('compound-key IN entry — full match required per entry', () => {
    // (a, b) IN ((1, 'x'), (2, 'y'))
    const mc = [
      {a: 1, b: 'x'},
      {a: 2, b: 'y'},
    ];
    // matches entry 1 exactly
    expect(
      overlaysForMultiConstraintForTest(
        {add: {a: 2, b: 'y', c: 'extra'}, remove: undefined},
        mc,
      ),
    ).toEqual({add: {a: 2, b: 'y', c: 'extra'}, remove: undefined});
    // partial match (a matches but b doesn't) → not in IN
    expect(
      overlaysForMultiConstraintForTest(
        {add: {a: 1, b: 'y'}, remove: undefined},
        mc,
      ),
    ).toEqual({add: undefined, remove: undefined});
  });

  test('row missing a constrained column does not match', () => {
    // constraint requires `id`, but overlay row only has `name`
    expect(
      overlaysForMultiConstraintForTest(
        {add: {name: 'foo'} as Row, remove: undefined},
        [{id: 'i1'}],
      ),
    ).toEqual({add: undefined, remove: undefined});
  });
});

describe('generateWithOverlayInnerUnordered', () => {
  const rows = [
    {id: 1, s: 'a', n: 11},
    {id: 2, s: 'b', n: 22},
    {id: 3, s: 'c', n: 33},
  ];

  const pk = ['id'] as const;

  test.each([
    {
      name: 'No overlay',
      overlays: {add: undefined, remove: undefined},
      expected: rows,
    },
    {
      name: 'Add overlay — yields added row first',
      overlays: {add: {id: 4, s: 'd', n: 44}, remove: undefined},
      expected: [{id: 4, s: 'd', n: 44}, ...rows],
    },
    {
      name: 'Remove overlay start',
      overlays: {add: undefined, remove: {id: 1, s: 'a', n: 11}},
      expected: [rows[1], rows[2]],
    },
    {
      name: 'Remove overlay middle',
      overlays: {add: undefined, remove: {id: 2, s: 'b', n: 22}},
      expected: [rows[0], rows[2]],
    },
    {
      name: 'Remove overlay end',
      overlays: {add: undefined, remove: {id: 3, s: 'c', n: 33}},
      expected: [rows[0], rows[1]],
    },
    {
      name: 'Remove overlay not found — yields all rows',
      overlays: {add: undefined, remove: {id: 99, s: 'z', n: 0}},
      expected: rows,
    },
    {
      name: 'Edit (add + remove) — new row first, old row suppressed',
      overlays: {
        add: {id: 2, s: 'b2', n: 225},
        remove: {id: 2, s: 'b', n: 22},
      },
      expected: [{id: 2, s: 'b2', n: 225}, rows[0], rows[2]],
    },
    {
      name: 'Edit position change — different non-PK values',
      overlays: {
        add: {id: 5, s: 'e', n: 55},
        remove: {id: 1, s: 'a', n: 11},
      },
      expected: [{id: 5, s: 'e', n: 55}, rows[1], rows[2]],
    },
    {
      name: 'Empty stream with add',
      overlays: {add: {id: 1, s: 'a', n: 11}, remove: undefined},
      rows: [],
      expected: [{id: 1, s: 'a', n: 11}],
    },
    {
      name: 'Empty stream no overlay',
      overlays: {add: undefined, remove: undefined},
      rows: [],
      expected: [],
    },
  ] as const)('$name', c => {
    const input = 'rows' in c ? c.rows : rows;
    const actual = Array.from(
      generateWithOverlayInnerUnordered(input, c.overlays, pk),
      ({row}) => row,
    );
    expect(actual).toEqual(c.expected);
  });

  test('Compound primary key — matches on all PK columns', () => {
    const compoundRows = [
      {a: 1, b: 'x', v: 10},
      {a: 1, b: 'y', v: 20},
      {a: 2, b: 'x', v: 30},
    ];
    const compoundPK = ['a', 'b'] as const;
    const actual = Array.from(
      generateWithOverlayInnerUnordered(
        compoundRows,
        {add: undefined, remove: {a: 1, b: 'y', v: 20}},
        compoundPK,
      ),
      ({row}) => row,
    );
    expect(actual).toEqual([compoundRows[0], compoundRows[2]]);
  });

  test('Compound PK partial match — does not suppress', () => {
    const compoundRows = [
      {a: 1, b: 'x', v: 10},
      {a: 1, b: 'y', v: 20},
      {a: 2, b: 'x', v: 30},
    ];
    const compoundPK = ['a', 'b'] as const;
    const actual = Array.from(
      generateWithOverlayInnerUnordered(
        compoundRows,
        {add: undefined, remove: {a: 1, b: 'z', v: 0}},
        compoundPK,
      ),
      ({row}) => row,
    );
    expect(actual).toEqual(compoundRows);
  });
});

describe('generateWithOverlayUnordered', () => {
  const rows = [
    {id: 1, s: 'a', n: 11},
    {id: 2, s: 'b', n: 22},
    {id: 3, s: 'c', n: 33},
  ];

  const pk = ['id'] as const;

  test('Epoch gating — overlay skipped when lastPushedEpoch < overlay.epoch', () => {
    const overlay = {
      epoch: 5,
      change: makeSourceChangeAdd({id: 4, s: 'd', n: 44}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, undefined, overlay, 4, pk),
      ({row}) => row,
    );
    expect(actual).toEqual(rows);
  });

  test('Epoch gating — overlay applied when lastPushedEpoch >= overlay.epoch', () => {
    const overlay = {
      epoch: 5,
      change: makeSourceChangeAdd({id: 4, s: 'd', n: 44}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, undefined, overlay, 5, pk),
      ({row}) => row,
    );
    expect(actual).toEqual([{id: 4, s: 'd', n: 44}, ...rows]);
  });

  test('Constraint filtering — overlay filtered out by constraint', () => {
    const overlay = {
      epoch: 1,
      change: makeSourceChangeAdd({id: 4, s: 'd', n: 44}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, {s: 'a'}, overlay, 1, pk),
      ({row}) => row,
    );
    expect(actual).toEqual(rows);
  });

  test('Filter predicate — overlay filtered out by predicate', () => {
    const overlay = {
      epoch: 1,
      change: makeSourceChangeAdd({id: 4, s: 'd', n: 44}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(
        rows,
        undefined,
        overlay,
        1,
        pk,
        (row: Row) => (row.n as number) < 40,
      ),
      ({row}) => row,
    );
    expect(actual).toEqual(rows);
  });

  test('Add change type', () => {
    const overlay = {
      epoch: 1,
      change: makeSourceChangeAdd({id: 4, s: 'd', n: 44}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, undefined, overlay, 1, pk),
      ({row}) => row,
    );
    expect(actual).toEqual([{id: 4, s: 'd', n: 44}, ...rows]);
  });

  test('Remove change type', () => {
    const overlay = {
      epoch: 1,
      change: makeSourceChangeRemove({id: 2, s: 'b', n: 22}),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, undefined, overlay, 1, pk),
      ({row}) => row,
    );
    expect(actual).toEqual([rows[0], rows[2]]);
  });

  test('Edit change type', () => {
    const overlay = {
      epoch: 1,
      change: makeSourceChangeEdit(
        {id: 2, s: 'b2', n: 225},
        {id: 2, s: 'b', n: 22},
      ),
    };
    const actual = Array.from(
      generateWithOverlayUnordered(rows, undefined, overlay, 1, pk),
      ({row}) => row,
    );
    expect(actual).toEqual([{id: 2, s: 'b2', n: 225}, rows[0], rows[2]]);
  });
});

describe('multiConstraints overlay handling — both helpers', () => {
  // Shared scenarios for `generateWithOverlay` (ordered) and
  // `generateWithOverlayUnordered`. Both helpers accept `multiConstraints`
  // and apply it to the overlay add/remove sides identically. They diverge
  // only in *output ordering* (ordered emits at sort position; unordered
  // prepends an ADD overlay), so each scenario specifies both expected
  // shapes.
  //
  // The ordered helper is exercised in production by TableSource
  // (table-source.ts:309); MemorySource's #fetchMulti post-filters at a
  // higher layer and passes `multiConstraints: undefined` to the helper.
  // These tests pin the helper's contract independently of either source.
  type Scenario = {
    name: string;
    iteratorRows?: readonly Row[];
    overlay: Overlay;
    multiConstraints: readonly MultiConstraint[];
    expectedUnordered: readonly Row[];
    expectedOrdered: readonly Row[];
  };

  const rows: readonly Row[] = [
    {id: 1, s: 'a', n: 11},
    {id: 2, s: 'b', n: 22},
    {id: 3, s: 'c', n: 33},
  ];
  const pk = ['id'] as const;
  const compare = (a: Row, b: Row) => (a.id as number) - (b.id as number);

  const ADD_4 = {id: 4, s: 'd', n: 44};
  const ADD_2_b2 = {id: 2, s: 'b2', n: 225};

  const scenarios: Scenario[] = [
    {
      name: 'add overlay matching IN list is yielded',
      overlay: {epoch: 1, change: makeSourceChangeAdd(ADD_4)},
      multiConstraints: [[{id: 1}, {id: 4}, {id: 7}]],
      expectedUnordered: [ADD_4, ...rows],
      expectedOrdered: [...rows, ADD_4],
    },
    {
      name: 'add overlay outside IN list is dropped',
      overlay: {epoch: 1, change: makeSourceChangeAdd(ADD_4)},
      multiConstraints: [[{id: 1}, {id: 2}]],
      expectedUnordered: rows,
      expectedOrdered: rows,
    },
    {
      name: 'remove overlay matching IN list suppresses row',
      overlay: {epoch: 1, change: makeSourceChangeRemove(rows[1])},
      multiConstraints: [[{id: 1}, {id: 2}]],
      expectedUnordered: [rows[0], rows[2]],
      expectedOrdered: [rows[0], rows[2]],
    },
    {
      // The remove overlay's row is not in the IN list → multi filters it
      // out. The underlying iterator already excludes the removed row
      // from its scan (storage write happens after this generator), so
      // we simulate that with `iteratorRows`.
      name: 'remove overlay outside IN list does NOT suppress row',
      iteratorRows: [rows[0], rows[2]],
      overlay: {epoch: 1, change: makeSourceChangeRemove(rows[1])},
      multiConstraints: [[{id: 99}]],
      expectedUnordered: [rows[0], rows[2]],
      expectedOrdered: [rows[0], rows[2]],
    },
    {
      // ADD side {id:2,b2} matches IN → injected. REMOVE side {id:1}
      // doesn't match → dropped, so rows[0] (id=1) survives. The ADD
      // appears alongside the existing id=2 row in both orderings; the
      // inner generator does not treat compare-equal as replacement
      // (see `generateWithOverlayInner > Add overlay replace`).
      name: 'edit overlay where new matches but old does not',
      overlay: {
        epoch: 1,
        change: makeSourceChangeEdit(ADD_2_b2, {id: 1, s: 'a', n: 11}),
      },
      multiConstraints: [[{id: 2}, {id: 3}]],
      expectedUnordered: [ADD_2_b2, ...rows],
      expectedOrdered: [rows[0], rows[1], ADD_2_b2, rows[2]],
    },
    {
      name: 'multiple entries are ANDed (match)',
      overlay: {epoch: 1, change: makeSourceChangeAdd(ADD_4)},
      multiConstraints: [[{id: 4}, {id: 5}], [{s: 'd'}]],
      expectedUnordered: [ADD_4, ...rows],
      expectedOrdered: [...rows, ADD_4],
    },
    {
      name: 'multiple entries are ANDed (one IN list rejects)',
      overlay: {epoch: 1, change: makeSourceChangeAdd(ADD_4)},
      multiConstraints: [[{id: 4}, {id: 5}], [{s: 'q'}]],
      expectedUnordered: rows,
      expectedOrdered: rows,
    },
    {
      // The mc.length > 0 guard means an empty MultiConstraint is a no-op.
      name: 'empty entry (length 0) is ignored, not treated as mismatch',
      overlay: {epoch: 1, change: makeSourceChangeAdd(ADD_4)},
      multiConstraints: [[]],
      expectedUnordered: [ADD_4, ...rows],
      expectedOrdered: [...rows, ADD_4],
    },
  ];

  describe.each(scenarios)(
    '$name',
    ({
      iteratorRows,
      overlay,
      multiConstraints,
      expectedUnordered,
      expectedOrdered,
    }) => {
      const input = iteratorRows ?? rows;

      test('unordered', () => {
        const actual = Array.from(
          generateWithOverlayUnordered(
            input,
            undefined,
            overlay,
            1,
            pk,
            undefined,
            multiConstraints,
          ),
          ({row}) => row,
        );
        expect(actual).toEqual(expectedUnordered);
      });

      test('ordered', () => {
        const actual = Array.from(
          generateWithOverlay(
            undefined,
            input,
            undefined,
            overlay,
            1,
            compare,
            undefined,
            multiConstraints,
          ),
          ({row}) => row,
        );
        expect(actual).toEqual(expectedOrdered);
      });
    },
  );
});

describe('mergeSortedStreams', () => {
  const node = (id: number): Node => ({row: {id}, relationships: {}});
  const ids = (xs: Iterable<Node | 'yield'>): (number | 'yield')[] =>
    Array.from(xs, x => (x === 'yield' ? 'yield' : (x.row.id as number)));
  const byId = (a: Node, b: Node) =>
    (a.row.id as number) - (b.row.id as number);

  function* gen(values: readonly (Node | 'yield')[]): Stream<Node | 'yield'> {
    for (const v of values) {
      yield v;
    }
  }

  test('no streams yields nothing', () => {
    expect(ids(mergeSortedStreams([], byId))).toEqual([]);
  });

  test('single empty stream yields nothing', () => {
    expect(ids(mergeSortedStreams([gen([])], byId))).toEqual([]);
  });

  test('all empty streams yields nothing', () => {
    expect(ids(mergeSortedStreams([gen([]), gen([]), gen([])], byId))).toEqual(
      [],
    );
  });

  test('single non-empty stream is passed through in order', () => {
    expect(
      ids(mergeSortedStreams([gen([node(1), node(2), node(3)])], byId)),
    ).toEqual([1, 2, 3]);
  });

  test('two streams are interleaved in compare order', () => {
    const a = gen([node(1), node(3), node(5)]);
    const b = gen([node(2), node(4), node(6)]);
    expect(ids(mergeSortedStreams([a, b], byId))).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('three streams of varying lengths', () => {
    const a = gen([node(1), node(10)]);
    const b = gen([node(2), node(3), node(4), node(11)]);
    const c = gen([node(5)]);
    expect(ids(mergeSortedStreams([a, b, c], byId))).toEqual([
      1, 2, 3, 4, 5, 10, 11,
    ]);
  });

  test('one empty stream among non-empty merges correctly', () => {
    const a = gen([node(1), node(3)]);
    const empty = gen([]);
    const c = gen([node(2), node(4)]);
    expect(ids(mergeSortedStreams([a, empty, c], byId))).toEqual([1, 2, 3, 4]);
  });

  test('equal-key rows across streams are all emitted', () => {
    const a = gen([node(1), node(2)]);
    const b = gen([node(1), node(2)]);
    const c = gen([node(2), node(3)]);
    const out = ids(mergeSortedStreams([a, b, c], byId));
    expect(out).toEqual([1, 1, 2, 2, 2, 3]);
  });

  test('reverse order via comparator', () => {
    const a = gen([node(5), node(3), node(1)]);
    const b = gen([node(6), node(4), node(2)]);
    const reverse = (x: Node, y: Node) => byId(y, x);
    expect(ids(mergeSortedStreams([a, b], reverse))).toEqual([
      6, 5, 4, 3, 2, 1,
    ]);
  });

  test("'yield' forwarded during priming", () => {
    const a = gen(['yield', node(1), node(3)]);
    const b = gen([node(2), node(4)]);
    // Priming both iterators yields the 'yield' from `a`'s first slot
    // before any Nodes are emitted.
    expect(ids(mergeSortedStreams([a, b], byId))).toEqual([
      'yield',
      1,
      2,
      3,
      4,
    ]);
  });

  test("'yield' forwarded during advance", () => {
    // After yielding node(1) the merge calls advance(0), which sees the
    // 'yield' before node(3) and forwards it immediately — so the
    // 'yield' lands between node(1) and node(2), not in id order.
    const a = gen([node(1), 'yield', node(3)]);
    const b = gen([node(2), node(4)]);
    expect(ids(mergeSortedStreams([a, b], byId))).toEqual([
      1,
      'yield',
      2,
      3,
      4,
    ]);
  });

  test("multiple 'yield's between rows are all forwarded", () => {
    const a = gen([node(1), 'yield', 'yield', node(3)]);
    const b = gen([node(2)]);
    expect(ids(mergeSortedStreams([a, b], byId))).toEqual([
      1,
      'yield',
      'yield',
      2,
      3,
    ]);
  });

  test('output preserves global sort invariant on randomized input', () => {
    // Generate a few independently-sorted streams and assert the merged
    // output is globally sorted. Keeps the test deterministic via a fixed
    // shuffle.
    const data = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3];
    const buckets: number[][] = [[], [], [], []];
    for (let i = 0; i < data.length; i++) {
      buckets[i % buckets.length].push(data[i]);
    }
    const streams = buckets.map(b => gen(b.sort((x, y) => x - y).map(node)));
    const out = ids(mergeSortedStreams(streams, byId)) as number[];
    for (let i = 1; i < out.length; i++) {
      expect(out[i]).toBeGreaterThanOrEqual(out[i - 1]);
    }
    expect(out.length).toBe(data.length);
  });

  test('.return() propagates to un-exhausted sub-iterators on early termination', () => {
    // Build streams that record whether .return() was invoked.
    const returned: boolean[] = [false, false, false];
    const trackable = (i: number, values: readonly Node[]): Stream<Node> => ({
      [Symbol.iterator]() {
        let idx = 0;
        return {
          next() {
            if (idx < values.length) {
              return {value: values[idx++], done: false};
            }
            return {value: undefined, done: true};
          },
          return(v?: unknown) {
            returned[i] = true;
            return {value: v, done: true};
          },
          [Symbol.iterator]() {
            return this;
          },
        };
      },
    });

    const a = trackable(0, [node(1), node(4), node(7)]);
    const b = trackable(1, [node(2), node(5), node(8)]);
    const c = trackable(2, [node(3), node(6), node(9)]);

    const merged = mergeSortedStreams([a, b, c], byId);
    const it = merged[Symbol.iterator]();
    // Pull a couple values then break — JS would call .return() under
    // for-of `break`; we invoke explicitly.
    expect(it.next().value).toEqual(node(1));
    expect(it.next().value).toEqual(node(2));
    it.return?.();

    // All three sub-iterators still had un-yielded rows, so all should
    // have been .return()-d via mergeSortedStreams's finally block.
    expect(returned).toEqual([true, true, true]);
  });

  test('.return() not called on already-exhausted sub-iterators', () => {
    let aReturned = false;
    let bReturned = false;
    const trackable = (
      values: readonly Node[],
      onReturn: () => void,
    ): Stream<Node> => ({
      [Symbol.iterator]() {
        let idx = 0;
        return {
          next() {
            if (idx < values.length) {
              return {value: values[idx++], done: false};
            }
            return {value: undefined, done: true};
          },
          return(v?: unknown) {
            onReturn();
            return {value: v, done: true};
          },
          [Symbol.iterator]() {
            return this;
          },
        };
      },
    });

    // `a` will be drained; `b` will still have rows when we early-exit.
    const a = trackable([node(1)], () => (aReturned = true));
    const b = trackable([node(2), node(3), node(4)], () => (bReturned = true));

    const it = mergeSortedStreams([a, b], byId)[Symbol.iterator]();
    expect(it.next().value).toEqual(node(1)); // from a — exhausts a
    expect(it.next().value).toEqual(node(2)); // from b
    it.return?.();

    // a was exhausted naturally; the merge marks heads[0]=null and skips
    // .return() on it. b still had rows, so it must be .return()'d.
    expect(aReturned).toBe(false);
    expect(bReturned).toBe(true);
  });
});
