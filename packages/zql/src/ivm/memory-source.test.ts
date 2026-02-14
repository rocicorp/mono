import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Catch} from './catch.ts';
import type {Change} from './change.ts';
import {
  clearOpBuffersForTesting,
  debugMemorySource,
  generateWithOverlayInner,
  MemorySource,
  overlaysForConstraintForTest,
  overlaysForStartAtForTest,
} from './memory-source.ts';
import {compareRowsTest} from './test/compare-rows-test.ts';
import {createSource} from './test/source-factory.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import {consume} from './stream.ts';

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

  consume(
    ms.push({
      type: 'add',
      row: {a: 'a', b: 'b', c: 'c'},
    }),
  );

  const conn = ms.connect([['a', 'asc']]);
  const c = new Catch(conn);

  consume(
    ms.push({
      type: 'edit',
      oldRow: {a: 'a', b: 'b', c: 'c'},
      row: {a: 'a', b: 'b2', c: 'c2'},
    }),
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

  consume(
    ms.push({
      type: 'add',
      row: {a: 'a', b: 'b', c: 'c'},
    }),
  );

  const conn = ms.connect([['a', 'asc']]);
  let fetchDuringPush = undefined;
  conn.setOutput({
    push(change: Change) {
      expect(change).toEqual({
        type: 'edit',
        oldNode: {row: {a: 'a', b: 'b', c: 'c'}, relationships: {}},
        node: {row: {a: 'a', b: 'b2', c: 'c2'}, relationships: {}},
      });
      fetchDuringPush = [...conn.fetch({})];
      return emptyArray;
    },
  });

  consume(
    ms.push({
      type: 'edit',
      oldRow: {a: 'a', b: 'b', c: 'c'},
      row: {a: 'a', b: 'b2', c: 'c2'},
    }),
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

describe('debugMemorySource', () => {
  afterEach(() => {
    // Clean up after each test
    debugMemorySource.opBufferTables.clear();
    debugMemorySource.opBufferSize = 100_000;
    clearOpBuffersForTesting();
  });

  test('does not record ops when table is not in opBufferTables', () => {
    const ms = new MemorySource('test_table', {id: {type: 'string'}}, ['id']);

    // Push without enabling debug
    consume(ms.push({type: 'add', row: {id: '1'}}));

    // Try to remove a non-existent row - error should not include op history
    expect(() => {
      consume(ms.push({type: 'remove', row: {id: 'nonexistent'}}));
    }).toThrow(/No op history recorded/);
  });

  test('records ops and includes history in error when table is in opBufferTables', () => {
    debugMemorySource.opBufferTables.add('test_table');

    const ms = new MemorySource('test_table', {id: {type: 'string'}}, ['id']);

    // Push some operations
    consume(ms.push({type: 'add', row: {id: '1'}}, {reason: 'init'}));
    consume(ms.push({type: 'add', row: {id: '2'}}, {reason: {type: 'poke'}}));
    consume(
      ms.push(
        {type: 'edit', row: {id: '1'}, oldRow: {id: '1'}},
        {reason: {type: 'mutation', name: 'updateItem'}},
      ),
    );
    consume(ms.push({type: 'remove', row: {id: '1'}}, {reason: 'rebase'}));

    // Try to remove a row that doesn't exist anymore - error should include history
    expect(() => {
      consume(ms.push({type: 'remove', row: {id: '1'}}));
    }).toThrow(/Op history for id="1"/);
  });

  test('includes reason in op history', () => {
    debugMemorySource.opBufferTables.add('test_table');

    const ms = new MemorySource('test_table', {id: {type: 'string'}}, ['id']);

    consume(ms.push({type: 'add', row: {id: '1'}}, {reason: {type: 'poke'}}));
    consume(ms.push({type: 'remove', row: {id: '1'}}, {reason: 'test-reason'}));

    // Try to remove again - error should include reasons
    let error: Error | undefined;
    try {
      consume(ms.push({type: 'remove', row: {id: '1'}}));
    } catch (e) {
      error = e as Error;
    }

    expect(error).toBeDefined();
    expect(error!.message).toContain('poke');
    expect(error!.message).toContain('test-reason');
  });

  test('ring buffer respects opBufferSize limit', () => {
    debugMemorySource.opBufferTables.add('test_table');
    // Set buffer size to 5 - we'll push 6 ops then the failing one
    // So we should see: op3, op4, op5, op6, and the failing remove
    debugMemorySource.opBufferSize = 5;

    const ms = new MemorySource('test_table', {id: {type: 'string'}}, ['id']);

    // Push 6 operations on the same row
    consume(ms.push({type: 'add', row: {id: '1'}}, {reason: 'op1'}));
    consume(
      ms.push(
        {type: 'edit', row: {id: '1'}, oldRow: {id: '1'}},
        {reason: 'op2'},
      ),
    );
    consume(
      ms.push(
        {type: 'edit', row: {id: '1'}, oldRow: {id: '1'}},
        {reason: 'op3'},
      ),
    );
    consume(
      ms.push(
        {type: 'edit', row: {id: '1'}, oldRow: {id: '1'}},
        {reason: 'op4'},
      ),
    );
    consume(
      ms.push(
        {type: 'edit', row: {id: '1'}, oldRow: {id: '1'}},
        {reason: 'op5'},
      ),
    );
    consume(ms.push({type: 'remove', row: {id: '1'}}, {reason: 'op6'}));

    // Try to remove again - failing push is also recorded, so buffer will have:
    // op3, op4, op5, op6, failing-remove (5 items after op1, op2 are evicted)
    let error: Error | undefined;
    try {
      consume(ms.push({type: 'remove', row: {id: '1'}}));
    } catch (e) {
      error = e as Error;
    }

    expect(error).toBeDefined();
    // op1, op2 should have been evicted
    expect(error!.message).not.toContain('op1');
    expect(error!.message).not.toContain('op2');
    // op3, op4, op5, op6 should still be there
    expect(error!.message).toContain('op3');
    expect(error!.message).toContain('op4');
    expect(error!.message).toContain('op5');
    expect(error!.message).toContain('op6');
  });

  test('only records ops for enabled tables', () => {
    debugMemorySource.opBufferTables.add('enabled_table');

    const enabled = new MemorySource('enabled_table', {id: {type: 'string'}}, [
      'id',
    ]);
    const disabled = new MemorySource(
      'disabled_table',
      {id: {type: 'string'}},
      ['id'],
    );

    consume(enabled.push({type: 'add', row: {id: '1'}}, {reason: 'tracked'}));
    consume(
      disabled.push({type: 'add', row: {id: '1'}}, {reason: 'not-tracked'}),
    );

    // Enabled table should have history
    expect(() => {
      consume(enabled.push({type: 'remove', row: {id: 'nonexistent'}}));
    }).toThrow(/Op history for/);

    // Disabled table should not have history
    expect(() => {
      consume(disabled.push({type: 'remove', row: {id: 'nonexistent'}}));
    }).toThrow(/No op history recorded/);
  });

  test('handles compound primary keys in row key', () => {
    debugMemorySource.opBufferTables.add('compound_table');

    const ms = new MemorySource(
      'compound_table',
      {a: {type: 'string'}, b: {type: 'number'}},
      ['a', 'b'],
    );

    consume(
      ms.push({type: 'add', row: {a: 'x', b: 1}}, {reason: 'add-compound'}),
    );
    consume(
      ms.push(
        {type: 'remove', row: {a: 'x', b: 1}},
        {reason: 'remove-compound'},
      ),
    );

    // Error should include both key parts
    let error: Error | undefined;
    try {
      consume(ms.push({type: 'remove', row: {a: 'x', b: 1}}));
    } catch (e) {
      error = e as Error;
    }

    expect(error).toBeDefined();
    expect(error!.message).toContain('a="x"');
    expect(error!.message).toContain('b=1');
    expect(error!.message).toContain('add-compound');
    expect(error!.message).toContain('remove-compound');
  });
});
