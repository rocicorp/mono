import Database from 'better-sqlite3';
import {describe, expect, test} from 'vitest';
import {TableSource} from './table-source.js';
import {Catch} from 'zql/src/zql/ivm2/catch.js';
import {makeComparator} from 'zql/src/zql/ivm2/data.js';
import {runCases} from 'zql/src/zql/ivm2/test/source-cases.js';
import {Ordering} from 'zql/src/zql/ast2/ast.js';
import {compile, sql} from '../internal/sql.js';
import {ValueType} from 'zql/src/zql/ivm2/schema.js';

const columns = {
  id: 'string',
  a: 'number',
  b: 'number',
  c: 'number',
} as const;

describe('fetching from a table source', () => {
  type Foo = {id: string; a: number; b: number; c: number};
  const allRows: Foo[] = [];
  const compoundOrder = [
    ['a', 'asc'],
    ['b', 'desc'],
    ['c', 'asc'],
  ] as const;
  const compoundComparator = makeComparator(compoundOrder);
  const db = new Database(':memory:');
  db.exec(/* sql */ `CREATE TABLE foo (id TEXT PRIMARY KEY, a, b, c);`);
  const stmt = db.prepare(
    /* sql */ `INSERT INTO foo (id, a, b, c) VALUES (?, ?, ?, ?);`,
  );
  let id = 0;
  for (let a = 1; a <= 3; ++a) {
    for (let b = 1; b <= 3; ++b) {
      for (let c = 1; c <= 3; ++c) {
        const row = [(++id).toString().padStart(2, '0'), a, b, c] as const;
        allRows.push({
          id: row[0],
          a: row[1],
          b: row[2],
          c: row[3],
        });
        stmt.run(...row);
      }
    }
  }

  test.each([
    {
      name: 'simple source with `id` order',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows,
    },
    {
      name: 'simple source with `id` order and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2),
    },
    {
      name: 'simple source with `id` order and start `before`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3),
    },
    {
      name: 'simple source with `id` order and start `before` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3).filter(r => r.b === 2),
    },
    {
      name: 'simple source with `id` order and start `after`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5),
    },
    {
      name: 'simple source with `id` order and start `after` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5).filter(r => r.b === 2),
    },
    {
      name: 'simple source with `id` order and start `at`',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4),
    },
    {
      name: 'simple source with `id` order and start `at` and constraint',
      sourceArgs: ['foo', columns, [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4).filter(r => r.b === 2),
    },
    {
      name: 'complex source with compound order',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows.slice().sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2).sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `before`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(3),
    },
    {
      name: 'complex source with compound order and start `before` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows
        .slice(3)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `after`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(5),
    },
    {
      name: 'complex source with compound order and start `after` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows
        .slice(5)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `at`',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(4),
    },
    {
      name: 'complex source with compound order and start `at` and constraint',
      sourceArgs: ['foo', columns, compoundOrder],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows
        .slice(4)
        .filter(r => r.b === 2)
        .sort(compoundComparator),
    },
  ] as const)('$name', ({sourceArgs, fetchArgs, expectedRows}) => {
    const source = new TableSource(
      db,
      sourceArgs[0],
      sourceArgs[1],
      sourceArgs[2],
      ['id'],
    );
    const out = new Catch(source);
    const rows = out.fetch(fetchArgs);
    expect(rows.map(r => r.row)).toEqual(expectedRows);
  });
});

test('pushing values does the correct writes', () => {
  const db = new Database(':memory:');
  db.exec(/* sql */ `CREATE TABLE foo (a, b, c, PRIMARY KEY (a, b));`);
  const read = db.prepare('SELECT * FROM foo');

  const source = new TableSource(
    db,
    'foo',
    {
      a: 'number',
      b: 'number',
      c: 'number',
    },
    [['a', 'asc']],
    ['a', 'b'],
  );

  /**
   * Test:
   * 1. add a row
   * 2. remove a row
   * 3. remove a row that doesn't exist throws
   * 4. add a row that already exists throws
   */
  source.push({
    type: 'add',
    row: {a: 1, b: 2, c: 3},
  });

  expect(read.all()).toEqual([{a: 1, b: 2, c: 3}]);

  source.push({
    type: 'remove',
    row: {a: 1, b: 2},
  });

  expect(read.all()).toEqual([]);

  expect(() => {
    source.push({
      type: 'remove',
      row: {a: 1, b: 2},
    });
  }).toThrow();

  source.push({
    type: 'add',
    row: {a: 1, b: 2, c: 3},
  });

  expect(() => {
    source.push({
      type: 'add',
      row: {a: 1, b: 2, c: 3},
    });
  }).toThrow();
});

describe('shared test cases', () => {
  runCases(
    (
      name: string,
      columns: Record<string, ValueType>,
      order: Ordering,
      primaryKeys: readonly [string, ...string[]],
    ) => {
      const db = new Database(':memory:');
      // create a table with desired columns and primary keys
      const query = compile(
        sql`CREATE TABLE ${sql.ident(name)} (${sql.join(
          Object.keys(columns).map(c => sql.ident(c)),
          sql`, `,
        )}, PRIMARY KEY (${sql.join(
          primaryKeys.map(p => sql.ident(p)),
          sql`, `,
        )}));`,
      );
      db.exec(query);
      return new TableSource(db, name, columns, order, primaryKeys);
    },
    new Set(),
    new Set(),
  );
});
