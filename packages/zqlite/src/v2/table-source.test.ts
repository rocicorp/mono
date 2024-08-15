import Database from 'better-sqlite3';
import {describe, expect, test} from 'vitest';
import {TableSource} from './table-source.js';
import {CaptureOutput} from 'zql/src/zql/ivm2/capture-output.js';
import {makeComparator} from 'zql/src/zql/ivm2/data.js';

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
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows,
    },
    {
      name: 'simple source with `id` order and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2),
    },
    {
      name: 'simple source with `id` order and start `before`',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3),
    },
    {
      name: 'simple source with `id` order and start `before` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice(3).filter(r => r.b === 2),
    },
    {
      name: 'simple source with `id` order and start `after`',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5),
    },
    {
      name: 'simple source with `id` order and start `after` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice(5).filter(r => r.b === 2),
    },
    {
      name: 'simple source with `id` order and start `at`',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4),
    },
    {
      name: 'simple source with `id` order and start `at` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], [['id', 'asc']]],
      fetchArgs: {
        constraint: {key: 'b', value: 2},
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice(4).filter(r => r.b === 2),
    },
    {
      name: 'complex source with compound order',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
      fetchArgs: {constraint: undefined, start: undefined},
      expectedRows: allRows.slice().sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
      fetchArgs: {constraint: {key: 'a', value: 2}, start: undefined},
      expectedRows: allRows.filter(r => r.a === 2).sort(compoundComparator),
    },
    {
      name: 'complex source with compound order and start `before`',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'before'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(3),
    },
    {
      name: 'complex source with compound order and start `before` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
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
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'after'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(5),
    },
    {
      name: 'complex source with compound order and start `after` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
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
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
      fetchArgs: {
        constraint: undefined,
        start: {row: allRows[4], basis: 'at'},
      },
      expectedRows: allRows.slice().sort(compoundComparator).slice(4),
    },
    {
      name: 'complex source with compound order and start `at` and constraint',
      sourceArgs: ['foo', ['id', 'a', 'b', 'c'], compoundOrder],
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
    );
    const out = new CaptureOutput();
    const rows = [...source.fetch(fetchArgs, out)];
    expect(rows.map(r => r.row)).toEqual(expectedRows);
  });
});
