import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {Database} from './db.ts';
import {format} from './internal/sql.ts';
import {buildSelectQuery} from './query-builder.ts';

test('non-nullable cursor columns use range and equality operators without IS NULL guards', () => {
  const columns = {
    id: {type: 'string'},
    name: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        undefined,
        undefined,
        [
          ['id', 'asc'],
          ['name', 'desc'],
        ],
        undefined,
        {
          row: {id: 'issue-1', name: 'z'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "id","name" FROM "issues" WHERE (("id" > ?) OR ("id" = ? AND "name" < ?)) ORDER BY "id" asc, "name" desc",
      "values": [
        "issue-1",
        "issue-1",
        "z",
      ],
    }
  `);
});

test('optional cursor columns keep IS and IS NULL checks while non-nullable columns do not', () => {
  const columns = {
    owner: {type: 'string', optional: true},
    id: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        undefined,
        undefined,
        [
          ['owner', 'asc'],
          ['id', 'asc'],
        ],
        undefined,
        {
          row: {owner: 'alice', id: 'issue-1'},
          basis: 'at',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "owner","id" FROM "issues" WHERE (((? IS NULL OR "owner" > ?)) OR ("owner" IS ? AND "id" > ?) OR ("owner" IS ? AND "id" = ?)) ORDER BY "owner" asc, "id" asc",
      "values": [
        "alice",
        "alice",
        "alice",
        "issue-1",
        "alice",
        "issue-1",
      ],
    }
  `);
});

test('multiConstraint with single column emits IN list', () => {
  const columns = {
    id: {type: 'string'},
    name: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        undefined,
        undefined,
        [['id', 'asc']],
        undefined,
        undefined,
        [{id: 'i1'}, {id: 'i2'}, {id: 'i3'}],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "id","name" FROM "issues" WHERE "id" IN (?,?,?) ORDER BY "id" asc",
      "values": [
        "i1",
        "i2",
        "i3",
      ],
    }
  `);
});

test('multiConstraint with compound key emits row-value VALUES', () => {
  const columns = {
    a: {type: 'string'},
    b: {type: 'number'},
    c: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'pairs',
        columns,
        undefined,
        undefined,
        [['a', 'asc']],
        undefined,
        undefined,
        [
          {a: 'x', b: 1},
          {a: 'y', b: 2},
        ],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "a","b","c" FROM "pairs" WHERE ("a","b") IN (VALUES (?,?),(?,?)) ORDER BY "a" asc",
      "values": [
        "x",
        1,
        "y",
        2,
      ],
    }
  `);
});

test('multiConstraint single-column IN uses index (EXPLAIN QUERY PLAN)', () => {
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(`
    CREATE TABLE t (id INTEGER PRIMARY KEY, fk INTEGER NOT NULL);
    CREATE INDEX t_fk_idx ON t(fk);
    INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 10), (5, 20);
  `);
  const plan = db
    .prepare('EXPLAIN QUERY PLAN SELECT * FROM t WHERE fk IN (?, ?)')
    .all<{detail: string}>(10, 20)
    .map(r => r.detail)
    .join('\n');
  // Must be index-driven, not a full table scan.
  expect(plan).toMatch(/SEARCH t USING (COVERING )?INDEX/);
  expect(plan).not.toMatch(/SCAN t\b/);
});

test('multiConstraint compound row-value IN uses index (EXPLAIN QUERY PLAN)', () => {
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(`
    CREATE TABLE pairs (a TEXT, b INTEGER, c TEXT, PRIMARY KEY (a, b));
    INSERT INTO pairs VALUES ('x', 1, 'p'), ('y', 2, 'q'), ('x', 2, 'r');
  `);
  const plan = db
    .prepare(
      'EXPLAIN QUERY PLAN SELECT * FROM pairs WHERE (a, b) IN (VALUES (?,?), (?,?))',
    )
    .all<{detail: string}>('x', 1, 'y', 2)
    .map(r => r.detail)
    .join('\n');
  expect(plan).toMatch(/SEARCH pairs USING/);
});
