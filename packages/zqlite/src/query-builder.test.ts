import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {Database} from './db.ts';
import {format} from './internal/sql.ts';
import {
  buildSelectQuery,
  filtersToSQL,
  multiConstraintToSQL,
  type NoSubqueryCondition,
} from './query-builder.ts';

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

test('multiConstraints with single column emits IN list', () => {
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
        [[{id: 'i1'}, {id: 'i2'}, {id: 'i3'}]],
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

test('multiConstraints with compound key emits row-value VALUES', () => {
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
          [
            {a: 'x', b: 1},
            {a: 'y', b: 2},
          ],
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

test('multiConstraints single-column IN uses index (EXPLAIN QUERY PLAN)', () => {
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

test('multiConstraints + constraint + start + reverse compose into a single WHERE', () => {
  // Pin the AND-ordering and clause shape when all four FetchRequest
  // fields are set. This is the production combination FlippedJoin will
  // produce once wired up: a parent constraint, a batched IN list from
  // the child→parent keys, a paging cursor from a prior page, and reverse
  // ordering for descending pagination.
  const columns = {
    id: {type: 'string'},
    org: {type: 'string'},
    rank: {type: 'number'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        {org: 'acme'},
        undefined,
        [['rank', 'asc']],
        true,
        {row: {rank: 100}, basis: 'after'},
        [[{id: 'i1'}, {id: 'i2'}, {id: 'i3'}]],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "id","org","rank" FROM "issues" WHERE "org" = ? AND "id" IN (?,?,?) AND (("rank" < ?)) ORDER BY "rank" desc",
      "values": [
        "acme",
        "i1",
        "i2",
        "i3",
        100,
      ],
    }
  `);
});

test('multiConstraintToSQL asserts on empty multiConstraint', () => {
  const columns = {id: {type: 'string'}} as const satisfies Record<
    string,
    SchemaValue
  >;
  expect(() => multiConstraintToSQL([], columns)).toThrow(
    'multiConstraint must be non-empty',
  );
});

test('multiConstraintToSQL asserts on entries with no keys', () => {
  const columns = {id: {type: 'string'}} as const satisfies Record<
    string,
    SchemaValue
  >;
  expect(() => multiConstraintToSQL([{}], columns)).toThrow(
    'multiConstraint entries must have at least one key',
  );
});

test('multiConstraintToSQL asserts on entries with mismatched keys', () => {
  // Heterogeneous keys would silently produce wrong SQL bindings — the
  // builder picks keys from `multiConstraint[0]` and applies that shape
  // to every entry. Enforce the shared-shape contract loudly.
  const columns = {
    a: {type: 'string'},
    b: {type: 'number'},
  } as const satisfies Record<string, SchemaValue>;
  expect(() => multiConstraintToSQL([{a: 'x'}, {b: 1}], columns)).toThrow(
    /share the same keys/,
  );
  expect(() =>
    multiConstraintToSQL([{a: 'x', b: 1}, {a: 'y'}], columns),
  ).toThrow(/share the same keys/);
});

test('multiConstraints compound row-value IN uses index (EXPLAIN QUERY PLAN)', () => {
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

function likeSQL(
  op: 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE',
  pattern: string,
) {
  return format(
    filtersToSQL({
      type: 'simple',
      left: {type: 'column', name: 'name'},
      op,
      right: {type: 'literal', value: pattern},
    } as NoSubqueryCondition),
  );
}

test('LIKE is case-sensitive and uses an explicit backslash escape', () => {
  const {text, values} = likeSQL('LIKE', 'a%');
  // Bare LIKE operator; case-sensitivity comes from PRAGMA case_sensitive_like.
  expect(text).toBe(`"name" LIKE ? ESCAPE '\\'`);
  expect(values).toEqual(['a%']);
});

test('NOT LIKE keeps the operator and the backslash escape', () => {
  const {text, values} = likeSQL('NOT LIKE', 'a%');
  expect(text).toBe(`"name" NOT LIKE ? ESCAPE '\\'`);
  expect(values).toEqual(['a%']);
});

test('ILIKE lowers both operands for Unicode case-insensitive matching', () => {
  const {text, values} = likeSQL('ILIKE', 'A%');
  expect(text).toBe(`lower("name") LIKE lower(?) ESCAPE '\\'`);
  expect(values).toEqual(['A%']);
});

test('NOT ILIKE lowers both operands and negates', () => {
  const {text, values} = likeSQL('NOT ILIKE', 'A%');
  expect(text).toBe(`lower("name") NOT LIKE lower(?) ESCAPE '\\'`);
  expect(values).toEqual(['A%']);
});
