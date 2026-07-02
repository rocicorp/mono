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
      "text": "SELECT "id","name" FROM "issues" WHERE ("id" >= ? AND (("id" > ?) OR ("id" = ? AND "name" < ?))) ORDER BY "id" asc, "name" desc",
      "values": [
        "issue-1",
        "issue-1",
        "issue-1",
        "z",
      ],
    }
  `);
});

test('optional cursor columns keep IS equality for tie-break groups; a non-null range bound needs no NULL guard', () => {
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
      "text": "SELECT "owner","id" FROM "issues" WHERE (("owner" > ?) OR ("owner" IS ? AND "id" > ?) OR ("owner" IS ? AND "id" = ?)) ORDER BY "owner" asc, "id" asc",
      "values": [
        "alice",
        "alice",
        "issue-1",
        "alice",
        "issue-1",
      ],
    }
  `);
});

test('a NULL cursor bound selects the strictly-after set, with or without column metadata', () => {
  // Replica-introspected specs historically carried no `optional` flag, so
  // the NULL handling must come from the bound value itself: strictly after
  // a NULL bound under SQLite's NULLS-first ordering is exactly the
  // non-NULL values, and the tie-break group needs the null-safe IS.
  const columns = {
    a: {type: 'number'},
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
          ['a', 'asc'],
          ['id', 'asc'],
        ],
        undefined,
        {
          row: {a: null, id: 'issue-5'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "a","id" FROM "issues" WHERE (("a" IS NOT NULL) OR ("a" IS ? AND "id" > ?)) ORDER BY "a" asc, "id" asc",
      "values": [
        null,
        "issue-5",
      ],
    }
  `);
});

test('a NULL cursor bound in a reverse walk yields the empty strictly-before set', () => {
  // Nothing sorts strictly before NULL under NULLS-first ordering, so the
  // range group must compile to FALSE — the previous `col < NULL` form was
  // never true either, but `col IS NULL OR col < ?` (the optional-column
  // form) would wrongly match the bound's own NULL group.
  const columns = {
    a: {type: 'number'},
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
          ['a', 'asc'],
          ['id', 'asc'],
        ],
        true,
        {
          row: {a: null, id: 'issue-5'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "a","id" FROM "issues" WHERE ((FALSE) OR ("a" IS ? AND "id" < ?)) ORDER BY "a" desc, "id" desc",
      "values": [
        null,
        "issue-5",
      ],
    }
  `);
});

test('a NULL cursor bound on a descending sort yields the empty strictly-after set', () => {
  // Under `ORDER BY a DESC` NULLs sort last, so nothing sorts strictly after
  // a NULL bound — the same truth-table cell as the reversed-ascending walk,
  // reached through the declared sort direction instead of `reverse`.
  const columns = {
    a: {type: 'number'},
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
          ['a', 'desc'],
          ['id', 'desc'],
        ],
        undefined,
        {
          row: {a: null, id: 'issue-5'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "a","id" FROM "issues" WHERE ((FALSE) OR ("a" IS ? AND "id" < ?)) ORDER BY "a" desc, "id" desc",
      "values": [
        null,
        "issue-5",
      ],
    }
  `);
});

test('basis at with a NULL bound keeps the anchor row reachable', () => {
  const columns = {
    a: {type: 'number'},
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
          ['a', 'asc'],
          ['id', 'asc'],
        ],
        undefined,
        {
          row: {a: null, id: 'issue-5'},
          basis: 'at',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "a","id" FROM "issues" WHERE (("a" IS NOT NULL) OR ("a" IS ? AND "id" > ?) OR ("a" IS ? AND "id" = ?)) ORDER BY "a" asc, "id" asc",
      "values": [
        null,
        "issue-5",
        null,
        "issue-5",
      ],
    }
  `);
});

test('a non-null bound on an optional column admits the NULL group when walking backward', () => {
  // NULLs sort before every non-NULL value, so the strictly-before set of a
  // non-NULL bound includes the whole NULL group; a bare `col < ?` silently
  // drops those rows from a reverse walk.
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
        true,
        {
          row: {owner: 'alice', id: 'issue-1'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "owner","id" FROM "issues" WHERE ((("owner" IS NULL OR "owner" < ?)) OR ("owner" IS ? AND "id" < ?)) ORDER BY "owner" desc, "id" desc",
      "values": [
        "alice",
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
      "text": "SELECT "id","org","rank" FROM "issues" WHERE "org" = ? AND "id" IN (?,?,?) AND ("rank" <= ? AND (("rank" < ?))) ORDER BY "rank" desc",
      "values": [
        "acme",
        "i1",
        "i2",
        "i3",
        100,
        100,
      ],
    }
  `);
});

test('start constraint adds a sargable leading-column bound', () => {
  const columns = {
    workspaceID: {type: 'string'},
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'number'},
  } as const satisfies Record<string, SchemaValue>;
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(`
    CREATE TABLE activity (
      workspaceID TEXT NOT NULL,
      a INTEGER NOT NULL,
      b INTEGER NOT NULL,
      c INTEGER NOT NULL
    );
    CREATE INDEX activity_sort ON activity(workspaceID, a DESC, b ASC, c ASC);
  `);

  const {text, values} = format(
    buildSelectQuery(
      'activity',
      columns,
      {workspaceID: 'w1'},
      undefined,
      [
        ['a', 'desc'],
        ['b', 'asc'],
        ['c', 'asc'],
      ],
      true,
      {row: {a: 500, b: 123, c: 99}, basis: 'after'},
    ),
  );
  const plan = db
    .prepare(`EXPLAIN QUERY PLAN ${text} LIMIT 2`)
    .all<{detail: string}>(...values)
    .map(r => r.detail)
    .join('\n');

  expect(text).toContain(`"workspaceID" = ? AND ("a" >= ? AND (("a" > ?)`);
  expect(plan).toMatch(/SEARCH activity USING (COVERING )?INDEX/);
  expect(plan).toMatch(/workspaceID=\? AND a>\?/);
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

test('ILIKE matches case-insensitively across Unicode (needs ICU lower())', () => {
  const lc = createSilentLogContext();
  const db = new Database(lc, ':memory:');
  db.exec(
    `CREATE TABLE t (name TEXT);
     INSERT INTO t VALUES ('MÜLLER'), ('Schmidt');`,
  );

  // Run the exact SQL the compiler emits for ILIKE.
  const {text, values} = likeSQL('ILIKE', 'müller');
  const rows = db
    .prepare(`SELECT name FROM t WHERE ${text}`)
    .all<{name: string}>(...values)
    .map(r => r.name);

  // ILIKE compiles to `lower(col) LIKE lower(pattern)`, so matching 'MÜLLER'
  // against 'müller' depends on lower() folding Ü -> ü. Only the Unicode-aware
  // lower() from @rocicorp/zero-sqlite3's ICU build (>= 1.1.0) does that; an
  // ASCII-only lower() leaves Ü untouched and this returns no rows.
  //
  // (Note: ß is deliberately NOT a good example here — case *folding* maps ß to
  // "ss", but lower() does not, so 'STRASSE' ILIKE 'straße' would not match even
  // with ICU. Umlauts/accents/Cyrillic are the clean cases.)
  expect(rows).toEqual(['MÜLLER']);
});
