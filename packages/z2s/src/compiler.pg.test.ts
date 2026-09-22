import type {JSONValue} from 'postgres';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {
  JsonPathReference,
  LiteralValue,
  SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPgInternalConvert} from './sql.ts';

const DB_NAME = 'compiler-test';

const timesTable = table('timesTable')
  .from('times')
  .columns({
    id: string(),
    timeWithoutTz: number().from('time_without_tz'),
    timeWithoutTzArray: json<number[]>().from('time_without_tz_array'),
    nullableTimeWithoutTzArray: json<number[]>()
      .optional()
      .from('nullable_time_without_tz_array'),
    timeWithTz: number().from('time_with_tz'),
    timeWithTzArray: json<number[]>().from('time_with_tz_array'),
    nullableTimeWithTzArray: json<number[]>()
      .optional()
      .from('nullable_time_with_tz_array'),
  })
  .primaryKey('id');

const temporalsTable = table('temporalsTable')
  .from('temporals')
  .columns({
    id: string(),
    nullableDateArray: json<number[]>().optional().from('nullable_date_array'),
    nullableTimestampArray: json<number[]>()
      .optional()
      .from('nullable_timestamp_array'),
    nullableTimestamptzArray: json<number[]>()
      .optional()
      .from('nullable_timestamptz_array'),
  })
  .primaryKey('id');

const docsTable = table('docsTable')
  .from('docs')
  .columns({
    id: string(),
    metadata: json(),
    // A json<T[]>() column backed by a native Postgres array, not json/jsonb.
    labels: json<string[]>().optional(),
  })
  .primaryKey('id');

const schema = createSchema({tables: [timesTable, temporalsTable, docsTable]});

const serverSchema: ServerSchema = {
  times: {
    id: {type: 'text', isArray: false, isEnum: false},
    time_without_tz: {type: 'time', isArray: false, isEnum: false},
    time_without_tz_array: {type: 'time', isArray: true, isEnum: false},
    nullable_time_without_tz_array: {
      type: 'time',
      isArray: true,
      isEnum: false,
    },
    time_with_tz: {type: 'timetz', isArray: false, isEnum: false},
    time_with_tz_array: {type: 'timetz', isArray: true, isEnum: false},
    nullable_time_with_tz_array: {
      type: 'timetz',
      isArray: true,
      isEnum: false,
    },
  },
  temporals: {
    id: {type: 'text', isArray: false, isEnum: false},
    nullable_date_array: {type: 'date', isArray: true, isEnum: false},
    nullable_timestamp_array: {
      type: 'timestamp',
      isArray: true,
      isEnum: false,
    },
    nullable_timestamptz_array: {
      type: 'timestamptz',
      isArray: true,
      isEnum: false,
    },
  },
  docs: {
    id: {type: 'text', isArray: false, isEnum: false},
    metadata: {type: 'jsonb', isArray: false, isEnum: false},
    labels: {type: 'text', isArray: true, isEnum: false},
  },
};

describe('compiler with PostgreSQL', () => {
  let pg: PostgresDB;

  beforeAll(async () => {
    pg = await testDBs.create(DB_NAME);
    await pg.unsafe("SET TIME ZONE 'UTC'");
    await pg.unsafe(`
      CREATE TABLE times (
        id TEXT PRIMARY KEY,
        time_without_tz TIME NOT NULL,
        time_without_tz_array TIME[] NOT NULL,
        nullable_time_without_tz_array TIME[],
        time_with_tz TIMETZ NOT NULL,
        time_with_tz_array TIMETZ[] NOT NULL,
        nullable_time_with_tz_array TIMETZ[]
      );

      INSERT INTO times (
        id,
        time_without_tz,
        time_without_tz_array,
        nullable_time_without_tz_array,
        time_with_tz,
        time_with_tz_array,
        nullable_time_with_tz_array
      ) VALUES (
        'row1',
        '09:08:07.654',
        ARRAY['09:08:07.654'::time, '00:00:00'::time],
        NULL,
        '01:00:00+02',
        ARRAY['01:00:00+02'::timetz, '23:00:00-02'::timetz],
        NULL
      );

      CREATE TABLE temporals (
        id TEXT PRIMARY KEY,
        nullable_date_array DATE[],
        nullable_timestamp_array TIMESTAMP[],
        nullable_timestamptz_array TIMESTAMPTZ[]
      );

      INSERT INTO temporals (id) VALUES ('row1');

      CREATE TABLE docs (
        id TEXT PRIMARY KEY,
        metadata JSONB NOT NULL,
        labels TEXT[]
      );

      INSERT INTO docs (id, metadata, labels) VALUES
        ('row1', '{"priority":"high","count":3,"flagged":true,"nested":{"zip":"94110"},"tags":["a","b"]}', ARRAY['x', 'y']),
        ('row2', '{"priority":"low","count":10,"flagged":false}', ARRAY[]::TEXT[]),
        ('row3', '{"priority":null}', NULL),
        ('row4', '{}', NULL),
        ('row5', '{"priority":42,"count":"n/a","flagged":{"v":true},"tags":"not-an-array"}', NULL);
    `);
  });

  afterAll(async () => {
    await testDBs.drop(pg);
  });

  test('compiled reads match canonical PG time parsing', async () => {
    const raw = await pg.unsafe(`
      SELECT
        id,
        time_without_tz AS "timeWithoutTz",
        time_without_tz_array AS "timeWithoutTzArray",
        nullable_time_without_tz_array AS "nullableTimeWithoutTzArray",
        time_with_tz AS "timeWithTz",
        time_with_tz_array AS "timeWithTzArray",
        nullable_time_with_tz_array AS "nullableTimeWithTzArray"
      FROM times
      ORDER BY id
    `);

    expect(raw).toEqual([
      {
        id: 'row1',
        timeWithoutTz: 32887654,
        timeWithoutTzArray: [32887654, 0],
        nullableTimeWithoutTzArray: null,
        timeWithTz: 82800000,
        timeWithTzArray: [82800000, 3600000],
        nullableTimeWithTzArray: null,
      },
    ]);

    const sqlQuery = formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'timesTable',
        related: [],
      }),
    );

    const compiled = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );

    expect(compiled).toEqual(raw);
  });

  test('null date/timestamp/timestamptz arrays are preserved as null', async () => {
    const sqlQuery = formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'temporalsTable',
        related: [],
      }),
    );

    const compiled = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );

    expect(compiled).toEqual([
      {
        id: 'row1',
        nullableDateArray: null,
        nullableTimestampArray: null,
        nullableTimestamptzArray: null,
      },
    ]);
  });

  // JSON path filters compiled to typed `->` navigation plus `->>` extraction,
  // executed against real Postgres. Seed data (see beforeAll):
  //   row1 {priority:'high', count:3, flagged:true, nested:{zip:'94110'}, tags:['a','b']}
  //   row2 {priority:'low',  count:10, flagged:false}
  //   row3 {priority:null}            -- explicit JSON null
  //   row4 {}                         -- missing key
  //   row5 {priority:42, count:'n/a', flagged:{v:true}, tags:'not-an-array'}
  //                                   -- wrong JSON type at every path
  // and the native TEXT[] column `labels`: row1 ['x','y'], row2 [], others NULL.
  const jsonRef = (...path: (string | number)[]): JsonPathReference => ({
    type: 'json',
    value: {type: 'column', name: 'metadata'},
    path,
  });
  const labelsRef = (...path: (string | number)[]): JsonPathReference => ({
    type: 'json',
    value: {type: 'column', name: 'labels'},
    path,
  });

  const queryDocIds = async (
    op: SimpleCondition['op'],
    left: JsonPathReference,
    right: LiteralValue,
  ): Promise<string[]> => {
    const sqlQuery = formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'docsTable',
        related: [],
        where: {
          type: 'simple',
          op,
          left,
          right: {type: 'literal', value: right},
        },
      }),
    );
    const rows = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    ) as Array<{id: string}>;
    return rows.map(r => r.id).sort();
  };

  test('json path filter: string leaf equality', async () => {
    expect(await queryDocIds('=', jsonRef('priority'), 'high')).toEqual([
      'row1',
    ]);
    expect(await queryDocIds('!=', jsonRef('priority'), 'high')).toEqual([
      'row2',
      'row5',
    ]);
  });

  test('json path filter: numeric ordering', async () => {
    // Text ordering would put '10' < '3'; the ::double precision cast makes
    // this a real numeric comparison.
    expect(await queryDocIds('>', jsonRef('count'), 5)).toEqual(['row2']);
    expect(await queryDocIds('<', jsonRef('count'), 5)).toEqual(['row1']);
  });

  test('json path filter: boolean leaf equality', async () => {
    expect(await queryDocIds('=', jsonRef('flagged'), true)).toEqual(['row1']);
    expect(await queryDocIds('=', jsonRef('flagged'), false)).toEqual(['row2']);
    expect(await queryDocIds('IN', jsonRef('flagged'), [true])).toEqual([
      'row1',
    ]);
    expect(await queryDocIds('IN', jsonRef('flagged'), [false, true])).toEqual([
      'row1',
      'row2',
    ]);
  });

  test('json path filter: mismatched leaf types are non-matches, not errors', async () => {
    // row5 holds the wrong JSON type at every path. A bare `::double precision`
    // / `::boolean` cast of its `->>` text would make Postgres throw and fail
    // the whole query; the jsonb_typeof gate makes each a SQL NULL instead, so
    // the row is simply excluded — matching the in-memory predicate and SQLite,
    // which never throw.

    // A string leaf ("n/a") in a number comparison — ordering, equality, IN.
    expect(await queryDocIds('>', jsonRef('count'), 0)).toEqual([
      'row1',
      'row2',
    ]);
    expect(await queryDocIds('=', jsonRef('count'), 3)).toEqual(['row1']);
    expect(await queryDocIds('IN', jsonRef('count'), [3, 10])).toEqual([
      'row1',
      'row2',
    ]);

    // An object leaf ({v:true}) in a boolean comparison — `=` and `IS`.
    expect(await queryDocIds('=', jsonRef('flagged'), true)).toEqual(['row1']);
    expect(await queryDocIds('IS', jsonRef('flagged'), false)).toEqual([
      'row2',
    ]);
  });

  test('json path filter: comparisons are type-strict (no cross-type coercion)', async () => {
    // row5's priority is the *number* 42. `#>>` renders it as the text '42',
    // so an ungated text comparison would match it against the string '42' —
    // unlike the in-memory predicate (42 !== '42') and SQLite (integer ≠ text).
    // The jsonb_typeof gate makes the leaf NULL for a positive comparison...
    expect(await queryDocIds('=', jsonRef('priority'), '42')).toEqual([]);
    expect(await queryDocIds('IN', jsonRef('priority'), ['42'])).toEqual([]);
    expect(await queryDocIds('IS', jsonRef('priority'), '42')).toEqual([]);
    // ...and a mismatched-type leaf is *not equal*, so it matches a negated
    // comparison — while null/missing leaves (row3, row4) still never match
    // a value operator.
    expect(await queryDocIds('!=', jsonRef('priority'), '42')).toEqual([
      'row1',
      'row2',
      'row5',
    ]);
    expect(await queryDocIds('NOT IN', jsonRef('priority'), ['high'])).toEqual([
      'row2',
      'row5',
    ]);
    // An empty NOT IN list matches every non-null leaf — but not a null or
    // missing one (bare SQL `NOT (x = ANY('{}'))` would be TRUE for NULL).
    expect(await queryDocIds('NOT IN', jsonRef('priority'), [])).toEqual([
      'row1',
      'row2',
      'row5',
    ]);
    // A null list is constant-false for IN and NOT IN alike.
    expect(await queryDocIds('NOT IN', jsonRef('priority'), null)).toEqual([]);
    expect(await queryDocIds('IN', jsonRef('priority'), null)).toEqual([]);
    // Segments are strict on Postgres too (`->` with a typed operand): a
    // string segment never indexes an array — not even '-1' — and a number
    // never reads an object key.
    expect(await queryDocIds('=', jsonRef('tags', '0'), 'a')).toEqual([]);
    expect(await queryDocIds('=', jsonRef('tags', '-1'), 'b')).toEqual([]);
    expect(await queryDocIds('=', jsonRef('nested', 0), '94110')).toEqual([]);
    expect(await queryDocIds('=', jsonRef('tags', 0), 'a')).toEqual(['row1']);
    // IS NOT has no null guard, matching JS `lhs !== rhs`.
    expect(await queryDocIds('IS NOT', jsonRef('priority'), '42')).toEqual([
      'row1',
      'row2',
      'row3',
      'row4',
      'row5',
    ]);
    // Same rule with a number literal against a string leaf (row5.count = "n/a").
    expect(await queryDocIds('!=', jsonRef('count'), 3)).toEqual([
      'row2',
      'row5',
    ]);
    expect(await queryDocIds('NOT IN', jsonRef('count'), [3, 10])).toEqual([
      'row5',
    ]);
  });

  test('json path filter: nested object and array index', async () => {
    expect(await queryDocIds('=', jsonRef('nested', 'zip'), '94110')).toEqual([
      'row1',
    ]);
    expect(await queryDocIds('=', jsonRef('tags', 0), 'a')).toEqual(['row1']);
    expect(await queryDocIds('=', jsonRef('tags', 1), 'a')).toEqual([]);
  });

  test('json path filter: ILIKE / IN', async () => {
    expect(await queryDocIds('ILIKE', jsonRef('priority'), 'HI%')).toEqual([
      'row1',
    ]);
    expect(
      await queryDocIds('IN', jsonRef('priority'), ['high', 'low']),
    ).toEqual(['row1', 'row2']);
  });

  test('json path filter: IS NULL collapses missing key and JSON null', async () => {
    // row3 has an explicit JSON null, row4 is missing the key entirely; `->>`
    // maps both to SQL NULL, matching SQLite/in-memory semantics.
    expect(await queryDocIds('IS', jsonRef('priority'), null)).toEqual([
      'row3',
      'row4',
    ]);
    expect(await queryDocIds('IS NOT', jsonRef('priority'), null)).toEqual([
      'row1',
      'row2',
      'row5',
    ]);
  });

  test('json path filter: native array column (to_jsonb)', async () => {
    // `labels` is TEXT[], not json: the compiler wraps it in to_jsonb() so a
    // number segment indexes it like a JSON array.
    expect(await queryDocIds('=', labelsRef(0), 'x')).toEqual(['row1']);
    expect(await queryDocIds('=', labelsRef(1), 'y')).toEqual(['row1']);
    expect(await queryDocIds('=', labelsRef(1), 'x')).toEqual([]);
    expect(await queryDocIds('!=', labelsRef(0), 'z')).toEqual(['row1']);
    // A string segment is a key, never an index — also on a native array.
    expect(await queryDocIds('=', labelsRef('0'), 'x')).toEqual([]);
    // Out of range, an empty array and a NULL column all read as SQL NULL.
    expect(await queryDocIds('IS', labelsRef(2), null)).toEqual([
      'row1',
      'row2',
      'row3',
      'row4',
      'row5',
    ]);
    expect(await queryDocIds('IS', labelsRef(0), null)).toEqual([
      'row2',
      'row3',
      'row4',
      'row5',
    ]);
  });

  test('json path filter: equality is served by an expression index', async () => {
    // `=`/`IN` against a string or boolean literal compare the leaf as jsonb
    // with no CASE around the extraction, so an expression index on
    // `(col -> 'key')` matches the predicate. (Tiny table: the planner only
    // considers the index with sequential scans disabled.)
    await pg.unsafe(
      `CREATE INDEX docs_priority_idx ON docs ((metadata -> 'priority'))`,
    );
    const plan = async (op: SimpleCondition['op'], right: LiteralValue) => {
      const sqlQuery = formatPgInternalConvert(
        compile(serverSchema, schema, {
          table: 'docsTable',
          related: [],
          where: {
            type: 'simple',
            op,
            left: jsonRef('priority'),
            right: {type: 'literal', value: right},
          },
        }),
      );
      const rows = await pg.begin(async tx => {
        await tx.unsafe(`SET LOCAL enable_seqscan = off`);
        return tx.unsafe(
          `EXPLAIN (FORMAT JSON) ${sqlQuery.text}`,
          sqlQuery.values as JSONValue[],
        );
      });
      return JSON.stringify(rows);
    };
    expect(await plan('=', 'high')).toContain('docs_priority_idx');
    expect(await plan('IN', ['high', 'low'])).toContain('docs_priority_idx');
    // The type-gated forms are plain filters.
    expect(await plan('>', 'a')).not.toContain('docs_priority_idx');
    expect(await plan('!=', 'high')).not.toContain('docs_priority_idx');
  });
});
