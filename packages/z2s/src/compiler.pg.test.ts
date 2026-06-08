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
        metadata JSONB NOT NULL
      );

      INSERT INTO docs (id, metadata) VALUES
        ('row1', '{"priority":"high","count":3,"flagged":true,"nested":{"zip":"94110"},"tags":["a","b"]}'),
        ('row2', '{"priority":"low","count":10,"flagged":false}'),
        ('row3', '{"priority":null}'),
        ('row4', '{}');
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

  // JSON path filters compiled to `#>>` extraction, executed against real
  // Postgres. Seed data (see beforeAll):
  //   row1 {priority:'high', count:3, flagged:true, nested:{zip:'94110'}, tags:['a','b']}
  //   row2 {priority:'low',  count:10, flagged:false}
  //   row3 {priority:null}            -- explicit JSON null
  //   row4 {}                         -- missing key
  const jsonRef = (...path: (string | number)[]): JsonPathReference => ({
    type: 'json',
    value: {type: 'column', name: 'metadata'},
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
    // row3 has an explicit JSON null, row4 is missing the key entirely; `#>>`
    // maps both to SQL NULL, matching SQLite/in-memory semantics.
    expect(await queryDocIds('IS', jsonRef('priority'), null)).toEqual([
      'row3',
      'row4',
    ]);
    expect(await queryDocIds('IS NOT', jsonRef('priority'), null)).toEqual([
      'row1',
      'row2',
    ]);
  });
});
