import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {beforeEach, describe, expect} from 'vitest';
import {stringify} from '../../../shared/src/bigint-json.ts';
import {type PgTest, test} from '../test/db.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../types/lite.ts';
import {type PostgresDB, type PostgresValueType} from '../types/pg.ts';
import {
  BinaryCopyParser,
  hasBinaryDecoder,
  makeBinaryDecoder,
  textCastDecoder,
} from './pg-copy-binary.ts';
import {TsvParser} from './pg-copy.ts';
import {getTypeParsers} from './pg-type-parser.ts';

describe('pg-copy-binary', () => {
  let sql: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    sql = await testDBs.create('pg_copy_binary_test');
    return () => testDBs.drop(sql);
  });

  /**
   * Copies table data via text format and returns LiteValueType[] per field.
   */
  async function copyText(
    tableName: string,
    columns: {name: string; typeOID: number; dataType: string}[],
  ): Promise<LiteValueType[]> {
    const pgParsers = await getTypeParsers(sql, {returnJsonAsString: true});
    const parsers = columns.map(c => {
      const pgParse = pgParsers.getTypeParser(c.typeOID);
      return (val: string) =>
        liteValue(
          pgParse(val) as PostgresValueType,
          c.dataType,
          JSON_STRINGIFIED,
        );
    });

    const tsvParser = new TsvParser();
    const results: LiteValueType[] = [];
    const colCount = columns.length;
    let col = 0;

    await pipeline(
      await sql
        .unsafe(
          `COPY ${tableName} (${columns.map(c => `"${c.name}"`).join(',')}) TO STDOUT`,
        )
        .readable(),
      new Writable({
        write(chunk: Buffer, _encoding, callback) {
          try {
            for (const text of tsvParser.parse(chunk)) {
              results.push(text === null ? null : parsers[col](text));
              col = (col + 1) % colCount;
            }
            callback();
          } catch (e) {
            callback(e instanceof Error ? e : new Error(String(e)));
          }
        },
      }),
    );
    return results;
  }

  /**
   * Copies table data via binary format and returns LiteValueType[] per field.
   */
  async function copyBinary(
    tableName: string,
    columns: {
      name: string;
      typeOID: number;
      dataType: string;
      pgTypeClass?: string | undefined;
      elemPgTypeClass?: string | null | undefined;
    }[],
  ): Promise<LiteValueType[]> {
    type Spec = Parameters<typeof makeBinaryDecoder>[0];
    const decoders = columns.map(c =>
      hasBinaryDecoder(c as Spec)
        ? makeBinaryDecoder(c as Spec)
        : textCastDecoder,
    );
    // Cast unknown-type columns to ::text in the SELECT.
    const selectCols = columns
      .map(c =>
        hasBinaryDecoder(c as Spec) ? `"${c.name}"` : `"${c.name}"::text`,
      )
      .join(',');
    const binaryParser = new BinaryCopyParser();
    const results: LiteValueType[] = [];
    const colCount = columns.length;
    let col = 0;

    await pipeline(
      await sql
        .unsafe(
          `COPY (SELECT ${selectCols} FROM ${tableName}) TO STDOUT WITH (FORMAT binary)`,
        )
        .readable(),
      new Writable({
        write(chunk: Buffer, _encoding, callback) {
          try {
            for (const fieldBuf of binaryParser.parse(chunk)) {
              results.push(fieldBuf === null ? null : decoders[col](fieldBuf));
              col = (col + 1) % colCount;
            }
            callback();
          } catch (e) {
            callback(e instanceof Error ? e : new Error(String(e)));
          }
        },
      }),
    );
    return results;
  }

  /** Gets the type OID for a column from pg_attribute. */
  async function getColumnOIDs(
    tableName: string,
  ): Promise<
    {name: string; typeOID: number; dataType: string; pgTypeClass: string}[]
  > {
    const rows = await sql`
      SELECT a.attname AS name,
             a.atttypid AS "typeOID",
             t.typname AS "dataType",
             t.typtype AS "pgTypeClass"
      FROM pg_attribute a
      JOIN pg_type t ON a.atttypid = t.oid
      WHERE a.attrelid = ${tableName}::regclass
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum
    `;
    return rows as unknown as {
      name: string;
      typeOID: number;
      dataType: string;
      pgTypeClass: string;
    }[];
  }

  test('integer types', async () => {
    await sql`
      CREATE TABLE int_types (
        a int2,
        b int4,
        c int8
      )`;
    await sql`INSERT INTO int_types VALUES
      (1, 100, 1000000000000),
      (-32768, -2147483648, -9223372036854775808),
      (32767, 2147483647, 9223372036854775807),
      (NULL, NULL, NULL)`;

    const cols = await getColumnOIDs('int_types');
    const textResult = await copyText('int_types', cols);
    const binaryResult = await copyBinary('int_types', cols);

    // INT8 is stored as bigint in binary but number-as-string text path
    // converts to bigint too via postgres.BigInt. They should match.
    expect(binaryResult).toEqual(textResult);
  });

  test('float types', async () => {
    await sql`
      CREATE TABLE float_types (
        a float4,
        b float8
      )`;
    // Use values that are exactly representable as float32 to avoid
    // precision differences between text (which rounds) and binary
    // (which gives exact IEEE 754 bits).
    await sql`INSERT INTO float_types VALUES
      (-1.5, 3.141592653589793),
      (0, -1.5),
      ('NaN', 'NaN'),
      ('Infinity', 'Infinity'),
      ('-Infinity', '-Infinity'),
      (NULL, NULL)`;

    const cols = await getColumnOIDs('float_types');
    const textResult = await copyText('float_types', cols);
    const binaryResult = await copyBinary('float_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('bool type', async () => {
    await sql`
      CREATE TABLE bool_types (a bool)`;
    await sql`INSERT INTO bool_types VALUES (true), (false), (NULL)`;

    const cols = await getColumnOIDs('bool_types');
    const textResult = await copyText('bool_types', cols);
    const binaryResult = await copyBinary('bool_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('text types', async () => {
    await sql`
      CREATE TABLE text_types (
        a text,
        b varchar(100),
        c bpchar(10)
      )`;
    await sql`INSERT INTO text_types VALUES
      ('hello world', 'foo', 'bar'),
      ('', '', ''),
      ('tab\there', 'new\nline', 'back\\slash'),
      (NULL, NULL, NULL)`;

    const cols = await getColumnOIDs('text_types');
    const textResult = await copyText('text_types', cols);
    const binaryResult = await copyBinary('text_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('uuid type', async () => {
    await sql`
      CREATE TABLE uuid_types (a uuid)`;
    await sql`INSERT INTO uuid_types VALUES
      ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
      ('00000000-0000-0000-0000-000000000000'),
      (NULL)`;

    const cols = await getColumnOIDs('uuid_types');
    const textResult = await copyText('uuid_types', cols);
    const binaryResult = await copyBinary('uuid_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('json and jsonb types', async () => {
    await sql`
      CREATE TABLE json_types (
        a json,
        b jsonb
      )`;
    await sql`INSERT INTO json_types VALUES
      ('{"key": "value"}', '{"key": "value"}'),
      ('123', '123'),
      ('"hello"', '"hello"'),
      ('[1,2,3]', '[1,2,3]'),
      ('null', 'null'),
      (NULL, NULL)`;

    const cols = await getColumnOIDs('json_types');
    const textResult = await copyText('json_types', cols);
    const binaryResult = await copyBinary('json_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('timestamp types', async () => {
    await sql`
      CREATE TABLE ts_types (
        a timestamp,
        b timestamptz
      )`;
    await sql`INSERT INTO ts_types VALUES
      ('2024-01-15 12:30:00', '2024-01-15 12:30:00+00'),
      ('2000-01-01 00:00:00', '2000-01-01 00:00:00+00'),
      ('1999-06-15 23:59:59.123456', '1999-06-15 23:59:59.123456+00'),
      ('infinity', 'infinity'),
      ('-infinity', '-infinity'),
      (NULL, NULL)`;

    const cols = await getColumnOIDs('ts_types');
    const textResult = await copyText('ts_types', cols);
    const binaryResult = await copyBinary('ts_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('date type', async () => {
    await sql`
      CREATE TABLE date_types (a date)`;
    await sql`INSERT INTO date_types VALUES
      ('2024-01-15'),
      ('2000-01-01'),
      ('1970-01-01'),
      ('1999-12-31'),
      ('infinity'),
      ('-infinity'),
      (NULL)`;

    const cols = await getColumnOIDs('date_types');
    const textResult = await copyText('date_types', cols);
    const binaryResult = await copyBinary('date_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('time types', async () => {
    await sql`
      CREATE TABLE time_types (
        a time,
        b timetz
      )`;
    await sql`INSERT INTO time_types VALUES
      ('12:30:00', '12:30:00+00'),
      ('00:00:00', '00:00:00+00'),
      ('23:59:59.999', '23:59:59.999+00'),
      ('12:30:00', '12:30:00+05'),
      ('12:30:00', '12:30:00-05'),
      ('01:00:00', '01:00:00+05'),
      (NULL, NULL)`;

    const cols = await getColumnOIDs('time_types');
    const textResult = await copyText('time_types', cols);
    const binaryResult = await copyBinary('time_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('numeric type', async () => {
    await sql`
      CREATE TABLE numeric_types (a numeric)`;
    await sql`INSERT INTO numeric_types VALUES
      (12345.67),
      (-99.99),
      (0),
      (1),
      (1000000),
      (0.001),
      ('NaN'),
      ('Infinity'),
      ('-Infinity'),
      (NULL)`;

    const cols = await getColumnOIDs('numeric_types');
    const textResult = await copyText('numeric_types', cols);
    const binaryResult = await copyBinary('numeric_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('bytea type', async () => {
    await sql`
      CREATE TABLE bytea_types (a bytea)`;
    // Use parameterized insertion for binary data.
    await sql`INSERT INTO bytea_types VALUES
      (${Buffer.from([0xde, 0xad, 0xbe, 0xef])}::bytea),
      (${Buffer.from([0x00])}::bytea),
      (${Buffer.alloc(0)}::bytea),
      (NULL)`;

    const cols = await getColumnOIDs('bytea_types');
    const textResult = await copyText('bytea_types', cols);
    const binaryResult = await copyBinary('bytea_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('enum type', async () => {
    await sql`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`;
    await sql`CREATE TABLE enum_types (a mood)`;
    await sql`INSERT INTO enum_types VALUES ('sad'), ('happy'), (NULL)`;

    const cols = await getColumnOIDs('enum_types');
    const textResult = await copyText('enum_types', cols);
    const binaryResult = await copyBinary('enum_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('integer array type', async () => {
    await sql`
      CREATE TABLE int_arr_types (a int4[])`;
    await sql`INSERT INTO int_arr_types VALUES
      ('{1,2,3}'),
      ('{}'),
      ('{-1,0,1}'),
      (NULL)`;

    const cols = (await getColumnOIDs('int_arr_types')).map(c => ({
      ...c,
      elemPgTypeClass: 'b' as const,
    }));
    const textResult = await copyText('int_arr_types', cols);
    const binaryResult = await copyBinary('int_arr_types', cols);

    // Text path produces arrays like "[1,2,3]" via stringify.
    // Binary path also produces JSON strings. They should match.
    expect(binaryResult).toEqual(textResult);
  });

  test('text array type', async () => {
    await sql`
      CREATE TABLE text_arr_types (a text[])`;
    await sql`INSERT INTO text_arr_types VALUES
      ('{"hello","world"}'),
      ('{}'),
      (NULL)`;

    const cols = (await getColumnOIDs('text_arr_types')).map(c => ({
      ...c,
      elemPgTypeClass: 'b' as const,
    }));
    const textResult = await copyText('text_arr_types', cols);
    const binaryResult = await copyBinary('text_arr_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('array with nulls', async () => {
    await sql`
      CREATE TABLE arr_null_types (a int4[])`;
    await sql`INSERT INTO arr_null_types VALUES ('{1,NULL,3}')`;

    const cols = (await getColumnOIDs('arr_null_types')).map(c => ({
      ...c,
      elemPgTypeClass: 'b' as const,
    }));
    const textResult = await copyText('arr_null_types', cols);
    const binaryResult = await copyBinary('arr_null_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('mixed types in one table', async () => {
    await sql`
      CREATE TABLE mixed_types (
        id int4,
        name text,
        active bool,
        score float8,
        data jsonb,
        created timestamptz
      )`;
    await sql`INSERT INTO mixed_types VALUES
      (1, 'Alice', true, 99.5, '{"role":"admin"}', '2024-01-15 12:00:00+00'),
      (2, 'Bob', false, 0, '[]', '2000-01-01 00:00:00+00'),
      (NULL, NULL, NULL, NULL, NULL, NULL)`;

    const cols = await getColumnOIDs('mixed_types');
    const textResult = await copyText('mixed_types', cols);
    const binaryResult = await copyBinary('mixed_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('many rows', {timeout: 30_000}, async () => {
    await sql`
      CREATE TABLE many_rows (
        id int4,
        val text
      )`;
    // Insert 500 rows
    const rows = Array.from({length: 500}, (_, i) => ({
      id: i,
      val: `row-${i}-${'x'.repeat(i % 100)}`,
    }));
    for (const row of rows) {
      await sql`INSERT INTO many_rows ${sql(row)}`;
    }

    const cols = await getColumnOIDs('many_rows');
    const textResult = await copyText('many_rows', cols);
    const binaryResult = await copyBinary('many_rows', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('all null row', async () => {
    await sql`
      CREATE TABLE all_null (
        a int4,
        b text,
        c bool,
        d float8
      )`;
    await sql`INSERT INTO all_null VALUES (NULL, NULL, NULL, NULL)`;

    const cols = await getColumnOIDs('all_null');
    const textResult = await copyText('all_null', cols);
    const binaryResult = await copyBinary('all_null', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('empty table', async () => {
    await sql`CREATE TABLE empty_tbl (a int4)`;

    const cols = await getColumnOIDs('empty_tbl');
    const textResult = await copyText('empty_tbl', cols);
    const binaryResult = await copyBinary('empty_tbl', cols);
    expect(binaryResult).toEqual(textResult);
    expect(binaryResult).toEqual([]);
  });

  test('bigint array type', async () => {
    await sql`
      CREATE TABLE bigint_arr_types (a int8[])`;
    await sql`INSERT INTO bigint_arr_types VALUES
      ('{1,2,9007199254740993}'),
      (NULL)`;

    const cols = (await getColumnOIDs('bigint_arr_types')).map(c => ({
      ...c,
      elemPgTypeClass: 'b' as const,
    }));
    const textResult = await copyText('bigint_arr_types', cols);
    const binaryResult = await copyBinary('bigint_arr_types', cols);

    // For bigint arrays, text path uses postgres.js parser which returns
    // bigints, while binary path converts to Number when safe.
    // Both are ultimately JSON.stringify'd — verify the string output matches.
    expect(binaryResult).toEqual(textResult);
  });

  test('large json values', async () => {
    await sql`
      CREATE TABLE large_json (a jsonb)`;
    const largeObj: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      largeObj[`key_${i}`] = 'x'.repeat(100);
    }
    await sql`INSERT INTO large_json VALUES (${stringify(largeObj)}::jsonb)`;

    const cols = await getColumnOIDs('large_json');
    const textResult = await copyText('large_json', cols);
    const binaryResult = await copyBinary('large_json', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('composite type (unknown OID, uses ::text cast)', async () => {
    await sql.unsafe(`CREATE TYPE custom_point AS (x float8, y float8)`);
    await sql`
      CREATE TABLE composite_types (
        id int4,
        pt custom_point
      )`;
    await sql.unsafe(`INSERT INTO composite_types VALUES
      (1, ROW(1.5, 2.5)),
      (2, ROW(-3.0, 4.0)),
      (3, NULL)`);

    const cols = await getColumnOIDs('composite_types');
    // Composite type has pgTypeClass='c' and an unknown OID.
    const ptCol = cols.find(c => c.name === 'pt');
    expect(ptCol?.pgTypeClass).toBe('c');

    const textResult = await copyText('composite_types', cols);
    const binaryResult = await copyBinary('composite_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  test('built-in types without binary decoders (::text fallback)', async () => {
    await sql`
      CREATE TABLE unknown_builtin_types (
        a macaddr,
        b interval,
        c point,
        d circle,
        e bit(8),
        f varbit(16)
      )`;
    await sql`INSERT INTO unknown_builtin_types VALUES
      ('08:00:2b:01:02:03',
       '1 year 2 months 3 days 04:05:06', '(1.5,2.5)', '<(0,0),5>',
       B'10101010', B'110011'),
      ('00:00:00:00:00:00',
       '00:00:00', '(0,0)', '<(1,2),3.5>',
       B'00000000', B'1'),
      (NULL, NULL, NULL, NULL, NULL, NULL)`;

    const cols = await getColumnOIDs('unknown_builtin_types');
    const textResult = await copyText('unknown_builtin_types', cols);
    const binaryResult = await copyBinary('unknown_builtin_types', cols);
    expect(binaryResult).toEqual(textResult);
  });

  // inet and cidr have a known divergence: PG's ::text cast (used by binary
  // COPY path) uses `network_show` which always includes the netmask
  // (e.g. "192.168.1.1/32"), while PG's text output function `inet_out`
  // (used by text COPY) omits it for host addresses (e.g. "192.168.1.1").
  // See postgres src/backend/utils/adt/network.c.
  test('inet/cidr ::text cast includes netmask (known divergence)', async () => {
    await sql`
      CREATE TABLE inet_types (
        a inet,
        b cidr
      )`;
    await sql`INSERT INTO inet_types VALUES
      ('192.168.1.1', '10.0.0.0/8'),
      ('::1', '2001:db8::/32'),
      (NULL, NULL)`;

    const cols = await getColumnOIDs('inet_types');
    const textResult = await copyText('inet_types', cols);
    const binaryResult = await copyBinary('inet_types', cols);

    // text path: inet_out omits /32 and /128 for host addresses
    expect(textResult).toEqual([
      '192.168.1.1',
      '10.0.0.0/8',
      '::1',
      '2001:db8::/32',
      null,
      null,
    ]);
    // binary path: network_show always includes the netmask
    expect(binaryResult).toEqual([
      '192.168.1.1/32',
      '10.0.0.0/8',
      '::1/128',
      '2001:db8::/32',
      null,
      null,
    ]);
  });

  test('mixed known and unknown types', async () => {
    await sql.unsafe(`CREATE TYPE status_composite AS (code int4, label text)`);
    await sql`
      CREATE TABLE mixed_known_unknown (
        id int4,
        name text,
        info status_composite,
        score float8
      )`;
    await sql.unsafe(`INSERT INTO mixed_known_unknown VALUES
      (1, 'alice', ROW(200, 'ok'), 99.5),
      (2, 'bob', NULL, 0)`);

    const cols = await getColumnOIDs('mixed_known_unknown');
    const textResult = await copyText('mixed_known_unknown', cols);
    const binaryResult = await copyBinary('mixed_known_unknown', cols);
    expect(binaryResult).toEqual(textResult);
  });
});
