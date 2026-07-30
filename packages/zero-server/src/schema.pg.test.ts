import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {serverSchemaQuery} from './schema.ts';

type Row = Record<string, unknown>;

// A catalog that exercises every branch of the expressions in
// serverSchemaQuery: domains (including over enums, arrays and parameterized
// types), enum arrays, composite types, extension types, the bit / varbit and
// float / integer precision special cases, views, dropped and generated
// columns, and a table outside the public schema.
const catalogSql = /*sql*/ `
CREATE EXTENSION IF NOT EXISTS citext;

CREATE SCHEMA alternate_schema;

CREATE TYPE mood AS ENUM ('happy', 'sad');
CREATE TYPE point3 AS (x FLOAT8, y FLOAT8, z FLOAT8);

CREATE DOMAIN short_text AS VARCHAR(10);
CREATE DOMAIN exact_amount AS NUMERIC(8, 3);
CREATE DOMAIN a_mood AS mood;
CREATE DOMAIN int_array AS INT4[];
CREATE DOMAIN counter AS BIGINT;

CREATE TABLE numbers (
  id SERIAL PRIMARY KEY,
  int_2 SMALLINT,
  int_4 INTEGER,
  int_8 BIGINT,
  float_4 REAL,
  float_8 DOUBLE PRECISION,
  exact NUMERIC(8, 3),
  inexact NUMERIC
);

CREATE TABLE strings (
  id TEXT PRIMARY KEY,
  fixed CHAR(10),
  varying VARCHAR(20),
  unbounded TEXT,
  unbounded_bpchar BPCHAR,
  insensitive CITEXT,
  bits BIT(8),
  varbits BIT VARYING(16),
  blob BYTEA
);

CREATE TABLE exotic (
  id UUID PRIMARY KEY,
  state mood,
  states mood[],
  coords point3,
  ip INET,
  mac MACADDR,
  doc JSON,
  docb JSONB,
  tags TEXT[],
  amounts NUMERIC(8, 3)[],
  moment TIMESTAMPTZ,
  day DATE,
  span INTERVAL
);

CREATE TABLE domains (
  id short_text PRIMARY KEY,
  amount exact_amount,
  state a_mood,
  states a_mood[],
  numbers int_array,
  hits counter
);

CREATE TABLE quirks (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  doubled INTEGER GENERATED ALWAYS AS (id * 2) STORED,
  kept TEXT,
  dropped TEXT
);
ALTER TABLE quirks DROP COLUMN dropped;

CREATE VIEW numbers_view AS SELECT id, exact FROM numbers;
CREATE MATERIALIZED VIEW numbers_matview AS SELECT id FROM numbers;

CREATE TABLE alternate_schema.numbers (LIKE numbers);
`;

const tables = [
  ['public', 'numbers'],
  ['public', 'strings'],
  ['public', 'exotic'],
  ['public', 'domains'],
  ['public', 'quirks'],
  ['public', 'numbers_view'],
  ['public', 'numbers_matview'],
  ['alternate_schema', 'numbers'],
] as const;

// The information_schema query that serverSchemaQuery replaced, kept so that
// the replacement can be diffed against it. The table pairs are inlined because
// the two queries do not bind their parameters in the same order.
function informationSchemaQuery(
  schemaTablePairs: readonly (readonly [string, string])[],
) {
  const inClause = schemaTablePairs
    .map(([schema, table]) => `('${schema}'::text, '${table}'::text)`)
    .join(',');
  return /*sql*/ `
      SELECT
          c.table_schema::text AS schema,
          c.table_name::text AS table,
          c.column_name::text AS column,
          c.data_type::text AS "dataType",
          c.character_maximum_length AS length,
          c.numeric_precision AS precision,
          c.numeric_scale AS scale,
          t.typtype::text AS typtype,
          t.typname::text AS typename,
          CASE WHEN t.typelem <> 0 THEN et.typtype::text ELSE NULL END AS "elemTyptype",
          CASE WHEN t.typelem <> 0 THEN et.typname::text ELSE NULL END AS "elemTypname"
      FROM
          information_schema.columns c
      JOIN
          pg_catalog.pg_type t ON c.udt_name = t.typname
      LEFT JOIN
          pg_catalog.pg_type et ON t.typelem = et.oid
      JOIN
          pg_catalog.pg_namespace n ON t.typnamespace = n.oid
      WHERE
          (c.table_schema, c.table_name) IN (${inClause})
    `;
}

function sorted(rows: readonly Row[]) {
  return rows.toSorted((a, b) =>
    `${a.schema}.${a.table}.${a.column}`.localeCompare(
      `${b.schema}.${b.table}.${b.column}`,
    ),
  );
}

function run(pg: PostgresDB, pairs: readonly (readonly [string, string])[]) {
  const {text, values} = serverSchemaQuery(pairs);
  return pg.unsafe(text, values as string[]);
}

describe('serverSchemaQuery', () => {
  let pg: PostgresDB;

  beforeEach(async () => {
    pg = await testDBs.create('server-schema-query-test');
    await pg.unsafe(catalogSql);
  });

  test('returns what information_schema returned', async () => {
    const actual = await run(pg, tables);
    const expected = await pg.unsafe(informationSchemaQuery(tables));

    expect(sorted(actual)).toEqual(sorted(expected));

    // Guard against both queries being wrong in the same way.
    const columns = actual.map(r => `${r.table}.${r.column}`);
    expect(columns).toContain('numbers_view.exact'); // views are included
    expect(columns).toContain('quirks.doubled'); // so are generated columns
    expect(columns).not.toContain('quirks.dropped'); // dropped columns are not
    expect(columns).not.toContain('numbers_matview.id'); // nor are matviews
  });

  test.each([
    // integers report a precision and a scale, floats only a precision
    ['numbers.int_2', {dataType: 'smallint', precision: 16, scale: 0}],
    ['numbers.int_8', {dataType: 'bigint', precision: 64, scale: 0}],
    ['numbers.float_4', {dataType: 'real', precision: 24, scale: null}],
    [
      'numbers.float_8',
      {dataType: 'double precision', precision: 53, scale: null},
    ],
    ['numbers.exact', {dataType: 'numeric', precision: 8, scale: 3}],
    ['numbers.inexact', {dataType: 'numeric', precision: null, scale: null}],
    // char and varchar subtract the header from their typmod, bit and varbit
    // do not
    ['strings.fixed', {dataType: 'character', length: 10, typename: 'bpchar'}],
    [
      'strings.varying',
      {dataType: 'character varying', length: 20, typename: 'varchar'},
    ],
    ['strings.unbounded_bpchar', {dataType: 'character', length: null}],
    ['strings.bits', {dataType: 'bit', length: 8}],
    ['strings.varbits', {dataType: 'bit varying', length: 16}],
    // types outside pg_catalog are USER-DEFINED, and typtype identifies them
    ['strings.insensitive', {dataType: 'USER-DEFINED', typename: 'citext'}],
    ['exotic.state', {dataType: 'USER-DEFINED', typtype: 'e'}],
    ['exotic.coords', {dataType: 'USER-DEFINED', typtype: 'c'}],
    [
      'exotic.states',
      {dataType: 'ARRAY', elemTyptype: 'e', elemTypname: 'mood'},
    ],
    // arrays report no precision or scale, even when their element type has one
    [
      'exotic.amounts',
      {dataType: 'ARRAY', elemTypname: 'numeric', precision: null, scale: null},
    ],
    // domains report their base type
    [
      'domains.id',
      {dataType: 'character varying', length: 10, typename: 'varchar'},
    ],
    [
      'domains.amount',
      {dataType: 'numeric', precision: 8, scale: 3, typename: 'numeric'},
    ],
    [
      'domains.state',
      {dataType: 'USER-DEFINED', typtype: 'e', typename: 'mood'},
    ],
    // an array of a domain is not itself a domain, so the element type is the
    // domain rather than what the domain wraps
    [
      'domains.states',
      {dataType: 'ARRAY', elemTyptype: 'd', elemTypname: 'a_mood'},
    ],
    ['domains.numbers', {dataType: 'ARRAY', elemTypname: 'int4'}],
    ['domains.hits', {dataType: 'bigint', precision: 64, scale: 0}],
  ] as const)('%s', async (column, expected) => {
    const rows = await run(pg, tables);
    const [table, name] = column.split('.');
    expect(
      rows.find(r => r.table === table && r.column === name),
    ).toMatchObject(expected);
  });

  test('does not duplicate columns whose type name is ambiguous', async () => {
    // udt_name only carries a type's name, so joining pg_type on it matched
    // every schema that declares a type with that name.
    await pg.unsafe(`CREATE TYPE alternate_schema.mood AS ENUM ('meh')`);

    const rows = await run(pg, [['public', 'exotic']]);
    expect(rows.filter(r => r.column === 'state')).toEqual([
      expect.objectContaining({typtype: 'e', typename: 'mood'}),
    ]);

    expect(
      (await pg.unsafe(informationSchemaQuery([['public', 'exotic']]))).filter(
        r => r.column === 'state',
      ),
    ).toHaveLength(2);
  });
});
