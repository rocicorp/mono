/**
 * Text semantics oracle corpus.
 *
 * These tests pin cases where ZQLite/SQLite and compiled ZQL SQL on
 * PostgreSQL must agree for text comparison, LIKE/ILIKE, and NULL logic.
 */
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {Query} from '../../zql/src/query/query.ts';
import type {Database} from '../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import './helpers/comparePg.ts';
import {fillPgAndSync} from './helpers/setup.ts';

const lc = createSilentLogContext();
const DB_NAME = 'text-semantics-test';

const textItem = table('textItem')
  .columns({
    id: string(),
    value: string().optional(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [textItem],
});
type Schema = typeof schema;

const serverSchema: ServerSchema = {
  textItem: {
    id: {type: 'text', isEnum: false, isArray: false},
    value: {type: 'text', isEnum: false, isArray: false},
  },
} as const;

const createTableSQL = /*sql*/ `
CREATE TABLE "textItem" (
  "id" TEXT PRIMARY KEY,
  "value" TEXT COLLATE "C"
);
`;

const testData = {
  textItem: [
    {id: '01-null', value: null},
    {id: '02-empty', value: ''},
    {id: '03-lower', value: 'abc'},
    {id: '04-upper', value: 'ABC'},
    {id: '05-percent', value: 'a%z'},
    {id: '06-underscore', value: 'a_z'},
    {id: '07-backslash', value: 'a\\z'},
    {id: '08-newline', value: 'a\nb'},
    {id: '09-omega', value: 'Ωmega'},
    {id: '10-emoji', value: 'emoji😀x'},
    {id: '11-combining', value: 'e\u0301'},
    {id: '12-composed', value: 'é'},
    {id: '13-z', value: 'z'},
    {id: '14-zz', value: 'zz'},
  ],
} satisfies Record<string, Row[]>;

let pg: PostgresDB;
let sqlite: Database;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  const setup = await fillPgAndSync(schema, createTableSQL, testData, DB_NAME);
  pg = setup.pg;
  sqlite = setup.sqlite;
  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
});

afterAll(() => {
  sqlite.close();
});

describe('text semantics oracle corpus', () => {
  const baseQuery = () => newQuery(schema, 'textItem');

  test.each([
    [
      'all rows ordered by C-collated text',
      baseQuery().orderBy('value', 'asc'),
    ],
    [
      'range greater than ASCII value',
      baseQuery().where('value', '>', 'a').orderBy('value', 'asc'),
    ],
    [
      'range greater than Unicode value',
      baseQuery().where('value', '>', 'Ω').orderBy('value', 'asc'),
    ],
    [
      'range less than composed accented value',
      baseQuery().where('value', '<', 'é').orderBy('value', 'asc'),
    ],
    [
      'LIKE matches literal percent via escaped pattern',
      baseQuery().where('value', 'LIKE', 'a\\%%').orderBy('id', 'asc'),
    ],
    [
      'LIKE matches literal underscore via escaped pattern',
      baseQuery().where('value', 'LIKE', 'a\\_%').orderBy('id', 'asc'),
    ],
    [
      'LIKE wildcard spans newline',
      baseQuery().where('value', 'LIKE', 'a%b').orderBy('id', 'asc'),
    ],
    [
      'LIKE matches Unicode suffix',
      baseQuery().where('value', 'LIKE', '%😀x').orderBy('id', 'asc'),
    ],
    [
      'ILIKE matches ASCII case-insensitively',
      baseQuery().where('value', 'ILIKE', 'ab%').orderBy('id', 'asc'),
    ],
    [
      'NOT LIKE excludes matching prefix and nulls',
      baseQuery().where('value', 'NOT LIKE', 'a%').orderBy('id', 'asc'),
    ],
    [
      'IS NULL returns only null text values',
      baseQuery().where('value', 'IS', null).orderBy('id', 'asc'),
    ],
    [
      'IS NOT NULL excludes null text values',
      baseQuery().where('value', 'IS NOT', null).orderBy('id', 'asc'),
    ],
    [
      'OR with one null branch preserves SQL three-valued logic',
      baseQuery()
        .where(({cmp, or}) =>
          or(cmp('value', 'IS', undefined), cmp('value', '=', 'abc')),
        )
        .orderBy('id', 'asc'),
    ],
  ] satisfies [string, Query<'textItem', Schema>][])(
    '%s',
    async (_name: string, query: Query<'textItem', Schema>) => {
      await expectZqliteToMatchPg(query);
    },
  );
});

async function expectZqliteToMatchPg(query: Query<'textItem', Schema>) {
  const pgResult = await runAsSQL(query);
  const zqliteResult = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    'textItem',
  );

  expect(zqliteResult).toEqualPg(pgResult);
}

async function runAsSQL(q: Query<'textItem', Schema>) {
  const c = compile(serverSchema, schema, asQueryInternals(q).ast);
  const sqlQuery = formatPgInternalConvert(c);
  return extractZqlResult(
    await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
  );
}
