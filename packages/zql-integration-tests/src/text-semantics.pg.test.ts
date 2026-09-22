/**
 * Text semantics oracle corpus.
 *
 * These tests pin cases where ZQLite/SQLite and compiled ZQL SQL on
 * PostgreSQL must agree for text comparison, LIKE/ILIKE, and NULL logic.
 *
 * It also pins one *known divergence*: case-insensitive matching (ILIKE) folds
 * case differently on the two sides.
 *  - ZQLite compiles ILIKE to `lower(col) LIKE lower(pat)` using
 *    @rocicorp/zero-sqlite3's ICU-backed `lower()`, which folds the *full
 *    Unicode* range (Ω→ω) regardless of any column collation. The in-memory IVM
 *    matcher folds the same way via JS `String.toLowerCase()` (see
 *    zqlite/src/ilike-parity.test.ts and zql/src/builder/like.ts).
 *  - Compiled ZQL SQL emits a bare `ILIKE`, so Postgres folds case according to
 *    the *column collation*. Under `COLLATE "C"` that is ASCII-only, so non-ASCII
 *    letters are not folded.
 *
 * The result is that ILIKE over non-ASCII text agrees under a Unicode-folding
 * collation (the default `en_US.utf8`) but diverges under `COLLATE "C"`. The
 * `textItemC` table pins the divergence; the `textItemDefault` table pins that
 * it disappears under the default collation. See the "known divergence" and
 * "collation control" describe blocks below.
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

// Two tables with identical data but different `value` collations, so we can
// pin both the parity cases and the collation-dependent ILIKE divergence.
//  - textItemC:       value COLLATE "C"  (ASCII-only case folding in Postgres)
//  - textItemDefault: value default collation (Unicode case folding in Postgres)
const textItemC = table('textItemC')
  .columns({
    id: string(),
    value: string().optional(),
  })
  .primaryKey('id');

const textItemDefault = table('textItemDefault')
  .columns({
    id: string(),
    value: string().optional(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [textItemC, textItemDefault],
});
type Schema = typeof schema;

const serverSchema: ServerSchema = {
  textItemC: {
    id: {type: 'text', isEnum: false, isArray: false},
    value: {type: 'text', isEnum: false, isArray: false},
  },
  textItemDefault: {
    id: {type: 'text', isEnum: false, isArray: false},
    value: {type: 'text', isEnum: false, isArray: false},
  },
} as const;

const createTableSQL = /*sql*/ `
CREATE TABLE "textItemC" (
  "id" TEXT PRIMARY KEY,
  "value" TEXT COLLATE "C"
);
-- No COLLATE clause: inherits the database default collation, which is a
-- Unicode-folding locale (en_US.utf8 in the test container).
CREATE TABLE "textItemDefault" (
  "id" TEXT PRIMARY KEY,
  "value" TEXT
);
`;

const rows: Row[] = [
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
  {id: '11-combining', value: 'é'},
  {id: '12-composed', value: 'é'},
  {id: '13-z', value: 'z'},
  {id: '14-zz', value: 'zz'},
  // Non-ASCII cased rows for the ILIKE folding cases below.
  {id: '15-omega-lower', value: 'ωmega'},
  {id: '16-cyrillic-upper', value: 'ПРИВЕТ'},
  {id: '17-cyrillic-lower', value: 'привет'},
  {id: '18-accent-upper', value: 'CAFÉ'},
  {id: '19-accent-lower', value: 'café'},
];

const testData = {
  textItemC: rows,
  textItemDefault: rows,
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
  const baseQuery = () => newQuery(schema, 'textItemC');

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
  ] satisfies [string, Query<'textItemC', Schema>][])(
    '%s',
    async (_name: string, query: Query<'textItemC', Schema>) => {
      await expectZqliteToMatchPg(query, 'textItemC');
    },
  );
});

/**
 * KNOWN DIVERGENCE. ZQLite folds the full Unicode range for ILIKE while
 * Postgres under `COLLATE "C"` folds ASCII only, so ZQLite returns a superset
 * for non-ASCII patterns. These tests pin the *current* divergent behavior:
 * they assert the exact (different) id sets each side returns. If the engines
 * are ever brought into agreement (e.g. ZQLite respects collation, or compiled
 * ZQL lowercases per-collation), the pinned `pgIds` here will change and this
 * block will fail — at which point move these cases up into the parity corpus.
 */
describe('known divergence: ILIKE folds Unicode in ZQLite but not under PG COLLATE "C"', () => {
  const baseQuery = () => newQuery(schema, 'textItemC');

  test.each([
    [
      'Greek omega: ZQLite folds Ω→ω, PG C does not',
      baseQuery().where('value', 'ILIKE', 'ωmega').orderBy('id', 'asc'),
      ['09-omega', '15-omega-lower'],
      ['15-omega-lower'],
    ],
    [
      'Cyrillic: ZQLite folds ПРИВЕТ→привет, PG C does not',
      baseQuery().where('value', 'ILIKE', 'привет').orderBy('id', 'asc'),
      ['16-cyrillic-upper', '17-cyrillic-lower'],
      ['17-cyrillic-lower'],
    ],
    [
      'Latin-1 accent: ZQLite folds É→é, PG C does not',
      baseQuery().where('value', 'ILIKE', 'café').orderBy('id', 'asc'),
      ['18-accent-upper', '19-accent-lower'],
      ['19-accent-lower'],
    ],
  ] satisfies [string, Query<'textItemC', Schema>, string[], string[]][])(
    '%s',
    async (
      _name: string,
      query: Query<'textItemC', Schema>,
      expectedZqliteIds: string[],
      expectedPgIds: string[],
    ) => {
      const {zqliteIds, pgIds} = await runBothIds(query, 'textItemC');
      expect(zqliteIds).toEqual(expectedZqliteIds);
      expect(pgIds).toEqual(expectedPgIds);
      // The point of this block: the two engines disagree here.
      expect(zqliteIds).not.toEqual(pgIds);
    },
  );
});

/**
 * COLLATION CONTROL. The same ILIKE patterns over the same data agree once the
 * column uses a Unicode-folding collation (the database default), because
 * Postgres then folds case the same way ZQLite always does. This isolates the
 * divergence above to the `COLLATE "C"` choice rather than to the ILIKE
 * operator itself.
 */
describe('collation control: ILIKE agrees under default (Unicode-folding) collation', () => {
  const baseQuery = () => newQuery(schema, 'textItemDefault');

  test.each([
    [
      'Greek omega folds on both sides',
      baseQuery().where('value', 'ILIKE', 'ωmega').orderBy('id', 'asc'),
    ],
    [
      'Cyrillic folds on both sides',
      baseQuery().where('value', 'ILIKE', 'привет').orderBy('id', 'asc'),
    ],
    [
      'Latin-1 accent folds on both sides',
      baseQuery().where('value', 'ILIKE', 'café').orderBy('id', 'asc'),
    ],
  ] satisfies [string, Query<'textItemDefault', Schema>][])(
    '%s',
    async (_name: string, query: Query<'textItemDefault', Schema>) => {
      await expectZqliteToMatchPg(query, 'textItemDefault');
    },
  );
});

async function expectZqliteToMatchPg(
  query: Query<'textItemC' | 'textItemDefault', Schema>,
  tableName: 'textItemC' | 'textItemDefault',
) {
  const pgResult = await runAsSQL(query);
  const zqliteResult = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    tableName,
  );

  expect(zqliteResult).toEqualPg(pgResult);
}

async function runBothIds(
  query: Query<'textItemC' | 'textItemDefault', Schema>,
  tableName: 'textItemC' | 'textItemDefault',
) {
  const pgResult = (await runAsSQL(query)) as Row[];
  const zqliteResult = mapResultToClientNames(
    await queryDelegate.run(query),
    schema,
    tableName,
  ) as Row[];
  return {
    pgIds: pgResult.map(r => r.id as string),
    zqliteIds: zqliteResult.map(r => r.id as string),
  };
}

async function runAsSQL(q: Query<'textItemC' | 'textItemDefault', Schema>) {
  const c = compile(serverSchema, schema, asQueryInternals(q).ast);
  const sqlQuery = formatPgInternalConvert(c);
  return extractZqlResult(
    await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
  );
}
