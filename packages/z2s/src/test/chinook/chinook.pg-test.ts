/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/naming-convention */
/**
 * Test suite that
 * 1. Downloads the chinook dataset
 * 2. Allows comparing manually crafted ZQL queries with Postgres output
 *
 * The ZQL will be run from scratch and via
 * diffs. The diffs applied are randomly generated.
 * The seed used for the random generation will be output
 * so that the test can be reproduced.
 */

import {beforeAll, describe, expect, test} from 'vitest';
import {testDBs} from '../../../../zero-cache/src/test/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {PostgresDB} from '../../../../zero-cache/src/types/pg.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {writeChinook} from './get-deps.ts';
import {
  completedAstSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../../zql/src/query/query-impl.ts';
import {newQueryDelegate} from '../../../../zqlite/src/test/source-factory.ts';
import type {LogConfig} from '../../../../otel/src/log-options.ts';
import {schema} from './schema.ts';
import type {Query} from '../../../../zql/src/query/query.ts';
import {formatPg} from '../../sql.ts';
import {compile} from '../../compiler.ts';
import type {JSONValue} from '../../../../shared/src/json.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../../zql/src/query/test/query-delegate.ts';

let pg: PostgresDB;
let sqlite: Database;
let zqliteQueryDelegate: QueryDelegate;
let memoryQueryDelegate: QueryDelegate;
type AnyQuery = Query<any, any, any>;

type Schema = typeof schema;
type Queries = {
  album: Query<Schema, 'album'>;
  artist: Query<Schema, 'artist'>;
  customer: Query<Schema, 'customer'>;
  employee: Query<Schema, 'employee'>;
  genre: Query<Schema, 'genre'>;
  media_type: Query<Schema, 'media_type'>;
  playlist: Query<Schema, 'playlist'>;
  playlist_track: Query<Schema, 'playlist_track'>;
  invoice: Query<Schema, 'invoice'>;
  invoice_line: Query<Schema, 'invoice_line'>;
  track: Query<Schema, 'track'>;
};
const zqliteQueries: Queries = {
  album: null,
  artist: null,
  customer: null,
  employee: null,
  genre: null,
  media_type: null,
  playlist: null,
  playlist_track: null,
  invoice: null,
  invoice_line: null,
  track: null,
} as any;
const memoryQueries: Queries = {...zqliteQueries} as any;
const tables = Object.keys(zqliteQueries) as (keyof typeof zqliteQueries)[];

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

function makeMemorySources() {
  return Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  );
}

beforeAll(async () => {
  pg = await testDBs.create('chinook');
  sqlite = new Database(lc, ':memory:');
  const memorySources = makeMemorySources();
  await writeChinook(pg, sqlite);

  zqliteQueryDelegate = newQueryDelegate(lc, logConfig, sqlite, schema);
  memoryQueryDelegate = new TestMemoryQueryDelegate(memorySources);

  tables.forEach(table => {
    zqliteQueries[table] = newQuery(zqliteQueryDelegate, schema, table) as any;
    memoryQueries[table] = newQuery(memoryQueryDelegate, schema, table) as any;
  });

  tables.forEach(table => {
    const rows = zqliteQueries[table].run();
    for (const row of rows) {
      memorySources[table].push({
        type: 'add',
        row,
      });
    }
  });
});

describe('basic select', () => {
  test.each(tables.map(table => [table]))('select * from %s', async table => {
    await checkZqlAndSql(pg, zqliteQueries[table], memoryQueries[table]);
  });

  test.each(tables.map(table => [table]))(
    'select * from %s limit 100',
    async table => {
      await checkZqlAndSql(
        pg,
        zqliteQueries[table].limit(100),
        memoryQueries[table].limit(100),
      );
    },
  );
});

describe('1 level related', () => {
  test.each(tables.map(table => [table]))('%s w/ related', async table => {
    const brokenRelationships = [
      // Bad type conversion. We need to convert types when doing JSON aggregation
      // as `postgresTypeConfig` does.
      'supportRep',
      'reportsTo',
    ];
    const zqliteQuery = zqliteQueries[table] as AnyQuery;
    const memoryQuery = memoryQueries[table] as AnyQuery;
    const relationships = Object.keys(
      (schema.relationships as Record<string, Record<string, unknown>>)[
        table
      ] ?? {},
    );

    for (const r of relationships) {
      if (brokenRelationships.includes(r)) {
        continue;
      }
      await checkZqlAndSql(pg, zqliteQuery.related(r), memoryQuery.related(r));
    }

    // Junction edges do not correctly handle limits
    // in ZQL 😬
    const brokenLimits = ['tracks'];
    for (const r of relationships) {
      if (brokenRelationships.includes(r) || brokenLimits.includes(r)) {
        continue;
      }
      await checkZqlAndSql(
        pg,
        zqliteQuery.related(r, q => q.limit(100)).limit(100),
        memoryQuery.related(r, q => q.limit(100)).limit(100),
      );
    }
  });
});

async function checkZqlAndSql(
  pg: PostgresDB,
  zqliteQuery: Query<Schema, keyof Schema['tables']>,
  memoryQuery: Query<Schema, keyof Schema['tables']>,
) {
  const pgResult = await runZqlAsSql(pg, zqliteQuery);
  const zqliteResult = zqliteQuery.run();
  const zqlMemResult = memoryQuery.run();
  // In failure output:
  // `-` is PG
  // `+` is ZQLite
  expect(zqliteResult).toEqual(pgResult);
  expect(zqliteResult).toEqual(zqlMemResult);
}

function runZqlAsSql(
  pg: PostgresDB,
  query: Query<Schema, keyof Schema['tables']>,
) {
  const sqlQuery = formatPg(compile(ast(query), format(query)));
  return pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]);
}

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

function format(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>).format;
}
