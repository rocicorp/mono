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

let pg: PostgresDB;
let sqlite: Database;
let queryDelegate: QueryDelegate;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyQuery = Query<any, any, any>;

type Schema = typeof schema;
const queries: {
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} = {
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
} as any;
const tables = Object.keys(queries) as (keyof typeof queries)[];

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

beforeAll(async () => {
  pg = await testDBs.create('compiler');
  sqlite = new Database(lc, ':memory:');
  await writeChinook(pg, sqlite);

  queryDelegate = newQueryDelegate(lc, logConfig, sqlite, schema);

  queries.album = newQuery(queryDelegate, schema, 'album');
  queries.artist = newQuery(queryDelegate, schema, 'artist');
  queries.customer = newQuery(queryDelegate, schema, 'customer');
  queries.employee = newQuery(queryDelegate, schema, 'employee');
  queries.genre = newQuery(queryDelegate, schema, 'genre');
  queries.invoice = newQuery(queryDelegate, schema, 'invoice');
  queries.invoice_line = newQuery(queryDelegate, schema, 'invoice_line');
  queries.media_type = newQuery(queryDelegate, schema, 'media_type');
  queries.playlist = newQuery(queryDelegate, schema, 'playlist');
  queries.playlist_track = newQuery(queryDelegate, schema, 'playlist_track');
  queries.track = newQuery(queryDelegate, schema, 'track');
});

describe('basic select', () => {
  test.each(tables.map(table => [table]))('select * from %s', async table => {
    await checkZqlAndSql(pg, queries[table]);
  });

  test.each(tables.map(table => [table]))(
    'select * from %s limit 100',
    async table => {
      await checkZqlAndSql(pg, queries[table].limit(100));
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
    const query = queries[table] as AnyQuery;
    const relationships = Object.keys(
      (schema.relationships as Record<string, Record<string, unknown>>)[
        table
      ] ?? {},
    );

    for (const r of relationships) {
      if (brokenRelationships.includes(r)) {
        continue;
      }
      await checkZqlAndSql(pg, query.related(r));
    }

    // Junction edges do not correctly handle limits
    // in ZQL 😬
    const brokenLimits = ['tracks'];
    for (const r of relationships) {
      if (brokenRelationships.includes(r) || brokenLimits.includes(r)) {
        continue;
      }
      await checkZqlAndSql(pg, query.related(r, q => q.limit(100)).limit(100));
    }
  });
});

async function checkZqlAndSql(
  pg: PostgresDB,
  query: Query<Schema, keyof Schema['tables']>,
) {
  const pgResult = await runZqlAsSql(pg, query);
  const zqlResult = query.run();
  // In failure output:
  // `-` is PG
  // `+` is ZQL
  expect(zqlResult).toEqual(pgResult);
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
