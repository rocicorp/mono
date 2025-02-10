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
  astForTestingSymbol,
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

type Schema = typeof schema;
let albumQuery: Query<Schema, 'album'>;
let artistQuery: Query<Schema, 'artist'>;
let customerQuery: Query<Schema, 'customer'>;
let employeeQuery: Query<Schema, 'employee'>;
let genreQuery: Query<Schema, 'genre'>;
let mediaTypeQuery: Query<Schema, 'media_type'>;
let playlistQuery: Query<Schema, 'playlist'>;
// TODO: buggy tables
// let _invoiceQuery: Query<Schema, 'invoice'>;
// let _invoiceLineQuery: Query<Schema, 'invoice_line'>;
// let playlistTrackQuery: Query<Schema, 'playlist_track'>;
// let trackQuery: Query<Schema, 'track'>;

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

  albumQuery = newQuery(queryDelegate, schema, 'album');
  artistQuery = newQuery(queryDelegate, schema, 'artist');
  customerQuery = newQuery(queryDelegate, schema, 'customer');
  employeeQuery = newQuery(queryDelegate, schema, 'employee');
  genreQuery = newQuery(queryDelegate, schema, 'genre');
  // invoiceQuery = newQuery(queryDelegate, schema, 'invoice');
  // invoiceLineQuery = newQuery(queryDelegate, schema, 'invoice_line');
  mediaTypeQuery = newQuery(queryDelegate, schema, 'media_type');
  playlistQuery = newQuery(queryDelegate, schema, 'playlist');
  // playlistTrackQuery = newQuery(queryDelegate, schema, 'playlist_track');
  // trackQuery = newQuery(queryDelegate, schema, 'track');
});

describe('basic select', () => {
  test.each([
    ['album', () => albumQuery],
    ['artist', () => artistQuery],
    ['customer', () => customerQuery],
    ['employee', () => employeeQuery],
    ['genre', () => genreQuery],
    // ['invoice', () => invoiceQuery], --> total is showing up as a string
    // ['invoice_line', () => invoiceLineQuery], --> numeric columns are showing up as strings
    ['media_type', () => mediaTypeQuery],
    ['playlist', () => playlistQuery],
    // ['playlist_track', () => playlistTrackQuery], --> this is not sorting correctly between zql and pg
    // ['track', () => trackQuery], --> numeric columns are showing up as strings
  ])('select * from %s', async (_table, q) => {
    const query = q();
    const pgResult = await runZqlAsSql(pg, query);
    const zqlResult = query.run();
    expect(zqlResult).toEqual(pgResult);
  });
});

function runZqlAsSql(
  pg: PostgresDB,
  query: Query<Schema, keyof Schema['tables']>,
) {
  const sqlQuery = formatPg(compile(ast(query), format(query)));
  return pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]);
}

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[astForTestingSymbol];
}

function format(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>).format;
}
