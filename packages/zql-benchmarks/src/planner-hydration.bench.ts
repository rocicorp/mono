import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {defaultFormat} from '../../zql/src/ivm/default-format.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {QueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {PullRow, Query} from '../../zql/src/query/query.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import {must} from '../../shared/src/must.ts';

const pgContent = await getChinook();

const {dbs, delegates, queries} = await bootstrap({
  suiteName: 'planner_hydration_bench',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for cost model
dbs.sqlite.exec('ANALYZE;');

const tables: {[key: string]: TableSchema} = schema.tables;
// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), dbs.sqlite, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Helper to create a query from an AST
function createQuery<TTable extends keyof typeof schema.tables>(
  tableName: TTable,
  queryAST: AST,
) {
  const q = new QueryImpl(schema, tableName, queryAST, defaultFormat, 'test');
  return q as Query<TTable, typeof schema, PullRow<TTable, typeof schema>>;
}

// Helper to benchmark planned vs unplanned
function benchmarkQuery<TTable extends keyof typeof schema.tables>(
  name: string,
  query: Query<TTable, typeof schema>,
) {
  const unplannedAST = asQueryInternals(query).ast;
  const completeOrderAst = completeOrdering(
    unplannedAST,
    tableName =>
      must(tables[tableName], `Table ${tableName} not found`).primaryKey,
  );
  // Map to server names, plan, then map back to client names
  const mappedAST = mapAST(completeOrderAst, clientToServerMapper);

  const plannedServerAST = planQuery(mappedAST, costModel);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  const delegate = delegates.sqlite;
  const tableName = unplannedAST.table as TTable;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  const plannedQuery = createQuery(tableName, plannedClientAST);

  summary(() => {
    bench(`unplanned: ${name}`, async () => {
      await delegate.run(unplannedQuery);
    });

    bench(`planned: ${name}`, async () => {
      await delegate.run(plannedQuery);
    });
  });
}

// Benchmark queries
benchmarkQuery(
  'track.exists(album) where title="Big Ones"',
  queries.track.whereExists('album', q => q.where('title', 'Big Ones')),
);

benchmarkQuery(
  'track.exists(album).exists(genre)',
  queries.track.whereExists('album').whereExists('genre'),
);

benchmarkQuery(
  'track.exists(album).exists(genre) with filters',
  queries.track
    .whereExists('album', q => q.where('title', 'Big Ones'))
    .whereExists('genre', q => q.where('name', 'Rock')),
);

benchmarkQuery(
  'playlist.exists(tracks)',
  queries.playlist.whereExists('tracks'),
);

benchmarkQuery(
  'track.exists(playlists)',
  queries.track.whereExists('playlists'),
);

benchmarkQuery(
  'track.exists(album) OR exists(genre)',
  queries.track.where(({or, exists}) =>
    or(
      exists('album', q => q.where('title', 'Big Ones')),
      exists('genre', q => q.where('name', 'Rock')),
    ),
  ),
);

// ============================================
// Sophisticated queries using `related`
// ============================================

// Full invoice report with customer, support rep, and line items with tracks
benchmarkQuery(
  'invoice.related(customer, lines.related(track))',
  queries.invoice
    .related('customer', c => c.related('supportRep'))
    .related('lines', line => line.related('track')),
);

// Full invoice report - even deeper nesting
benchmarkQuery(
  'invoice deep: customer->supportRep->reportsTo, lines->track->album->artist',
  queries.invoice
    .related('customer', c =>
      c.related('supportRep', rep => rep.related('reportsToEmployee')),
    )
    .related('lines', line =>
      line.related('track', t => t.related('album', a => a.related('artist'))),
    ),
);

// Playlist with all track data including album and artist
benchmarkQuery(
  'playlist.related(tracks.related(album.related(artist), genre))',
  queries.playlist.related('tracks', t =>
    t.related('album', a => a.related('artist')).related('genre'),
  ),
);

// Artist catalog - artist with all albums and their tracks
benchmarkQuery(
  'artist.related(albums.related(tracks))',
  queries.artist.related('albums', a => a.related('tracks')),
);

// Artist catalog with full track details
benchmarkQuery(
  'artist.related(albums.related(tracks.related(genre, mediaType)))',
  queries.artist.related('albums', a =>
    a.related('tracks', t => t.related('genre').related('mediaType')),
  ),
);

// Track with all related data
benchmarkQuery(
  'track.related(album.related(artist), genre, mediaType, playlists)',
  queries.track
    .related('album', a => a.related('artist'))
    .related('genre')
    .related('mediaType')
    .related('playlists'),
);

// ============================================
// Combined `exists` and `related` queries
// ============================================

// Invoices for customers in USA with line items
benchmarkQuery(
  'invoice.whereExists(customer where country=USA).related(lines)',
  queries.invoice
    .whereExists('customer', c => c.where('country', 'USA'))
    .related('lines', line => line.related('track')),
);

// Playlists that contain Rock tracks, with full track data
benchmarkQuery(
  'playlist.whereExists(tracks.whereExists(genre=Rock)).related(tracks)',
  queries.playlist
    .whereExists('tracks', t =>
      t.whereExists('genre', g => g.where('name', 'Rock')),
    )
    .related('tracks', t =>
      t.related('album', a => a.related('artist')).related('genre'),
    ),
);

// Artists who have albums with tracks in specific genre
benchmarkQuery(
  'artist.whereExists(albums.whereExists(tracks.whereExists(genre=Rock)))',
  queries.artist.whereExists('albums', a =>
    a.whereExists('tracks', t =>
      t.whereExists('genre', g => g.where('name', 'Rock')),
    ),
  ),
);

// Tracks that have been purchased (exist in invoice lines)
benchmarkQuery(
  'track.whereExists(invoiceLines).related(album, genre)',
  queries.track
    .whereExists('invoiceLines')
    .related('album', a => a.related('artist'))
    .related('genre'),
);

// Complex: Customers who bought rock tracks, with their invoices
benchmarkQuery(
  'customer.whereExists(deep invoice->line->track->genre=Rock).related(supportRep)',
  queries.customer
    .whereExists('supportRep', rep => rep.whereExists('reportsToEmployee'))
    .related('supportRep'),
);

// ============================================
// Filtered `related` queries
// ============================================

// Invoice with only high-quantity line items
benchmarkQuery(
  'invoice.related(lines where quantity>1, customer)',
  queries.invoice
    .related('lines', line => line.where('quantity', '>', 1).related('track'))
    .related('customer'),
);

// Album with filtered tracks (only long tracks)
benchmarkQuery(
  'album.related(tracks where milliseconds>300000, artist)',
  queries.album
    .related('tracks', t => t.where('milliseconds', '>', 300000))
    .related('artist'),
);

// Album with ordered tracks and artist
benchmarkQuery(
  'album.related(tracks orderBy name, artist)',
  queries.album
    .related('tracks', t => t.orderBy('name', 'asc').limit(10))
    .related('artist'),
);

// ============================================
// Complex OR conditions with related
// ============================================

// Tracks from specific albums OR genres, with full data
benchmarkQuery(
  'track.where(OR album=BigOnes, genre=Rock).related(album, genre)',
  queries.track
    .where(({or, exists}) =>
      or(
        exists('album', a => a.where('title', 'Big Ones')),
        exists('genre', g => g.where('name', 'Rock')),
      ),
    )
    .related('album', a => a.related('artist'))
    .related('genre'),
);

// Check if JSON output is requested via environment variable
const format = process.env.BENCH_OUTPUT_FORMAT;

if (format === 'json') {
  // Output JSON without samples for smaller, cleaner output
  await run({
    format: {
      json: {
        samples: false,
        debug: false,
      },
    },
  });
} else {
  await run();
}

test('no-op', () => {
  expect(true).toBe(true);
});
