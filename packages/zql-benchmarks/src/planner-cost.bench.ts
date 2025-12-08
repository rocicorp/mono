import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import type {Query} from '../../zql/src/query/query-builder.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';

const pgContent = await getChinook();

const {dbs, queries} = await bootstrap({
  suiteName: 'planner_cost_bench',
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

// Create name mapper
const clientToServerMapper = clientToServer(schema.tables);

// Helper to benchmark planning time
function benchmarkPlanning<TTable extends keyof typeof schema.tables & string>(
  name: string,
  query: Query<TTable, typeof schema>,
) {
  const unplannedAST = asQueryInternals(query).ast;
  const completeOrderAst = completeOrdering(
    unplannedAST,
    tableName =>
      must(tables[tableName], `Table ${tableName} not found`).primaryKey,
  );
  const mappedAST = mapAST(completeOrderAst, clientToServerMapper);

  bench(name, () => {
    planQuery(mappedAST, costModel);
  });
}

summary(() => {
  benchmarkPlanning(
    '1 exists: track.exists(album)',
    queries.track.whereExists('album'),
  );

  benchmarkPlanning(
    '2 exists (AND): track.exists(album).exists(genre)',
    queries.track.whereExists('album').whereExists('genre'),
  );

  benchmarkPlanning(
    '3 exists (AND)',
    queries.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType'),
  );

  benchmarkPlanning(
    '3 exists (OR)',
    queries.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('title', 'Big Ones')),
        exists('genre', q => q.where('name', 'Rock')),
        exists('mediaType', q => q.where('name', 'MPEG audio file')),
      ),
    ),
  );

  benchmarkPlanning(
    '5 exists (AND)',
    queries.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('invoiceLines')
      .whereExists('playlistTrackJunction'),
  );

  benchmarkPlanning(
    '5 exists (OR)',
    queries.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('title', 'Big Ones')),
        exists('genre', q => q.where('name', 'Rock')),
        exists('mediaType', q => q.where('name', 'MPEG audio file')),
        exists('album', q => q.where('title', 'Big Ones 2')),
        exists('genre', q => q.where('name', 'Pop')),
      ),
    ),
  );

  benchmarkPlanning(
    'Nested 2 levels: track > album > artist',
    queries.track.whereExists('album', q => q.whereExists('artist')),
  );

  benchmarkPlanning(
    'Nested 4 levels: playlist > tracks > album > artist',
    queries.playlist.whereExists('tracks', q =>
      q.whereExists('album', q2 => q2.whereExists('artist')),
    ),
  );

  benchmarkPlanning(
    'Nested with filters: track > album > artist (filtered)',
    queries.track.whereExists('album', q =>
      q
        .where('title', 'Big Ones')
        .whereExists('artist', q2 => q2.where('name', 'Aerosmith')),
    ),
  );

  benchmarkPlanning(
    '10 exists (AND)',
    queries.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('album')
      .whereExists('genre')
      .whereExists('playlistTrackJunction')
      .whereExists('invoiceLines'),
  );

  benchmarkPlanning(
    '10 exists (OR)',
    queries.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('id', 1)),
        exists('album', q => q.where('id', 2)),
        exists('album', q => q.where('id', 3)),
        exists('album', q => q.where('id', 4)),
        exists('album', q => q.where('id', 5)),
        exists('genre', q => q.where('id', 1)),
        exists('genre', q => q.where('id', 2)),
        exists('genre', q => q.where('id', 3)),
        exists('mediaType', q => q.where('id', 1)),
        exists('mediaType', q => q.where('id', 2)),
      ),
    ),
  );

  benchmarkPlanning(
    '12 exists (AND)',
    queries.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType'),
  );

  benchmarkPlanning(
    '12 exists (OR)',
    queries.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('id', 1)),
        exists('album', q => q.where('id', 2)),
        exists('album', q => q.where('id', 3)),
        exists('album', q => q.where('id', 4)),
        exists('album', q => q.where('id', 5)),
        exists('genre', q => q.where('id', 1)),
        exists('genre', q => q.where('id', 2)),
        exists('genre', q => q.where('id', 3)),
        exists('genre', q => q.where('id', 4)),
        exists('genre', q => q.where('id', 5)),
        exists('mediaType', q => q.where('id', 1)),
        exists('mediaType', q => q.where('id', 2)),
      ),
    ),
  );

  benchmarkPlanning(
    '12 level nesting',
    queries.track.whereExists('playlists', q =>
      q.whereExists('tracks', q2 =>
        q2.whereExists('album', q3 =>
          q3.whereExists('artist', q4 =>
            q4.whereExists('albums', q5 =>
              q5.whereExists('tracks', q6 =>
                q6.whereExists('genre', q7 =>
                  q7.whereExists('tracks', q8 =>
                    q8.whereExists('mediaType', q9 =>
                      q9.whereExists('tracks', q => q.whereExists('album')),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
  );
});

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
