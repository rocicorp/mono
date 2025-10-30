import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';

const pgContent = await getChinook();

const {dbs, queries, delegates} = await bootstrap({
  suiteName: 'planner_cost_bench',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for cost model
dbs.sqlite.exec('ANALYZE;');

// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), dbs.sqlite, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);

// Create name mapper
const clientToServerMapper = clientToServer(schema.tables);

// Helper to benchmark planning time
function benchmarkPlanning<TTable extends keyof typeof schema.tables>(
  name: string,
  query: Query<typeof schema, TTable>,
  delegate: QueryDelegate<unknown>,
) {
  const unplannedAST = delegate.withContext(query).ast;
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  bench(name, () => {
    planQuery(mappedAST, costModel);
  });
}

summary(() => {
  benchmarkPlanning(
    '1 exists: track.exists(album)',
    queries.sqlite.track.whereExists('album'),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '2 exists (AND): track.exists(album).exists(genre)',
    queries.sqlite.track.whereExists('album').whereExists('genre'),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '3 exists (AND)',
    queries.sqlite.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType'),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '3 exists (OR)',
    queries.sqlite.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('title', 'Big Ones')),
        exists('genre', q => q.where('name', 'Rock')),
        exists('mediaType', q => q.where('name', 'MPEG audio file')),
      ),
    ),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '5 exists (AND)',
    queries.sqlite.track
      .whereExists('album')
      .whereExists('genre')
      .whereExists('mediaType')
      .whereExists('invoiceLines')
      .whereExists('playlistTrackJunction'),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '5 exists (OR)',
    queries.sqlite.track.where(({or, exists}) =>
      or(
        exists('album', q => q.where('title', 'Big Ones')),
        exists('genre', q => q.where('name', 'Rock')),
        exists('mediaType', q => q.where('name', 'MPEG audio file')),
        exists('album', q => q.where('title', 'Big Ones 2')),
        exists('genre', q => q.where('name', 'Pop')),
      ),
    ),
    delegates.sqlite,
  );

  benchmarkPlanning(
    'Nested 2 levels: track > album > artist',
    queries.sqlite.track.whereExists('album', q => q.whereExists('artist')),
    delegates.sqlite,
  );

  benchmarkPlanning(
    'Nested 4 levels: playlist > tracks > album > artist',
    queries.sqlite.playlist.whereExists('tracks', q =>
      q.whereExists('album', q2 => q2.whereExists('artist')),
    ),
    delegates.sqlite,
  );

  benchmarkPlanning(
    'Nested with filters: track > album > artist (filtered)',
    queries.sqlite.track.whereExists('album', q =>
      q
        .where('title', 'Big Ones')
        .whereExists('artist', q2 => q2.where('name', 'Aerosmith')),
    ),
    delegates.sqlite,
  );

  benchmarkPlanning(
    '10 exists (AND)',
    queries.sqlite.track
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
    delegates.sqlite,
  );

  benchmarkPlanning(
    '10 exists (OR)',
    queries.sqlite.track.where(({or, exists}) =>
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
    delegates.sqlite,
  );

  benchmarkPlanning(
    '12 exists (AND)',
    queries.sqlite.track
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
    delegates.sqlite,
  );

  benchmarkPlanning(
    '12 exists (OR)',
    queries.sqlite.track.where(({or, exists}) =>
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
    delegates.sqlite,
  );

  benchmarkPlanning(
    '12 level nesting',
    queries.sqlite.track.whereExists('playlists', q =>
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
    delegates.sqlite,
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
