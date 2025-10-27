import {bench, run, summary} from 'mitata';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {expect, test} from 'vitest';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';

const pgContent = await getChinook();

const {dbs, queries} = await bootstrap({
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
function benchmarkPlanning<TTable extends keyof typeof schema.tables & string>(
  name: string,
  query: Query<typeof schema, TTable>,
) {
  const unplannedAST = query.ast;
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  bench(name, () => {
    planQuery(mappedAST, costModel);
  });
}

// // Simple queries (1-2 exists)
// summary(() => {
//   benchmarkPlanning(
//     '1 exists: track.exists(album)',
//     queries.sqlite.track.whereExists('album'),
//   );

//   benchmarkPlanning(
//     '2 exists (AND): track.exists(album).exists(genre)',
//     queries.sqlite.track.whereExists('album').whereExists('genre'),
//   );
// });

// // Medium complexity (3-5 exists)
// summary(() => {
//   benchmarkPlanning(
//     '3 exists (AND)',
//     queries.sqlite.track
//       .whereExists('album')
//       .whereExists('genre')
//       .whereExists('mediaType'),
//   );

//   benchmarkPlanning(
//     '5 exists (AND)',
//     queries.sqlite.track
//       .whereExists('album')
//       .whereExists('genre')
//       .whereExists('mediaType')
//       .whereExists('playlists')
//       .where('unitPrice', '>', 0),
//   );

//   benchmarkPlanning(
//     '3 exists (OR)',
//     queries.sqlite.track.where(({or, exists}) =>
//       or(
//         exists('album', q => q.where('title', 'Big Ones')),
//         exists('genre', q => q.where('name', 'Rock')),
//         exists('mediaType', q => q.where('name', 'MPEG audio file')),
//       ),
//     ),
//   );
// });

// // Nested exists (depth testing)
// summary(() => {
//   benchmarkPlanning(
//     'Nested 2 levels: track > album > artist',
//     queries.sqlite.track.whereExists('album', q => q.whereExists('artist')),
//   );

//   benchmarkPlanning(
//     'Nested 3 levels: playlist > tracks > album > artist',
//     queries.sqlite.playlist.whereExists('tracks', q =>
//       q.whereExists('album', q2 => q2.whereExists('artist')),
//     ),
//   );

//   benchmarkPlanning(
//     'Nested with filters: track > album > artist (filtered)',
//     queries.sqlite.track.whereExists('album', q =>
//       q
//         .where('title', 'Big Ones')
//         .whereExists('artist', q2 => q2.where('name', 'Aerosmith')),
//     ),
//   );
// });

// // Complex queries (10 exists)
// summary(() => {
//   benchmarkPlanning(
//     '10 exists (AND)',
//     queries.sqlite.track
//       .whereExists('album')
//       .whereExists('genre')
//       .whereExists('mediaType')
//       .whereExists('playlists')
//       .whereExists('album')
//       .whereExists('genre')
//       .whereExists('mediaType')
//       .whereExists('playlists')
//       .whereExists('album')
//       .whereExists('genre'),
//   );

//   benchmarkPlanning(
//     '10 exists (OR)',
//     queries.sqlite.track.where(({or, exists}) =>
//       or(
//         exists('album', q => q.where('id', 1)),
//         exists('album', q => q.where('id', 2)),
//         exists('album', q => q.where('id', 3)),
//         exists('album', q => q.where('id', 4)),
//         exists('album', q => q.where('id', 5)),
//         exists('genre', q => q.where('id', 1)),
//         exists('genre', q => q.where('id', 2)),
//         exists('genre', q => q.where('id', 3)),
//         exists('mediaType', q => q.where('id', 1)),
//         exists('mediaType', q => q.where('id', 2)),
//       ),
//     ),
//   );
// });

// Very complex queries (15+ exists)
summary(() => {
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
  );

  // benchmarkPlanning(
  //   '13 exists (AND)',
  //   queries.sqlite.track
  //     .whereExists('album')
  //     .whereExists('genre')
  //     .whereExists('mediaType')
  //     .whereExists('album')
  //     .whereExists('genre')
  //     .whereExists('mediaType')
  //     .whereExists('album')
  //     .whereExists('genre')
  //     .whereExists('mediaType')
  //     .whereExists('album')
  //     .whereExists('genre')
  //     .whereExists('mediaType')
  //     .whereExists('album'),
  // );

  // benchmarkPlanning(
  //   '15 exists (OR)',
  //   queries.sqlite.track.where(({or, exists}) =>
  //     or(
  //       exists('album', q => q.where('id', 1)),
  //       exists('album', q => q.where('id', 2)),
  //       exists('album', q => q.where('id', 3)),
  //       exists('album', q => q.where('id', 4)),
  //       exists('album', q => q.where('id', 5)),
  //       exists('genre', q => q.where('id', 1)),
  //       exists('genre', q => q.where('id', 2)),
  //       exists('genre', q => q.where('id', 3)),
  //       exists('genre', q => q.where('id', 4)),
  //       exists('genre', q => q.where('id', 5)),
  //       exists('mediaType', q => q.where('id', 1)),
  //       exists('mediaType', q => q.where('id', 2)),
  //       exists('mediaType', q => q.where('id', 3)),
  //       exists('mediaType', q => q.where('id', 4)),
  //       exists('mediaType', q => q.where('id', 5)),
  //     ),
  //   ),
  // );

  // benchmarkPlanning(
  //   'Mixed: 5 AND + 5 OR + 5 nested',
  //   queries.sqlite.track
  //     .whereExists('album')
  //     .whereExists('genre')
  //     .whereExists('mediaType')
  //     .whereExists('playlists')
  //     .whereExists('album', q => q.whereExists('artist'))
  //     .where(({or, exists}) =>
  //       or(
  //         exists('genre', q => q.where('id', 1)),
  //         exists('genre', q => q.where('id', 2)),
  //         exists('genre', q => q.where('id', 3)),
  //         exists('genre', q => q.where('id', 4)),
  //         exists('genre', q => q.where('id', 5)),
  //       ),
  //     ),
  // );
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
