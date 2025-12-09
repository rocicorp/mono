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
