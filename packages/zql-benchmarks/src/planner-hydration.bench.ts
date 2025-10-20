import {bench, run, summary} from 'mitata';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {ast, QueryImpl} from '../../zql/src/query/query-impl.ts';
import {defaultFormat} from '../../zql/src/ivm/default-format.ts';
import type {Query} from '../../zql/src/query/query.ts';

const pgContent = await getChinook();

const {dbs, delegates, queries} = await bootstrap({
  suiteName: 'planner_hydration_bench',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for cost model
dbs.sqlite.exec('ANALYZE;');

// Create SQLite cost model
const costModel = createSQLiteCostModel(
  dbs.sqlite,
  Object.fromEntries(
    Object.entries(schema.tables).map(([k, v]) => [
      'serverName' in v ? v.serverName : k,
      {
        columns: Object.fromEntries(
          Object.entries(v.columns).map(([colName, col]) => [
            'serverName' in col ? col.serverName : colName,
            {
              ...col,
            },
          ]),
        ),
        primaryKey: v.primaryKey,
      },
    ]),
  ),
);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Helper to create a query from an AST
function createQuery<TTable extends keyof typeof schema.tables & string>(
  tableName: TTable,
  queryAST: AST,
) {
  return new QueryImpl(
    delegates.sqlite,
    schema,
    tableName,
    queryAST,
    defaultFormat,
    'test',
  );
}

// Helper to benchmark planned vs unplanned
function benchmarkQuery<TTable extends keyof typeof schema.tables & string>(
  name: string,
  query: Query<typeof schema, TTable>,
  debug = false,
) {
  const unplannedAST = ast(query);

  // Map to server names, plan, then map back to client names
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  if (debug) {
    console.log(`\n=== DEBUG: ${name} ===`);
    console.log('Unplanned AST (client names):');
    console.log(JSON.stringify(unplannedAST, null, 2));
    console.log('\nMapped AST (server names):');
    console.log(JSON.stringify(mappedAST, null, 2));

    // Create logging cost model
    const loggingCostModel = (
      table: string,
      sort: any,
      filters: any,
      constraint: any,
    ) => {
      const cost = costModel(table, sort, filters, constraint);
      console.log(`Cost for ${table}:`, {
        constraint,
        filters: filters ? 'present' : 'none',
        cost,
      });
      return cost;
    };

    console.log('\nPlanning with costs:');
    const plannedServerAST = planQuery(mappedAST, loggingCostModel);

    console.log('\nPlanned AST (server names):');
    console.log(JSON.stringify(plannedServerAST, null, 2));

    const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);
    console.log('\nPlanned AST (client names):');
    console.log(JSON.stringify(plannedClientAST, null, 2));

    // Check flip decisions
    function checkFlips(ast: any, path: string = 'root') {
      if (ast.where) {
        if ('flip' in ast.where) {
          console.log(`${path}.where.flip = ${ast.where.flip}`);
        }
        if (ast.where.related?.subquery?.where) {
          checkFlips(
            ast.where.related.subquery,
            `${path}.where.related.subquery`,
          );
        }
        if (ast.where.conditions) {
          ast.where.conditions.forEach((c: any, i: number) => {
            if ('flip' in c) {
              console.log(`${path}.where.conditions[${i}].flip = ${c.flip}`);
            }
          });
        }
      }
    }

    console.log('\nFlip decisions:');
    checkFlips(plannedClientAST);
    console.log('=== END DEBUG ===\n');
  }

  const plannedServerAST = planQuery(mappedAST, costModel);
  const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

  const tableName = unplannedAST.table as TTable;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  const plannedQuery = createQuery(tableName, plannedClientAST);

  summary(() => {
    bench(`unplanned: ${name}`, async () => {
      await unplannedQuery.run();
    });

    bench(`planned: ${name}`, async () => {
      await plannedQuery.run();
    });
  });
}

// Print table row counts
console.log('\n=== TABLE ROW COUNTS ===');
const playlistCount = dbs.sqlite
  .prepare('SELECT COUNT(*) as count FROM playlist')
  .get() as {count: number};
const playlistTrackCount = dbs.sqlite
  .prepare('SELECT COUNT(*) as count FROM playlist_track')
  .get() as {count: number};
const trackCount = dbs.sqlite
  .prepare('SELECT COUNT(*) as count FROM track')
  .get() as {count: number};
console.log(`Playlist: ${playlistCount.count}`);
console.log(`PlaylistTrack: ${playlistTrackCount.count}`);
console.log(`Track: ${trackCount.count}\n`);

// Benchmark queries
benchmarkQuery(
  'track.exists(album) where title="Big Ones"',
  queries.sqlite.track.whereExists('album', q => q.where('title', 'Big Ones')),
);

benchmarkQuery(
  'track.exists(album).exists(genre)',
  queries.sqlite.track.whereExists('album').whereExists('genre'),
);

benchmarkQuery(
  'track.exists(album).exists(genre) with filters',
  queries.sqlite.track
    .whereExists('album', q => q.where('title', 'Big Ones'))
    .whereExists('genre', q => q.where('name', 'Rock')),
);

// DEBUG: Just print costs for playlist.exists(tracks) without running benchmark
{
  const query = queries.sqlite.playlist.whereExists('tracks');
  const unplannedAST = ast(query);
  const mappedAST = mapAST(unplannedAST, clientToServerMapper);

  console.log('\n=== DEBUG: playlist.exists(tracks) ===');
  console.log('Unplanned AST (client names):');
  console.log(JSON.stringify(unplannedAST, null, 2));

  // Create logging cost model
  const loggingCostModel = (
    table: string,
    sort: any,
    filters: any,
    constraint: any,
  ) => {
    const cost = costModel(table, sort, filters, constraint);
    console.log(`Cost for ${table}:`, {
      constraint,
      filters: filters ? 'present' : 'none',
      cost,
    });
    return cost;
  };

  console.log('\nPlanning with costs:');
  try {
    const plannedServerAST = planQuery(mappedAST, loggingCostModel);
    console.log('\nPlanned AST (server names):');
    console.log(JSON.stringify(plannedServerAST, null, 2));

    const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);
    console.log('\nPlanned AST (client names):');
    console.log(JSON.stringify(plannedClientAST, null, 2));
  } catch (e) {
    console.error('Planning failed:', e);
  }
  console.log('=== END DEBUG ===\n');
}

benchmarkQuery(
  'playlist.exists(tracks)',
  queries.sqlite.playlist.whereExists('tracks'),
);

benchmarkQuery(
  'track.exists(playlists)',
  queries.sqlite.track.whereExists('playlists'),
);

benchmarkQuery(
  'track.exists(album) OR exists(genre)',
  queries.sqlite.track.where(({or, exists}) =>
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
