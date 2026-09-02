import {assert} from '../../shared/src/asserts.ts';
import {bench, describe} from '../../shared/src/bench.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {
  BoundedPlanCache,
  type PlanCache,
} from '../../zql/src/builder/plan-cache.ts';
import {TestBuilderDelegate} from '../../zql/src/builder/test-builder-delegate.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {defaultFormat} from '../../zql/src/ivm/default-format.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {completeOrdering} from '../../zql/src/query/complete-ordering.ts';
import {newQueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {ROSTER_AST, TRACKERS_AST} from './assignment-wave-asts.ts';
import {
  createAssignmentWaveReplica,
  createAssignmentWaveSources,
  primaryKeys,
} from './assignment-wave-replica.ts';

const pgContent = await getChinook();

const {dbs, delegates, queries} = await bootstrap({
  suiteName: 'planner_hydration_bench',
  zqlSchema: schema,
  pgContent,
});

dbs.sqlite.exec('CREATE INDEX IF NOT EXISTS idx_album_title ON album(title)');
dbs.sqlite.exec('CREATE INDEX IF NOT EXISTS idx_genre_name ON genre(name)');

// Run ANALYZE after index creation to populate SQLite statistics for cost model.
dbs.sqlite.exec('ANALYZE;');

const tables: {[key: string]: TableSchema} = schema.tables;
// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(
  createSilentLogContext(),
  dbs.sqlite,
  {includeBackfillingColumns: false},
  tableSpecs,
);

// Create SQLite cost model
const costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Helper to create a query from an AST
function createQuery(tableName: string, queryAST: AST): AnyQuery {
  return newQueryImpl(
    schema,
    tableName as keyof typeof schema.tables,
    queryAST,
    defaultFormat,
    'test',
  );
}

// Helper to benchmark planned vs unplanned
function benchmarkQuery(name: string, query: AnyQuery) {
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
  const tableName = unplannedAST.table;
  const unplannedQuery = createQuery(tableName, unplannedAST);
  const plannedQuery = createQuery(tableName, plannedClientAST);

  describe(name, () => {
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

// The assignment-wave ASTs from the zero-cache planning diagnosis, built end to
// end so that a cache hit is measured against the real cost it displaces:
// buildPipeline plus hydration, not planning alone.
const assignmentWave = createAssignmentWaveReplica();
const assignmentWaveCostModel = createSQLiteCostModel(
  assignmentWave.db,
  assignmentWave.tableSpecs,
);

function benchmarkAssignmentWaveHydration(name: string, ast: AST) {
  const plannerInput = completeOrdering(ast, tableName =>
    must(primaryKeys.get(tableName), `Table ${tableName} not found`),
  );

  const hydrate = (planCache: PlanCache | undefined) => {
    const delegate = new TestBuilderDelegate(
      createAssignmentWaveSources(assignmentWave.db, assignmentWave.tableSpecs),
    );
    delegate.planCache = planCache;
    return new Catch(
      buildPipeline(
        plannerInput,
        delegate,
        'assignment-wave-bench',
        assignmentWaveCostModel,
      ),
    ).fetch();
  };

  const warm: PlanCache = {
    store: new BoundedPlanCache(1024, 16 * 1024 * 1024),
    epoch: 'e',
  };
  // A cache hit must produce exactly the rows an uncached build produces.
  const uncachedRows = hydrate(undefined);
  hydrate(warm);
  assert(
    JSON.stringify(hydrate(warm)) === JSON.stringify(uncachedRows),
    `Cached hydration of ${name} differs from uncached`,
  );

  describe(name, () => {
    bench(`build + hydrate, plan cache off: ${name}`, () => {
      hydrate(undefined);
    });

    bench(`build + hydrate, plan cache cold: ${name}`, () => {
      hydrate({
        store: new BoundedPlanCache(1024, 16 * 1024 * 1024),
        epoch: 'e',
      });
    });

    bench(`build + hydrate, plan cache warm: ${name}`, () => {
      hydrate(warm);
    });
  });
}

describe('assignment wave hydration', () => {
  benchmarkAssignmentWaveHydration('assignment.roster', ROSTER_AST);
  benchmarkAssignmentWaveHydration(
    'problem_trackers.for_assignment',
    TRACKERS_AST,
  );
});
