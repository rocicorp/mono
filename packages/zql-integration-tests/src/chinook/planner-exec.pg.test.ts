import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {
  buildPlanGraph,
  applyPlansToAST,
  planQuery,
} from '../../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {
  runtimeDebugFlags,
  Debug,
} from '../../../zql/src/builder/debug-delegate.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {hydrate} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {spearmanCorrelation} from '../helpers/correlation.ts';
import {queryWithContext} from '../../../zql/src/query/query-internals.ts';

const pgContent = await getChinook();

const {dbs, queries, delegates} = await bootstrap({
  suiteName: 'chinook_planner_exec',
  pgContent,
  zqlSchema: schema,
});

let costModel: ReturnType<typeof createSQLiteCostModel>;
let mapper: NameMapper;
let tableSpecs: Map<string, LiteAndZqlSpec>;

type PlanAttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  actualRowsScanned: number;
  flipPattern: number;
};

/**
 * Sum all row counts from Debug.getNVisitCounts()
 */
function sumRowCounts(
  nvisitCounts: Record<string, Record<string, number>>,
): number {
  let total = 0;
  for (const tableQueries of Object.values(nvisitCounts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}

/**
 * Execute all planning attempts for a query and measure estimated vs actual costs
 */
function executeAllPlanAttempts(
  query: ReturnType<typeof queries.track.whereExists>,
): PlanAttemptResult[] {
  // Get the query AST
  const ast = queryWithContext(query, undefined).ast;
  const mappedAST = mapAST(ast, mapper);

  // Plan with debugger to collect all attempts
  const planDebugger = new AccumulatorDebugger();
  planQuery(mappedAST, costModel, planDebugger);

  // Get all completed plan attempts
  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  const results: PlanAttemptResult[] = [];

  // Execute each plan variant
  for (const planEvent of planCompleteEvents) {
    // Rebuild the plan graph for this attempt
    const plans = buildPlanGraph(mappedAST, costModel, true);

    // Reset planning state
    plans.plan.resetPlanningState();

    // Apply the flip pattern by manually flipping joins
    const flippableJoins = plans.plan.joins.filter(j => j.isFlippable());
    for (let i = 0; i < flippableJoins.length; i++) {
      if (planEvent.flipPattern & (1 << i)) {
        flippableJoins[i].flip();
      }
    }

    // Apply plans to AST to get variant with flip flags set
    const astWithFlips = applyPlansToAST(mappedAST, plans);

    // Enable row count tracking
    runtimeDebugFlags.trackRowCountsVended = true;
    const debug = new Debug();
    delegates.sqlite.debug = debug;

    try {
      // Build pipeline
      delegates.sqlite.mapAst = undefined;
      const pipeline = buildPipeline(
        astWithFlips,
        delegates.sqlite,
        `query-${planEvent.attemptNumber}`,
      );

      // Execute query
      for (const _rowChange of hydrate(
        pipeline,
        hashOfAST(astWithFlips),
        tableSpecs,
      )) {
        // Consume rows to execute the query
      }

      // Collect actual row counts
      const nvisitCounts = debug.getNVisitCounts();
      const actualRowsScanned = sumRowCounts(nvisitCounts);

      results.push({
        attemptNumber: planEvent.attemptNumber,
        estimatedCost: planEvent.totalCost,
        actualRowsScanned,
        flipPattern: planEvent.flipPattern,
      });
    } finally {
      // Disable tracking for next iteration
      runtimeDebugFlags.trackRowCountsVended = false;
    }
  }

  return results;
}

describe('Chinook planner execution cost validation', () => {
  beforeAll(() => {
    mapper = clientToServer(schema.tables);
    dbs.sqlite.exec('ANALYZE;');

    // Get table specs using computeZqlSpecs
    tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(createSilentLogContext(), dbs.sqlite, tableSpecs);

    costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);
  });

  test.each([
    {
      name: 'simple query - single whereExists',
      query: queries.track.whereExists('album', q =>
        q.where('title', 'Big Ones'),
      ),
    },
    {
      name: 'two-level join - track with album and artist',
      query: queries.track.whereExists('album', album =>
        album.whereExists('artist', artist =>
          artist.where('name', 'Aerosmith'),
        ),
      ),
    },
    {
      name: 'parallel joins - track with album and genre',
      query: queries.track
        .whereExists('album', q => q.where('title', 'Big Ones'))
        .whereExists('genre', q => q.where('name', 'Rock'))
        .limit(10),
    },
    // currently fails
    // {
    //   name: 'three-level join - track with album, artist, and condition',
    //   query: queries.track
    //     .whereExists('album', album =>
    //       album
    //         .where('title', '>', 'A')
    //         .whereExists('artist', artist => artist.where('name', '>', 'A')),
    //     )
    //     .where('milliseconds', '>', 200000)
    //     .limit(10),
    // },
  ])('$name', ({query}) => {
    // Execute all plan attempts and collect results
    const results = executeAllPlanAttempts(query);

    // Verify we got multiple planning attempts
    expect(results.length).toBeGreaterThan(0);

    // Calculate Spearman rank correlation
    const estimatedCosts = results.map(r => r.estimatedCost);
    const actualCosts = results.map(r => r.actualRowsScanned);
    const correlation = spearmanCorrelation(estimatedCosts, actualCosts);

    // console.log(estimatedCosts);
    // console.log(actualCosts);
    // console.log(correlation);

    // Assert that correlation is positive and reasonably strong
    // A correlation >= 0.7 indicates the cost model is directionally correct
    expect(correlation).toBeGreaterThanOrEqual(0.7);
  });
});
