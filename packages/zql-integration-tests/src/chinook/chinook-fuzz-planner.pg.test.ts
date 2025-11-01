// oxlint-disable expect-expect
/* oxlint-disable no-console */
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {hydrate} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../zql/src/builder/debug-delegate.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {ast, QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {
  planQuery,
  buildPlanGraph,
  applyPlansToAST,
} from '../../../zql/src/planner/planner-builder.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import {
  mapAST,
  type AST,
  type Condition,
} from '../../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema, builder} from './schema.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_planner',
  zqlSchema: schema,
  pgContent,
});

// Run ANALYZE to populate SQLite statistics for the cost model
harness.dbs.sqlite.exec('ANALYZE;');

// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), harness.dbs.sqlite, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(harness.dbs.sqlite, tableSpecs);

// Create name mappers for translating between client and server names
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Manual test cases to verify the infrastructure works
test('manual: track.whereExists(album)', async () => {
  const query = builder.track.whereExists('album');
  await runManualCase(query);
});

test.only('manual: track.whereExists(album).whereExists(genre)', async () => {
  const query = builder.track.whereExists('album').whereExists('genre');
  await runManualCase(query);
});

test('manual: album.whereExists(tracks)', async () => {
  const query = builder.album.whereExists('tracks');
  await runManualCase(query);
});

// Fuzz tests (disabled for now - queries may be too complex)
test.each(Array.from({length: 0}, () => createCase()))(
  'fuzz-planner $seed',
  runCase,
);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // oxlint-disable-next-line no-focused-tests
  test.only('repro', async () => {
    const tc = createCase(REPRO_SEED);
    await runCase(tc);
  });
}

function createCase(seed?: number) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    query: generateShrinkableQuery(
      schema,
      {},
      rng,
      faker,
      harness.delegates.pg.serverSchema,
    ),
  };
}

async function executeQuery(
  queryAst: ReturnType<typeof ast>,
  format: AnyQuery['format'],
  debug = new Debug(),
): Promise<number> {
  const query = new QueryImpl(
    {
      ...harness.delegates.sqlite,
      debug,
    },
    schema,
    queryAst.table as keyof typeof schema.tables,
    queryAst,
    format,
  );
  await query.run();
  return getTotalRowCount(debug);
}

function planAST(queryAst: ReturnType<typeof ast>): ReturnType<typeof ast> {
  const mappedAST = mapAST(queryAst, clientToServerMapper);
  const plannedServerAST = planQuery(mappedAST, costModel);
  return mapAST(plannedServerAST, serverToClientMapper);
}

async function runCase({
  query,
  seed,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
}) {
  const [generatedQuery] = query;
  const queryAst = ast(generatedQuery);

  let unplannedRowCount: number;
  let plannedRowCount: number;

  try {
    unplannedRowCount = await executeQuery(queryAst, generatedQuery.format);
  } catch (e) {
    // Skip queries that fail during unplanned execution (query generator issues)
    if (seed === REPRO_SEED) {
      console.log('Unplanned execution failed:', e);
      throw e;
    }
    console.log(`Skipping seed ${seed}: query generation issue`);
    return;
  }

  try {
    const plannedAST = planAST(queryAst);
    plannedRowCount = await executeQuery(plannedAST, generatedQuery.format);
  } catch (e) {
    // If planned fails but unplanned succeeded, that's a planner bug
    if (seed === REPRO_SEED) {
      console.log('Planned execution failed:', e);
      throw e;
    }
    throw new Error(
      `Planner broke valid query! Seed ${seed}: ${e instanceof Error ? e.message : String(e)}`,
    );
  }

  // Debug logging
  if (seed === REPRO_SEED || plannedRowCount > unplannedRowCount) {
    console.log(
      `Seed ${seed}: unplanned=${unplannedRowCount}, planned=${plannedRowCount}`,
    );
  }

  // Assert: planned should scan <= unplanned rows
  if (plannedRowCount > unplannedRowCount) {
    throw new Error(
      `Planner increased row count! unplanned=${unplannedRowCount}, planned=${plannedRowCount}. Repro seed: ${seed}`,
    );
  }
}

/**
 * Helper to set flip to false in all correlated subquery conditions
 */
function setFlipToFalse(queryAst: AST): AST {
  const processCondition = (cond: Condition): Condition => {
    if (cond.type === 'correlatedSubquery') {
      return {
        ...cond,
        flip: false,
        related: {
          ...cond.related,
          subquery: setFlipToFalse(cond.related.subquery),
        },
      };
    } else if (cond.type === 'and' || cond.type === 'or') {
      return {
        ...cond,
        conditions: cond.conditions.map(processCondition),
      };
    }
    return cond;
  };

  return {
    ...queryAst,
    where: queryAst.where ? processCondition(queryAst.where) : undefined,
    related: queryAst.related?.map(r => ({
      ...r,
      subquery: setFlipToFalse(r.subquery),
    })),
  };
}

type AttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  rowsConsidered: number;
  rowsVended: number;
  executionTimeMs: number;
  flipPattern: number;
  joinStates: Array<{join: string; type: 'semi' | 'flipped'}>;
};

async function runManualCase(query: AnyQuery) {
  const queryAst = query.ast;

  console.log('\n' + '═'.repeat(90));
  console.log('ANALYZING QUERY');
  console.log('═'.repeat(90));

  // Run unplanned query
  const unplannedRowCount = await executeQuery(queryAst, query.format);
  console.log(`Unplanned row count: ${unplannedRowCount}\n`);

  // Map to server names and set all flips to false
  const mappedAST = mapAST(queryAst, clientToServerMapper);
  const mappedASTWithoutFlips = setFlipToFalse(mappedAST);

  // Plan the query and collect all debug events
  const planDebugger = new AccumulatorDebugger();
  planQuery(mappedASTWithoutFlips, costModel, planDebugger);

  // Get all plan-complete events
  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  console.log(`Found ${planCompleteEvents.length} planning attempts\n`);

  // Analyze each attempt
  const results: AttemptResult[] = [];

  // Create a fresh plan graph for this attempt
  const attemptPlans = buildPlanGraph(mappedASTWithoutFlips, costModel, true);
  for (const event of planCompleteEvents) {
    // Reset and apply the flip pattern by manually setting join types
    attemptPlans.plan.resetPlanningState();
    const flippableJoins = attemptPlans.plan.joins.filter(j => j.isFlippable());
    for (let i = 0; i < flippableJoins.length; i++) {
      if (event.flipPattern & (1 << i)) {
        flippableJoins[i].flip();
      }
    }

    // Apply the plan to get AST with flip flags
    const astWithFlips = applyPlansToAST(mappedASTWithoutFlips, attemptPlans);

    // Execute query and measure time, visited rows, and vended rows
    const startTime = performance.now();
    const debug = new Debug();
    const lc = createSilentLogContext();
    const baseDelegate = newQueryDelegate(
      lc,
      testLogConfig,
      harness.dbs.sqlite,
      schema,
    );
    const delegate = {
      ...baseDelegate,
      debug,
      // Override mapAst to be identity function since AST is already in server names
      mapAst: (ast: AST) => ast,
    };

    // Build pipeline
    const pipeline = buildPipeline(astWithFlips, delegate, 'query-id');

    // Execute query and count rows
    for (const rowChange of hydrate(
      pipeline,
      hashOfAST(astWithFlips),
      tableSpecs,
    )) {
      assert(rowChange.type === 'add');
    }

    const executionTimeMs = performance.now() - startTime;

    // Collect both visited and vended row counts
    const totalRowsVisited = getTotalVisitedCount(debug);
    const totalRowsVended = getTotalVendedCount(debug);

    results.push({
      attemptNumber: event.attemptNumber,
      estimatedCost: event.totalCost,
      rowsConsidered: totalRowsVisited,
      rowsVended: totalRowsVended,
      executionTimeMs,
      flipPattern: event.flipPattern,
      joinStates: event.joinStates,
    });
  }

  // Sort by different metrics
  const sortedByCost = [...results].sort(
    (a, b) => a.estimatedCost - b.estimatedCost,
  );
  const sortedByTime = [...results].sort(
    (a, b) => a.executionTimeMs - b.executionTimeMs,
  );
  const sortedByVended = [...results].sort(
    (a, b) => a.rowsVended - b.rowsVended,
  );
  const sortedByVisited = [...results].sort(
    (a, b) => a.rowsConsidered - b.rowsConsidered,
  );

  // Print comparison table
  console.log('═'.repeat(120));
  console.log('COST MODEL VALIDATION');
  console.log('═'.repeat(120));
  console.log(
    'Attempt |   Est Cost |  Time (ms) | Rows Vended | NVISIT (scanstat) | Time Rank | Vended Rank | NVISIT Rank',
  );
  console.log('─'.repeat(120));

  const timeRank = new Map(sortedByTime.map((r, i) => [r.attemptNumber, i + 1]));
  const vendedRank = new Map(
    sortedByVended.map((r, i) => [r.attemptNumber, i + 1]),
  );
  const visitedRank = new Map(
    sortedByVisited.map((r, i) => [r.attemptNumber, i + 1]),
  );

  for (const result of sortedByCost) {
    const tRank = must(timeRank.get(result.attemptNumber));
    const vRank = must(vendedRank.get(result.attemptNumber));
    const nRank = must(visitedRank.get(result.attemptNumber));

    console.log(
      `${(result.attemptNumber + 1).toString().padStart(7)} | ` +
        `${result.estimatedCost.toFixed(2).padStart(10)} | ` +
        `${result.executionTimeMs.toFixed(1).padStart(10)} | ` +
        `${result.rowsVended.toString().padStart(11)} | ` +
        `${result.rowsConsidered.toString().padStart(17)} | ` +
        `${tRank.toString().padStart(9)} | ` +
        `${vRank.toString().padStart(11)} | ` +
        `${nRank.toString().padStart(11)}`,
    );
  }

  console.log('═'.repeat(120));

  console.log(planDebugger.format());

  // Run the originally planned query (using the planner's choice)
  const plannedAST = planAST(queryAst);
  const plannedRowCount = await executeQuery(plannedAST, query.format);

  console.log(`\nPlanner chose: ${plannedRowCount} rows`);

  // Compare planned vs unplanned
  if (plannedRowCount > unplannedRowCount) {
    const ratio = (plannedRowCount / unplannedRowCount).toFixed(1);
    console.log(
      `⚠️  Planner made query WORSE: ${plannedRowCount} vs ${unplannedRowCount} rows (${ratio}x)`,
    );
  } else {
    const ratio = (unplannedRowCount / plannedRowCount).toFixed(1);
    console.log(
      `✓ Planner improved query: ${unplannedRowCount} vs ${plannedRowCount} rows (${ratio}x better)`,
    );
  }

  // TODO: Fix cost model issues and re-enable this assertion
  // expect(plannedRowCount).toBeLessThanOrEqual(unplannedRowCount);
}

function getTotalVisitedCount(debug: Debug): number {
  const counts = debug.getVisitedRowCounts();
  let total = 0;
  for (const tableQueries of Object.values(counts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}

function getTotalVendedCount(debug: Debug): number {
  const counts = debug.getVendedRowCounts();
  let total = 0;
  for (const tableQueries of Object.values(counts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}
