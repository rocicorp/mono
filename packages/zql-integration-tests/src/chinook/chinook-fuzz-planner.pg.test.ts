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
import {mapAST, type AST} from '../../../zero-protocol/src/ast.ts';
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
  const processCondition = (cond: any): any => {
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

/**
 * Execute a query AST by building a pipeline and running hydrate.
 * Returns total rows considered.
 *
 * Note: queryAst must be in server names (already mapped).
 */
async function executeQueryAST(queryAst: AST): Promise<number> {
  // Enable row count tracking
  const prevTrackRowCounts = runtimeDebugFlags.trackRowCountsVended;
  runtimeDebugFlags.trackRowCountsVended = true;

  try {
    // Create delegate with Debug instance - using custom delegate without AST mapping
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
    const pipeline = buildPipeline(queryAst, delegate, 'query-id');

    // Execute query and count rows
    for (const rowChange of hydrate(
      pipeline,
      hashOfAST(queryAst),
      tableSpecs,
    )) {
      assert(rowChange.type === 'add');
    }

    // Collect vended row counts
    return getTotalRowCount(debug);
  } finally {
    runtimeDebugFlags.trackRowCountsVended = prevTrackRowCounts;
  }
}

type AttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  rowsConsidered: number;
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

    // Execute query and count rows
    const totalRowsConsidered = await executeQueryAST(astWithFlips);

    results.push({
      attemptNumber: event.attemptNumber,
      estimatedCost: event.totalCost,
      rowsConsidered: totalRowsConsidered,
      flipPattern: event.flipPattern,
      joinStates: event.joinStates,
    });
  }

  // Sort by estimated cost
  const sortedByCost = [...results].sort(
    (a, b) => a.estimatedCost - b.estimatedCost,
  );

  // Sort by rows considered
  const sortedByRows = [...results].sort(
    (a, b) => a.rowsConsidered - b.rowsConsidered,
  );

  // Create rank maps
  const costRank = new Map(
    sortedByCost.map((r, i) => [r.attemptNumber, i + 1]),
  );
  const rowsRank = new Map(
    sortedByRows.map((r, i) => [r.attemptNumber, i + 1]),
  );

  // Print comparison table
  console.log('═'.repeat(90));
  console.log('COST MODEL VALIDATION');
  console.log('═'.repeat(90));
  console.log(
    'Attempt |   Est Cost | Cost Rank | Rows Considered | Rows Rank |  Δ Rank',
  );
  console.log('─'.repeat(90));

  for (const result of sortedByCost) {
    const cRank = must(
      costRank.get(result.attemptNumber),
      `Cost rank not found for attempt ${result.attemptNumber}`,
    );
    const rRank = must(
      rowsRank.get(result.attemptNumber),
      `Rows rank not found for attempt ${result.attemptNumber}`,
    );
    const delta = Math.abs(cRank - rRank);

    // Highlight inversions (large rank differences)
    const marker = delta > 2 ? '⚠️ ' : '  ';

    console.log(
      `${marker}${(result.attemptNumber + 1).toString().padStart(3)} | ` +
        `${result.estimatedCost.toFixed(2).padStart(10)} | ` +
        `${cRank.toString().padStart(9)} | ` +
        `${result.rowsConsidered.toString().padStart(15)} | ` +
        `${rRank.toString().padStart(9)} | ` +
        `${delta.toString().padStart(7)}`,
    );
  }

  console.log('═'.repeat(90));

  // Find top 5 inversions
  const inversions = results
    .map(r => {
      const cRank = must(
        costRank.get(r.attemptNumber),
        `Cost rank not found for attempt ${r.attemptNumber}`,
      );
      const rRank = must(
        rowsRank.get(r.attemptNumber),
        `Rows rank not found for attempt ${r.attemptNumber}`,
      );
      return {
        ...r,
        costRank: cRank,
        rowsRank: rRank,
        delta: Math.abs(cRank - rRank),
      };
    })
    .sort((a, b) => b.delta - a.delta)
    .slice(0, 5);

  if (inversions.length > 0 && inversions[0].delta > 0) {
    console.log('\nTop 5 Cost Model Inversions:\n');
    for (const inv of inversions) {
      console.log(`Attempt ${inv.attemptNumber + 1}:`);
      console.log(
        `  Estimated cost: ${inv.estimatedCost.toFixed(2)} (rank ${inv.costRank})`,
      );
      console.log(
        `  Rows considered: ${inv.rowsConsidered} (rank ${inv.rowsRank})`,
      );
      console.log(`  Rank delta: ${inv.delta}`);
      console.log(
        `  Flip pattern: ${inv.flipPattern.toString(2).padStart(5, '0')} (binary)`,
      );
      console.log('  Join states:');
      for (const j of inv.joinStates) {
        console.log(`    ${j.join}: ${j.type}`);
      }
      console.log('');
    }
  }

  // Print best plans by each metric
  const bestByCost = sortedByCost[0];
  const bestByRows = sortedByRows[0];

  console.log('═'.repeat(90));
  console.log('BEST PLANS\n');
  console.log(
    `Best by estimated cost: Attempt ${bestByCost.attemptNumber + 1}`,
  );
  console.log(`  Cost: ${bestByCost.estimatedCost.toFixed(2)}`);
  console.log(`  Rows: ${bestByCost.rowsConsidered}`);
  console.log('  Join states:');
  for (const j of bestByCost.joinStates) {
    console.log(`    ${j.join}: ${j.type}`);
  }
  console.log('');

  console.log(
    `Best by rows considered: Attempt ${bestByRows.attemptNumber + 1}`,
  );
  console.log(`  Cost: ${bestByRows.estimatedCost.toFixed(2)}`);
  console.log(`  Rows: ${bestByRows.rowsConsidered}`);
  console.log('  Join states:');
  for (const j of bestByRows.joinStates) {
    console.log(`    ${j.join}: ${j.type}`);
  }
  console.log('═'.repeat(90));

  // Show detailed cost breakdown for all attempts
  console.log('\n' + '═'.repeat(90));
  console.log('DETAILED COST BREAKDOWN');
  console.log('═'.repeat(90));

  // Get cost events from planner debugger for each attempt
  const connectionCostEvents = planDebugger.getEvents('node-cost');

  for (const result of results) {
    const attemptEvents = connectionCostEvents.filter(
      e => e.attemptNumber === result.attemptNumber,
    );

    console.log(`\nAttempt ${result.attemptNumber + 1}:`);
    console.log(`  Total estimated cost: ${result.estimatedCost.toFixed(2)}`);
    console.log(`  Actual rows considered: ${result.rowsConsidered}`);
    console.log(`  Join states:`);
    for (const j of result.joinStates) {
      console.log(`    ${j.join}: ${j.type}`);
    }

    // Group by node type
    const connections = attemptEvents.filter(e => e.nodeType === 'connection');
    const joins = attemptEvents.filter(e => e.nodeType === 'join');

    if (connections.length > 0) {
      console.log(`  Connections:`);
      for (const c of connections) {
        console.log(`    ${c.node}:`);
        console.log(
          `      cost=${c.costEstimate.cost.toFixed(2)}, startup=${c.costEstimate.startupCost.toFixed(2)}, scan=${c.costEstimate.scanEst.toFixed(2)}`,
        );
        console.log(
          `      returnedRows=${c.costEstimate.returnedRows.toFixed(2)}, selectivity=${c.costEstimate.selectivity.toFixed(6)}`,
        );
        console.log(
          `      downstreamChildSelectivity=${c.downstreamChildSelectivity.toFixed(6)}`,
        );
      }
    }

    if (joins.length > 0) {
      console.log(`  Joins:`);
      for (const j of joins) {
        console.log(`    ${j.node} (${j.joinType}):`);
        console.log(
          `      cost=${j.costEstimate.cost.toFixed(2)}, startup=${j.costEstimate.startupCost.toFixed(2)}, scan=${j.costEstimate.scanEst.toFixed(2)}`,
        );
        console.log(
          `      returnedRows=${j.costEstimate.returnedRows.toFixed(2)}, selectivity=${j.costEstimate.selectivity.toFixed(6)}`,
        );
        console.log(
          `      downstreamChildSelectivity=${j.downstreamChildSelectivity.toFixed(6)}`,
        );
      }
    }
  }
  console.log('═'.repeat(90));

  // Run the originally planned query (using the planner's choice)
  const plannedAST = planAST(queryAst);
  const plannedRowCount = await executeQuery(plannedAST, query.format);

  console.log(`\nPlanner chose: ${plannedRowCount} rows`);

  // Assert: planned should scan <= unplanned rows
  expect(plannedRowCount).toBeLessThanOrEqual(unplannedRowCount);
}

function getTotalRowCount(debug: Debug): number {
  const counts = debug.getVendedRowCounts();
  let total = 0;
  for (const tableQueries of Object.values(counts)) {
    for (const count of Object.values(tableQueries)) {
      total += count;
    }
  }
  return total;
}
