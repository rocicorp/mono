// oxlint-disable no-non-null-assertion
// oxlint-disable no-console
/**
 * Analyze all planning attempts for a query to validate cost model.
 *
 * This script:
 * 1. Plans a query and collects debug events from all attempts
 * 2. For each attempt, reconstructs the AST with the flip pattern
 * 3. Executes the query and collects "rows considered" metric
 * 4. Compares estimated cost vs actual rows considered
 *
 * Usage: npx tsx src/analyze-all-plans.ts
 */

import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from '../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {mapAST, type AST, type Condition} from '../../zero-protocol/src/ast.ts';
import {AccumulatorDebugger} from '../../zql/src/planner/planner-debug.ts';
import {schema, builder} from './schema.ts';
import {runtimeDebugFlags} from '../../zql/src/builder/debug-delegate.ts';

// Open the zbugs SQLite database
const db = new Database(
  createSilentLogContext(),
  '/Users/mlaw/workspace/mono/apps/zbugs/zbugs-replica.db',
);
const lc = createSilentLogContext();

// Run ANALYZE to populate SQLite statistics
db.exec('ANALYZE;');

// Get table specs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(lc, db, tableSpecs);

// Create SQLite cost model
const costModel = createSQLiteCostModel(db, tableSpecs);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);

// Helper to set flip to false in all correlated subquery conditions
function setFlipToFalse(ast: AST): AST {
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
    ...ast,
    where: ast.where ? processCondition(ast.where) : undefined,
    related: ast.related?.map(r => ({
      ...r,
      subquery: setFlipToFalse(r.subquery),
    })),
  };
}

// The query to analyze: issueList with multiple labels
const query = builder.issue
  .whereExists('project', p => p.where('lowerCaseName', 'roci'), {flip: true})
  .whereExists('labels', q => q.where('name', 'bug'), {flip: true})
  .whereExists('labels', q => q.where('name', 'armor'), {flip: true})
  .orderBy('modified', 'desc')
  .orderBy('id', 'desc')
  .limit(50);

const originalAST = query.ast;

// Map to server names and set all flips to false
const mappedAST = mapAST(originalAST, clientToServerMapper);
const mappedASTWithoutFlips = setFlipToFalse(mappedAST);

// Plan the query and collect all debug events
const planDebugger = new AccumulatorDebugger();
planQuery(mappedASTWithoutFlips, costModel, planDebugger);

// Get all plan-complete events
const planCompleteEvents = planDebugger.getEvents('plan-complete');

console.log(planDebugger.format());
console.log(`Found ${planCompleteEvents.length} planning attempts\n`);

// Analyze each attempt
type AttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  rowsConsidered: number;
  flipPattern: number;
  joinStates: Array<{join: string; type: 'semi' | 'flipped'}>;
};

const results: AttemptResult[] = [];

// Enable row count tracking
runtimeDebugFlags.trackRowCountsVended = true;

// we know these already. So we don't have to run the query again
// because the flip pattern does not change between plan runs.
const rowsConsidered = [
  289325, 289321, 487042, 487038, 47918, 48914, 3236, 4232, 289110, 289106,
  486827, 486823, 47703, 48699, 3021, 4017, 47406, 48169, 47237, 48000, 46634,
  46633, 46465, 46464, 2491, 3254, 2322, 3085, 1719, 1718, 1550, 1549,
];

for (const event of planCompleteEvents) {
  /*
  // Create a fresh plan graph for this attempt
  const attemptPlans = buildPlanGraph(mappedASTWithoutFlips, costModel);

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
  // console.log('AST with flips applied:', JSON.stringify(astWithFlips, null, 2));

  // Create delegate with Debug instance
  const debug = new Debug();
  const delegate = {
    ...newQueryDelegate(lc, testLogConfig, db, schema),
    debug,
  };

  // Build pipeline
  const pipeline = buildPipeline(astWithFlips, delegate, 'query-id');

  // Execute query and count rows
  let rowCount = 0;
  for (const rowChange of hydrate(
    pipeline,
    hashOfAST(astWithFlips),
    tableSpecs,
  )) {
    assert(rowChange.type === 'add');
    rowCount++;
  }

  // Collect vended row counts
  const vendedRowCounts = debug.getVendedRowCounts();
  let totalRowsConsidered = 0;
  for (const tableCounts of Object.values(vendedRowCounts)) {
    for (const count of Object.values(tableCounts)) {
      totalRowsConsidered += count;
    }
  }
  */

  results.push({
    attemptNumber: event.attemptNumber,
    estimatedCost: event.totalCost,
    rowsConsidered: rowsConsidered[event.attemptNumber],
    flipPattern: event.flipPattern,
    joinStates: event.joinStates,
  });

  // console.log(`  Estimated cost: ${event.totalCost.toFixed(2)}`);
  // console.log(`  Rows considered: ${totalRowsConsidered}`);
  // console.log(`  Output rows: ${rowCount}\n`);
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
const costRank = new Map(sortedByCost.map((r, i) => [r.attemptNumber, i + 1]));
const rowsRank = new Map(sortedByRows.map((r, i) => [r.attemptNumber, i + 1]));

// Print comparison table
console.log('═'.repeat(90));
console.log('COST MODEL VALIDATION');
console.log('═'.repeat(90));
console.log(
  'Attempt |   Est Cost | Cost Rank | Rows Considered | Rows Rank |  Δ Rank',
);
console.log('─'.repeat(90));

for (const result of sortedByCost) {
  const cRank = costRank.get(result.attemptNumber)!;
  const rRank = rowsRank.get(result.attemptNumber)!;
  const delta = Math.abs(cRank - rRank);

  // Highlight inversions (large rank differences)
  const marker = delta > 5 ? '⚠️ ' : '  ';

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
// const inversions = results
//   .map(r => ({
//     ...r,
//     costRank: costRank.get(r.attemptNumber)!,
//     rowsRank: rowsRank.get(r.attemptNumber)!,
//     delta: Math.abs(
//       costRank.get(r.attemptNumber)! - rowsRank.get(r.attemptNumber)!,
//     ),
//   }))
//   .sort((a, b) => b.delta - a.delta)
//   .slice(0, 5);

// console.log('\nTop 5 Cost Model Inversions:\n');
// for (const inv of inversions) {
//   console.log(`Attempt ${inv.attemptNumber + 1}:`);
//   console.log(
//     `  Estimated cost: ${inv.estimatedCost.toFixed(2)} (rank ${inv.costRank})`,
//   );
//   console.log(
//     `  Rows considered: ${inv.rowsConsidered} (rank ${inv.rowsRank})`,
//   );
//   console.log(`  Rank delta: ${inv.delta}`);
//   console.log(
//     `  Flip pattern: ${inv.flipPattern.toString(2).padStart(5, '0')} (binary)`,
//   );
//   console.log('  Join states:');
//   for (const j of inv.joinStates) {
//     console.log(`    ${j.join}: ${j.type}`);
//   }
//   console.log('');
// }

// Print best plans by each metric
const bestByCost = sortedByCost[0];
const bestByRows = sortedByRows[0];

console.log('═'.repeat(90));
console.log('BEST PLANS\n');
console.log(`Best by estimated cost: Attempt ${bestByCost.attemptNumber + 1}`);
console.log(`  Cost: ${bestByCost.estimatedCost.toFixed(2)}`);
console.log(`  Rows: ${bestByCost.rowsConsidered}`);
console.log('  Join states:');
for (const j of bestByCost.joinStates) {
  console.log(`    ${j.join}: ${j.type}`);
}
console.log('');

console.log(`Best by rows considered: Attempt ${bestByRows.attemptNumber + 1}`);
console.log(`  Cost: ${bestByRows.estimatedCost.toFixed(2)}`);
console.log(`  Rows: ${bestByRows.rowsConsidered}`);
console.log('  Join states:');
for (const j of bestByRows.joinStates) {
  console.log(`    ${j.join}: ${j.type}`);
}
console.log('═'.repeat(90));
