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

// Bootstrap setup
export const pgContent = await getChinook();

export const {dbs, queries, delegates} = await bootstrap({
  suiteName: 'chinook_planner_exec',
  pgContent,
  zqlSchema: schema,
});

// Global state for planner infrastructure
export let costModel: ReturnType<typeof createSQLiteCostModel>;
export let mapper: NameMapper;
export let tableSpecs: Map<string, LiteAndZqlSpec>;

/**
 * Initialize planner infrastructure - call this in beforeAll()
 */
export function initializePlannerInfrastructure(): void {
  mapper = clientToServer(schema.tables);
  dbs.sqlite.exec('ANALYZE;');

  // Get table specs using computeZqlSpecs
  tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(createSilentLogContext(), dbs.sqlite, tableSpecs);

  costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);
}

// Type definitions

export type PlanAttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  actualRowsScanned: number;
  flipPattern: number;
};

export type PlanValidation =
  | 'correlation'
  | 'cost-tolerance'
  | 'better-than-baseline';

export type ValidationResult = {
  type: PlanValidation;
  passed: boolean;
  details: string;
};

// Validation functions

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
 * Validate correlation between estimated costs and actual costs using Spearman correlation
 */
export function validateCorrelation(
  results: PlanAttemptResult[],
): ValidationResult {
  const estimatedCosts = results.map(r => r.estimatedCost);
  const actualCosts = results.map(r => r.actualRowsScanned);
  const correlation = spearmanCorrelation(estimatedCosts, actualCosts);
  const passed = correlation >= 0.7;

  const details = passed
    ? `Spearman correlation ${correlation.toFixed(3)} >= 0.7 threshold`
    : `Spearman correlation ${correlation.toFixed(3)} < 0.7 threshold`;

  return {
    type: 'correlation',
    passed,
    details,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the optimal plan (lowest actual rows scanned)
 */
export function validatePickedVsOptimal(
  results: PlanAttemptResult[],
  toleranceFactor: number,
): ValidationResult {
  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // Find the optimal plan (lowest actual rows scanned)
  const optimalPlan = results.reduce((best, current) =>
    current.actualRowsScanned < best.actualRowsScanned ? current : best,
  );

  // Calculate ratio
  const ratio = pickedPlan.actualRowsScanned / optimalPlan.actualRowsScanned;
  const passed = ratio <= toleranceFactor;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is within ${toleranceFactor}x of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} exceeds ${toleranceFactor}x tolerance of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'cost-tolerance',
    passed,
    details,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) performs better than
 * the baseline query-as-written (attempt 0)
 */
export function validateBetterThanBaseline(
  results: PlanAttemptResult[],
): ValidationResult {
  // Find the baseline plan (attempt 0 - query as written)
  const baselinePlan = results.find(r => r.attemptNumber === 0);
  if (!baselinePlan) {
    throw new Error('Baseline plan (attempt 0) not found in results');
  }

  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // If the picked plan IS the baseline, that's OK - the planner didn't make it worse
  if (pickedPlan.attemptNumber === baselinePlan.attemptNumber) {
    return {
      type: 'better-than-baseline',
      passed: true,
      details: `Picked plan is the baseline (attempt ${baselinePlan.attemptNumber}) - no optimization applied`,
    };
  }

  // Check if picked plan is better than baseline
  const passed = pickedPlan.actualRowsScanned <= baselinePlan.actualRowsScanned;
  const ratio =
    baselinePlan.actualRowsScanned > 0
      ? pickedPlan.actualRowsScanned / baselinePlan.actualRowsScanned
      : 1;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is better than baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is worse than baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'better-than-baseline',
    passed,
    details,
  };
}

/**
 * Execute all planning attempts for a query and measure estimated vs actual costs
 */
export function executeAllPlanAttempts(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query: any,
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

    // Restore the exact plan state from the snapshot
    plans.plan.restorePlanningSnapshot(planEvent.planSnapshot);

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
