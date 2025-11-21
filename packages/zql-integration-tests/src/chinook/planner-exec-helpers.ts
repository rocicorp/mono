import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {hydrate} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {clientSchemaFrom} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../zql/src/builder/debug-delegate.ts';
import {
  applyPlansToAST,
  buildPlanGraph,
  planQuery,
} from '../../../zql/src/planner/planner-builder.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {completeOrdering} from '../../../zql/src/query/complete-ordering.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {spearmanCorrelation} from '../helpers/correlation.ts';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

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
  | ['correlation', number]
  | ['within-optimal', number]
  | ['within-baseline', number];

export type ValidationResult = {
  type: 'correlation' | 'within-optimal' | 'within-baseline';
  passed: boolean;
  details: string;
  actualValue: number;
  threshold: number;
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
  threshold: number,
): ValidationResult {
  const estimatedCosts = results.map(r => r.estimatedCost);
  const actualCosts = results.map(r => r.actualRowsScanned);
  const correlation = spearmanCorrelation(estimatedCosts, actualCosts);
  const passed = correlation >= threshold;

  const details = passed
    ? `Spearman correlation ${correlation.toFixed(3)} >= ${threshold} threshold`
    : `Spearman correlation ${correlation.toFixed(3)} < ${threshold} threshold`;

  return {
    type: 'correlation',
    passed,
    details,
    actualValue: correlation,
    threshold,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the optimal plan (lowest actual rows scanned)
 */
export function validateWithinOptimal(
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
    type: 'within-optimal',
    passed,
    details,
    actualValue: ratio,
    threshold: toleranceFactor,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the baseline query-as-written (attempt 0)
 * Formula: picked <= baseline × toleranceFactor
 * - toleranceFactor < 1.0: picked must be better (e.g., 0.5 = picked must be ≤50% of baseline)
 * - toleranceFactor = 1.0: picked must be as good or better than baseline
 * - toleranceFactor > 1.0: picked can be worse (e.g., 1.5 = picked can be ≤150% of baseline)
 */
export function validateWithinBaseline(
  results: PlanAttemptResult[],
  toleranceFactor: number,
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

  // Check if picked plan is within tolerance of baseline
  const maxAllowedCost = baselinePlan.actualRowsScanned * toleranceFactor;
  const passed = pickedPlan.actualRowsScanned <= maxAllowedCost;
  const ratio =
    baselinePlan.actualRowsScanned > 0
      ? pickedPlan.actualRowsScanned / baselinePlan.actualRowsScanned
      : 1;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is within ${toleranceFactor}x of baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} exceeds ${toleranceFactor}x tolerance of baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'within-baseline',
    passed,
    details,
    actualValue: ratio,
    threshold: toleranceFactor,
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
  const ast = mapAST(
    completeOrdering(asQueryInternals(query).ast, tableName => {
      const s: Schema = schema;
      return s.tables[tableName].primaryKey;
    }),
    mapper,
  );

  // Plan with debugger to collect all attempts
  const planDebugger = new AccumulatorDebugger();
  planQuery(ast, costModel, planDebugger);
  console.log(planDebugger.format());

  // Get all completed plan attempts
  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  const results: PlanAttemptResult[] = [];

  // Execute each plan variant
  for (const planEvent of planCompleteEvents) {
    // Rebuild the plan graph for this attempt
    const plans = buildPlanGraph(ast, costModel, true);

    // Restore the exact plan state from the snapshot
    plans.plan.restorePlanningSnapshot(planEvent.planSnapshot);

    // Apply plans to AST to get variant with flip flags set
    const astWithFlips = applyPlansToAST(ast, plans);

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
        clientSchemaFrom(schema).clientSchema,
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
