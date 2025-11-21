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
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
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

// Create a copy of the baseline SQLite database for the indexed version
const indexedDbFile = dbs.sqliteFile.replace('.db', '-indexed.db');

// Use VACUUM INTO to create a proper copy of the database (handles WAL files)
dbs.sqlite.exec(`VACUUM INTO '${indexedDbFile}'`);

// Create a second Database connection to the indexed file
export const indexedDb = new Database(createSilentLogContext(), indexedDbFile);

// Set journal mode to WAL2 to match the original
indexedDb.pragma('journal_mode = WAL2');

// Create a query delegate for the indexed database
export const indexedDelegate = newQueryDelegate(
  createSilentLogContext(),
  testLogConfig,
  indexedDb,
  schema,
);

// Global state for planner infrastructure
export let costModel: ReturnType<typeof createSQLiteCostModel>;
export let mapper: NameMapper;
export let tableSpecs: Map<string, LiteAndZqlSpec>;

// Global state for indexed database infrastructure
export let indexedCostModel: ReturnType<typeof createSQLiteCostModel>;
export let indexedTableSpecs: Map<string, LiteAndZqlSpec>;

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

/**
 * Initialize indexed database infrastructure with extra indices on commonly-queried columns.
 * This allows us to compare planner performance with better statistics.
 */
export function initializeIndexedDatabase(): void {
  // Add indices on columns used in query predicates (to the indexed database copy)
  const indices = [
    'CREATE INDEX IF NOT EXISTS idx_album_title ON album(title)',
    'CREATE INDEX IF NOT EXISTS idx_artist_name ON artist(name)',
    'CREATE INDEX IF NOT EXISTS idx_track_composer ON track(composer)',
    'CREATE INDEX IF NOT EXISTS idx_track_milliseconds ON track(milliseconds)',
    'CREATE INDEX IF NOT EXISTS idx_track_name ON track(name)',
    'CREATE INDEX IF NOT EXISTS idx_genre_name ON genre(name)',
    'CREATE INDEX IF NOT EXISTS idx_employee_title ON employee(title)',
    'CREATE INDEX IF NOT EXISTS idx_customer_country ON customer(country)',
    'CREATE INDEX IF NOT EXISTS idx_playlist_name ON playlist(name)',
    'CREATE INDEX IF NOT EXISTS idx_invoice_line_quantity ON invoice_line(quantity)',
  ];

  for (const indexSql of indices) {
    indexedDb.exec(indexSql);
  }

  // Run ANALYZE to generate new statistics with indices
  indexedDb.exec('ANALYZE;');

  // Get table specs with indexed statistics
  indexedTableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(createSilentLogContext(), indexedDb, indexedTableSpecs);

  indexedCostModel = createSQLiteCostModel(indexedDb, indexedTableSpecs);
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
 * @param query The ZQL query to execute
 * @param useIndexedDb If true, use the indexed database's cost model for planning
 */
export function executeAllPlanAttempts(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query: any,
  useIndexedDb = false,
): PlanAttemptResult[] {
  // Get the query AST
  const ast = mapAST(
    completeOrdering(asQueryInternals(query).ast, tableName => {
      const s: Schema = schema;
      return s.tables[tableName].primaryKey;
    }),
    mapper,
  );

  // Select the cost model and delegate based on which database to use
  const selectedCostModel = useIndexedDb ? indexedCostModel : costModel;
  const selectedDelegate = useIndexedDb ? indexedDelegate : delegates.sqlite;

  // Plan with debugger to collect all attempts
  const planDebugger = new AccumulatorDebugger();
  planQuery(ast, selectedCostModel, planDebugger);
  // console.log(planDebugger.format());

  // Get all completed plan attempts
  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  const results: PlanAttemptResult[] = [];

  // Execute each plan variant
  for (const planEvent of planCompleteEvents) {
    // Rebuild the plan graph for this attempt
    const plans = buildPlanGraph(ast, selectedCostModel, true);

    // Restore the exact plan state from the snapshot
    plans.plan.restorePlanningSnapshot(planEvent.planSnapshot);

    // Apply plans to AST to get variant with flip flags set
    const astWithFlips = applyPlansToAST(ast, plans);

    // Enable row count tracking
    runtimeDebugFlags.trackRowCountsVended = true;
    const debug = new Debug();
    selectedDelegate.debug = debug;

    try {
      // Build pipeline
      selectedDelegate.mapAst = undefined;
      const pipeline = buildPipeline(
        astWithFlips,
        selectedDelegate,
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
