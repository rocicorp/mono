// oxlint-disable no-console
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
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
import {Database} from '../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {bootstrap} from './runner.ts';
export {
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type PlanAttemptResult,
  type PlanValidation,
  type ValidationResult,
  type TestSummary,
} from './planner-validation.ts';
import type {PlanAttemptResult} from './planner-validation.ts';

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

// =============================================================================
// Infrastructure types
// =============================================================================

export type PlannerInfrastructure = {
  dbs: Awaited<ReturnType<typeof bootstrap>>['dbs'];
  queries: Awaited<ReturnType<typeof bootstrap>>['queries'];
  delegates: Awaited<ReturnType<typeof bootstrap>>['delegates'];
  costModel: ReturnType<typeof createSQLiteCostModel>;
  mapper: NameMapper;
  tableSpecs: Map<string, LiteAndZqlSpec>;
  indexedDb: Database;
  indexedDelegate: ReturnType<typeof newQueryDelegate>;
  indexedCostModel: ReturnType<typeof createSQLiteCostModel>;
  indexedTableSpecs: Map<string, LiteAndZqlSpec>;
  initializePlannerInfrastructure: () => void;
  initializeIndexedDatabase: () => void;
  executeAllPlanAttempts: (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    query: any,
    useIndexedDb?: boolean,
    maxEstimatedCost?: number | undefined,
  ) => PlanAttemptResult[];
};

// =============================================================================
// Infrastructure factory
// =============================================================================

/**
 * Create planner infrastructure for a given schema and dataset.
 * This sets up databases, cost models, and execution functions.
 */
export async function createPlannerInfrastructure(config: {
  suiteName: string;
  pgContent: string;
  schema: Schema;
  indices: string[];
}): Promise<PlannerInfrastructure> {
  const {suiteName, pgContent, schema, indices} = config;

  // Bootstrap databases
  const {dbs, queries, delegates} = await bootstrap({
    suiteName,
    pgContent,
    zqlSchema: schema,
  });

  // Create a copy of the baseline SQLite database for the indexed version
  const indexedDbFile = dbs.sqliteFile.replace('.db', '-indexed.db');

  // Use VACUUM INTO to create a proper copy of the database (handles WAL files)
  dbs.sqlite.exec(`VACUUM INTO '${indexedDbFile}'`);

  // Create a second Database connection to the indexed file
  const indexedDb = new Database(createSilentLogContext(), indexedDbFile);

  // Set journal mode to WAL2 to match the original
  indexedDb.pragma('journal_mode = WAL2');

  // Create a query delegate for the indexed database
  const indexedDelegate = newQueryDelegate(
    createSilentLogContext(),
    testLogConfig,
    indexedDb,
    schema,
  );

  // Mutable state for cost models (initialized later)
  let costModel: ReturnType<typeof createSQLiteCostModel>;
  let mapper: NameMapper;
  let tableSpecs = new Map<string, LiteAndZqlSpec>();
  let indexedCostModel: ReturnType<typeof createSQLiteCostModel>;
  let indexedTableSpecs = new Map<string, LiteAndZqlSpec>();

  /**
   * Initialize planner infrastructure - call this in beforeAll()
   */
  function initializePlannerInfrastructure(): void {
    mapper = clientToServer(schema.tables);
    dbs.sqlite.exec('ANALYZE;');

    // Get table specs using computeZqlSpecs
    tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(
      createSilentLogContext(),
      dbs.sqlite,
      {includeBackfillingColumns: false},
      tableSpecs,
    );

    costModel = createSQLiteCostModel(dbs.sqlite, tableSpecs);
  }

  /**
   * Initialize indexed database infrastructure with extra indices on commonly-queried columns.
   * This allows us to compare planner performance with better statistics.
   */
  function initializeIndexedDatabase(): void {
    // Add indices on columns used in query predicates (to the indexed database copy)
    for (const indexSql of indices) {
      indexedDb.exec(indexSql);
    }

    // Run ANALYZE to generate new statistics with indices
    indexedDb.exec('ANALYZE;');

    // Get table specs with indexed statistics
    indexedTableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(
      createSilentLogContext(),
      indexedDb,
      {includeBackfillingColumns: false},
      indexedTableSpecs,
    );

    indexedCostModel = createSQLiteCostModel(indexedDb, indexedTableSpecs);
  }

  /**
   * Execute all planning attempts for a query and measure estimated vs actual costs
   * @param query The ZQL query to execute
   * @param useIndexedDb If true, use the indexed database's cost model for planning
   * @param maxEstimatedCost If provided, skip executing plans with estimated cost above this threshold
   */
  function executeAllPlanAttempts(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    query: any,
    useIndexedDb = false,
    maxEstimatedCost?: number | undefined,
  ): PlanAttemptResult[] {
    // Get the query AST
    const ast = mapAST(
      completeOrdering(
        asQueryInternals(query).ast,
        tableName => schema.tables[tableName].primaryKey,
      ),
      mapper,
    );

    // Select the cost model and delegate based on which database to use
    const selectedCostModel = useIndexedDb ? indexedCostModel : costModel;
    const selectedDelegate = useIndexedDb ? indexedDelegate : delegates.sqlite;

    // Plan with debugger to collect all attempts
    const planDebugger = new AccumulatorDebugger();
    planQuery(ast, selectedCostModel, planDebugger);

    // Get all completed plan attempts
    const planCompleteEvents = planDebugger.getEvents('plan-complete');
    // console.log(planDebugger.format());

    const results: PlanAttemptResult[] = [];

    // Execute each plan variant
    for (const planEvent of planCompleteEvents) {
      // Skip plans that exceed cost threshold (but always include baseline attempt 0)
      if (
        maxEstimatedCost !== undefined &&
        planEvent.totalCost > maxEstimatedCost &&
        planEvent.attemptNumber !== 0
      ) {
        console.log(
          `Skipping plan ${planEvent.attemptNumber}: estimated cost ${planEvent.totalCost.toFixed(2)} exceeds threshold ${maxEstimatedCost}`,
        );
        continue;
      }

      // Reset delegate state before each iteration to ensure clean row counting
      selectedDelegate.debug = undefined;
      selectedDelegate.mapAst = undefined;

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

  return {
    dbs,
    queries,
    delegates,
    get costModel() {
      return costModel;
    },
    get mapper() {
      return mapper;
    },
    get tableSpecs() {
      return tableSpecs;
    },
    indexedDb,
    indexedDelegate,
    get indexedCostModel() {
      return indexedCostModel;
    },
    get indexedTableSpecs() {
      return indexedTableSpecs;
    },
    initializePlannerInfrastructure,
    initializeIndexedDatabase,
    executeAllPlanAttempts,
  };
}
