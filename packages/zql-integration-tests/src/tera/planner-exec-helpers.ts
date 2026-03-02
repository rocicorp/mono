// oxlint-disable no-console
import path from 'node:path';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {hydrate} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {mapAST} from '../../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import {clientSchemaFrom} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../zql/src/builder/debug-delegate.ts';
import {defaultFormat} from '../../../zql/src/ivm/default-format.ts';
import {
  applyPlansToAST,
  buildPlanGraph,
  planQuery,
} from '../../../zql/src/planner/planner-builder.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {completeOrdering} from '../../../zql/src/query/complete-ordering.ts';
import {newQueryImpl} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type PlanAttemptResult,
  type PlanValidation,
  type ValidationResult,
  type TestSummary,
} from '../helpers/planner-validation.ts';
import {schema} from './schema.ts';

// =============================================================================
// Infrastructure
// =============================================================================

const lc = createSilentLogContext();
const dbPath = path.resolve(
  import.meta.dirname,
  '..',
  '..',
  '..',
  '..',
  'tera.db',
);

const db = new Database(lc, dbPath);

const delegate = newQueryDelegate(lc, testLogConfig, db, schema);
const mapper = clientToServer(schema.tables);

type Queries = {
  [K in keyof (typeof schema)['tables'] & string]: Query<K, typeof schema>;
};

export const queries: Queries = Object.fromEntries(
  Object.keys(schema.tables).map(t => [
    t,
    newQueryImpl(
      schema,
      t as keyof (typeof schema)['tables'] & string,
      {table: t},
      defaultFormat,
      'test',
    ),
  ]),
) as unknown as Queries;

// Mutable state initialized by initializePlannerInfrastructure
let costModel: ReturnType<typeof createSQLiteCostModel>;
let tableSpecs: Map<string, LiteAndZqlSpec>;

/**
 * Initialize planner infrastructure.
 * tera.db already has sqlite_stat1/sqlite_stat4 populated, so no ANALYZE needed.
 */
export function initializePlannerInfrastructure(): void {
  tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);
  costModel = createSQLiteCostModel(db, tableSpecs);
}

// =============================================================================
// Row count helpers
// =============================================================================

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
// Plan execution
// =============================================================================

/**
 * Execute all planning attempts for a query and measure estimated vs actual costs.
 * @param query The ZQL query to execute
 * @param maxEstimatedCost If provided, skip executing plans with estimated cost above this threshold
 */
export function executeAllPlanAttempts(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  query: any,
  maxEstimatedCost?: number,
): PlanAttemptResult[] {
  const ast = mapAST(
    completeOrdering(
      asQueryInternals(query).ast,
      tableName =>
        schema.tables[tableName as keyof (typeof schema)['tables']].primaryKey,
    ),
    mapper,
  );

  const planDebugger = new AccumulatorDebugger();
  planQuery(ast, costModel, planDebugger);

  const planCompleteEvents = planDebugger.getEvents('plan-complete');

  const results: PlanAttemptResult[] = [];

  for (const planEvent of planCompleteEvents) {
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

    delegate.debug = undefined;
    delegate.mapAst = undefined;

    const plans = buildPlanGraph(ast, costModel, true);
    plans.plan.restorePlanningSnapshot(planEvent.planSnapshot);

    const astWithFlips = applyPlansToAST(ast, plans);

    runtimeDebugFlags.trackRowCountsVended = true;
    const debug = new Debug();
    delegate.debug = debug;

    try {
      const pipeline = buildPipeline(
        astWithFlips,
        delegate,
        `query-${planEvent.attemptNumber}`,
      );

      for (const _rowChange of hydrate(
        pipeline,
        hashOfAST(astWithFlips),
        clientSchemaFrom(schema).clientSchema,
      )) {
        // Consume rows to execute the query
      }

      const nvisitCounts = debug.getNVisitCounts();
      const actualRowsScanned = sumRowCounts(nvisitCounts);

      results.push({
        attemptNumber: planEvent.attemptNumber,
        estimatedCost: planEvent.totalCost,
        actualRowsScanned,
        flipPattern: planEvent.flipPattern,
      });
    } finally {
      runtimeDebugFlags.trackRowCountsVended = false;
    }
  }

  return results;
}

// Re-export types and validation functions from shared module
export {
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type PlanAttemptResult,
  type PlanValidation,
  type ValidationResult,
  type TestSummary,
};
