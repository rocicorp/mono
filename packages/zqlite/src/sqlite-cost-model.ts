import SQLite3Database from '@rocicorp/zero-sqlite3';
import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {Condition, Ordering} from '../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../zero-types/src/schema-value.ts';
import type {
  ConnectionCostModel,
  CostModelCost,
} from '../../zql/src/planner/planner-connection.ts';
import type {PlannerConstraint} from '../../zql/src/planner/planner-constraint.ts';
import type {Database, Statement} from './db.ts';
import {compileInline} from './internal/sql-inline.ts';
import {buildSelectQuery, type NoSubqueryCondition} from './query-builder.ts';
import {SQLiteStatFanout} from './sqlite-stat-fanout.ts';

const NO_SUBQUERY_FILTER: NoSubqueryCondition | undefined = undefined;
const ALWAYS_FALSE_FILTER: NoSubqueryCondition = {type: 'or', conditions: []};
const OPTIMISTIC_BLEND_WEIGHT = 1;
const PESSIMISTIC_BLEND_WEIGHT = 2;

/**
 * Loop information returned by SQLite's scanstatus API.
 */
interface ScanstatusLoop {
  /** Unique identifier for this loop */
  selectId: number;
  /** Parent loop ID, or 0 for root loops */
  parentId: number;
  /** Estimated rows emitted per turn of parent loop */
  est: number;
  /** EXPLAIN text for this loop to determine: b-tree vs list subquery */
  explain: string;
}

/**
 * Creates a SQLite-based cost model for query planning.
 * Uses SQLite's scanstatus API to estimate query costs based on the actual
 * SQLite query planner's analysis.
 *
 * @param db Database instance for preparing statements
 * @param tableSpecs Map of table names to their table specs with ZQL schemas
 * @returns ConnectionCostModel function for use with the planner
 */
export function createSQLiteCostModel(
  db: Database,
  tableSpecs: Map<string, {zqlSpec: Record<string, SchemaValue>}>,
): ConnectionCostModel {
  const fanoutEstimator = new SQLiteStatFanout(db);
  return (
    tableName: string,
    sort: Ordering,
    filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ): CostModelCost => {
    const {zqlSpec} = must(tableSpecs.get(tableName));
    const fanout = (columns: string[]) =>
      fanoutEstimator.getFanout(tableName, columns);

    const estimateForFilters = (
      noSubqueryFilters: NoSubqueryCondition | undefined,
    ): CostModelCost => {
      const query = buildSelectQuery(
        tableName,
        zqlSpec,
        constraint,
        noSubqueryFilters,
        sort,
        undefined, // reverse is undefined here
        undefined, // start is undefined here
      );

      // Use compileInline to inline actual values into the SQL for cost estimation.
      // This allows SQLite's query planner to see real values and make better decisions
      // about index usage and query plans. This is safe here because it's only used for
      // cost estimation, not for executing user-facing queries (which use parameterized
      // queries via the standard compile() function).
      const sql = compileInline(query);

      // Prepare statement to get scanstatus information
      const stmt = db.prepare(sql);

      // Get scanstatus loops from the prepared statement
      const loops = getScanstatusLoops(stmt);

      // Scanstatus should always be available - if we get no loops, something is wrong
      assert(
        loops.length > 0,
        `Expected scanstatus to return at least one loop for query: ${sql}`,
      );

      return estimateCost(loops, fanout);
    };

    const approximations = filters
      ? getFilterApproximations(filters)
      : NO_FILTER_APPROXIMATIONS;

    const optimistic = estimateForFilters(approximations.optimistic);

    if (
      sameNoSubqueryCondition(
        approximations.optimistic,
        approximations.pessimistic,
      )
    ) {
      return optimistic;
    }

    const pessimistic = estimateForFilters(approximations.pessimistic);
    return blendApproximateCosts(optimistic, pessimistic);
  };
}

/**
 * Approximations of a filter after removing correlated subqueries.
 *
 * - optimistic: keep whatever simple predicates remain, even inside ORs.
 * - pessimistic: if an OR loses any branch, drop the whole OR from costing.
 *
 * The planner uses the optimistic estimate to preserve simple-filter signal,
 * then blends it with the pessimistic estimate so mixed ORs cannot make a root
 * scan look unrealistically selective.
 */
export type FilterApproximations = {
  optimistic: NoSubqueryCondition | undefined;
  pessimistic: NoSubqueryCondition | undefined;
};

const NO_FILTER_APPROXIMATIONS: FilterApproximations = {
  optimistic: NO_SUBQUERY_FILTER,
  pessimistic: NO_SUBQUERY_FILTER,
};

export function getFilterApproximations(
  condition: Condition,
): FilterApproximations {
  switch (condition.type) {
    case 'correlatedSubquery':
      return NO_FILTER_APPROXIMATIONS;
    case 'simple':
      return {optimistic: condition, pessimistic: condition};
    case 'and': {
      if (condition.conditions.some(isAlwaysFalseCondition)) {
        return {
          optimistic: ALWAYS_FALSE_FILTER,
          pessimistic: ALWAYS_FALSE_FILTER,
        };
      }
      const parts = condition.conditions.map(getFilterApproximations);
      return {
        optimistic: combineApproximationBranch('and', parts, 'optimistic'),
        pessimistic: combineApproximationBranch('and', parts, 'pessimistic'),
      };
    }
    case 'or': {
      if (condition.conditions.length === 0) {
        return {
          optimistic: ALWAYS_FALSE_FILTER,
          pessimistic: ALWAYS_FALSE_FILTER,
        };
      }
      if (condition.conditions.some(isAlwaysTrueCondition)) {
        return NO_FILTER_APPROXIMATIONS;
      }

      const nonFalseConditions = condition.conditions.filter(
        condition => !isAlwaysFalseCondition(condition),
      );
      if (nonFalseConditions.length === 0) {
        return {
          optimistic: ALWAYS_FALSE_FILTER,
          pessimistic: ALWAYS_FALSE_FILTER,
        };
      }

      const parts = nonFalseConditions.map(getFilterApproximations);
      const optimistic = combineApproximationBranch('or', parts, 'optimistic');

      // If any branch loses information after correlated subquery removal,
      // the OR can no longer be conservatively approximated by its survivors.
      // In that case we keep an optimistic estimate for signal, but drop the
      // whole OR from the pessimistic estimate.
      const pessimistic = parts.some(p => p.pessimistic === NO_SUBQUERY_FILTER)
        ? NO_SUBQUERY_FILTER
        : combineApproximationBranch('or', parts, 'pessimistic');

      return {optimistic, pessimistic};
    }
  }
}

function isAlwaysFalseCondition(condition: Condition): boolean {
  return condition.type === 'or' && condition.conditions.length === 0;
}

function isAlwaysTrueCondition(condition: Condition): boolean {
  return condition.type === 'and' && condition.conditions.length === 0;
}

export function removeCorrelatedSubqueries(
  condition: Condition,
): NoSubqueryCondition | undefined {
  return getFilterApproximations(condition).pessimistic;
}

function combineApproximationBranch(
  type: 'and' | 'or',
  parts: FilterApproximations[],
  key: keyof FilterApproximations,
): NoSubqueryCondition | undefined {
  return combineConditions(
    type,
    parts
      .map(part => part[key])
      .filter(
        (condition): condition is NoSubqueryCondition =>
          condition !== undefined,
      ),
  );
}

function combineConditions(
  type: 'and' | 'or',
  conditions: NoSubqueryCondition[],
): NoSubqueryCondition | undefined {
  if (conditions.length === 0) return NO_SUBQUERY_FILTER;
  if (conditions.length === 1) return conditions[0];
  return {type, conditions};
}

function sameNoSubqueryCondition(
  a: NoSubqueryCondition | undefined,
  b: NoSubqueryCondition | undefined,
): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function blendApproximateCosts(
  optimistic: CostModelCost,
  pessimistic: CostModelCost,
): CostModelCost {
  return {
    // Blend in log space, weighted toward the pessimistic estimate.
    // This preserves simple-filter signal without allowing mixed ORs to
    // collapse to implausibly tiny row counts.
    rows: blendApproximateRows(optimistic.rows, pessimistic.rows),
    startupCost: Math.max(optimistic.startupCost, pessimistic.startupCost),
    fanout: optimistic.fanout,
  };
}

function blendApproximateRows(
  optimisticRows: number,
  pessimisticRows: number,
): number {
  const optimisticLogRows = Math.log(Math.max(optimisticRows, 1));
  const pessimisticLogRows = Math.log(Math.max(pessimisticRows, 1));
  const totalWeight = OPTIMISTIC_BLEND_WEIGHT + PESSIMISTIC_BLEND_WEIGHT;

  return Math.exp(
    (optimisticLogRows * OPTIMISTIC_BLEND_WEIGHT +
      pessimisticLogRows * PESSIMISTIC_BLEND_WEIGHT) /
      totalWeight,
  );
}

/**
 * Gets scanstatus loop information from a prepared statement.
 * Iterates through all query elements and extracts loop statistics.
 *
 * Uses SQLITE_SCANSTAT_COMPLEX flag (1) to get all loops including sorting operations.
 *
 * @param stmt Prepared statement to get scanstatus from
 * @returns Array of loop information, or empty array if scanstatus unavailable
 */
function getScanstatusLoops(stmt: Statement): ScanstatusLoop[] {
  const loops: ScanstatusLoop[] = [];

  // Iterate through query elements by incrementing idx until we get undefined
  // which indicates we've reached the end
  for (let idx = 0; ; idx++) {
    const selectId = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_SELECTID,
      1,
    );

    if (selectId === undefined) {
      break;
    }

    loops.push({
      selectId: must(selectId),
      parentId: must(
        stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_PARENTID, 1),
      ),
      explain: must(
        stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EXPLAIN, 1),
      ),
      est: must(stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EST, 1)),
    });
  }

  return loops.sort((a, b) => a.selectId - b.selectId);
}

/**
 * Estimates the cost of a query based on scanstats from sqlite3_stmt_scanstatus_v2
 */
function estimateCost(
  scanstats: ScanstatusLoop[],
  fanout: CostModelCost['fanout'],
): CostModelCost {
  // Sort by selectId to process in execution order
  const sorted = scanstats.toSorted((a, b) => a.selectId - b.selectId);

  let totalRows = 0;
  let totalCost = 0;

  // Identify if there are multiple top-level (parentId=0) operations
  // If so, the first is typically the scan, and subsequent ones are sorts
  const topLevelOps = sorted.filter(s => s.parentId === 0);

  // We only consider top level ops since ZQL queries are single-table when hitting SQLite.
  // We do have a nested op in the case of `WHERE x IN (:arg)` but it is negligible
  // assuming :arg is small.
  let firstLoop = true;
  for (const op of topLevelOps) {
    if (firstLoop) {
      // First top-level op is the main scan
      // and determines the total number of rows output.
      totalRows = op.est;
      firstLoop = false;
    } else {
      if (op.explain.includes('ORDER BY')) {
        totalCost += btreeCost(totalRows);
      }
    }
  }

  return {
    rows: totalRows,
    startupCost: totalCost,
    fanout,
  };
}

export function btreeCost(rows: number): number {
  // B-Tree construction is ~O(n log n) so we estimate the cost as such.
  // We divide the cost by 10 because sorting in SQLite is ~10x faster
  // than bringing the data into JS and sorting there.
  return (rows * Math.log2(rows)) / 10;
}
