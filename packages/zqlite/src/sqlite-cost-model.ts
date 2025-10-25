import type {Condition, Ordering} from '../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from '../../zql/src/planner/planner-connection.ts';
import type {PlannerConstraint} from '../../zql/src/planner/planner-constraint.ts';
import SQLite3Database from '@rocicorp/zero-sqlite3';
import {buildSelectQuery, type NoSubqueryCondition} from './query-builder.ts';
import type {Database, Statement} from './db.ts';
import {compile} from './internal/sql.ts';
import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {SchemaValue} from '../../zero-types/src/schema-value.ts';

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
  return (
    tableName: string,
    sort: Ordering,
    filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ): {startupCost: number; baseCardinality: number} => {
    // Transform filters to remove correlated subqueries
    // The cost model can't handle correlated subqueries, so we estimate cost
    // without them. This is conservative - actual cost may be higher.
    const noSubqueryFilters = filters
      ? removeCorrelatedSubqueries(filters)
      : undefined;

    // Build the SQL query using the same logic as actual queries
    const {zqlSpec} = must(tableSpecs.get(tableName));

    const query = buildSelectQuery(
      tableName,
      zqlSpec,
      constraint,
      noSubqueryFilters,
      sort,
      undefined, // reverse is undefined here
      undefined, // start is undefined here
    );

    const sql = compile(query);

    // Prepare statement to get scanstatus information
    const stmt = db.prepare(sql);

    // Get scanstatus loops from the prepared statement
    const loops = getScanstatusLoops(stmt);

    // Scanstatus should always be available - if we get no loops, something is wrong
    assert(
      loops.length > 0,
      `Expected scanstatus to return at least one loop for query: ${sql}`,
    );

    const {startupCost, runningCost} = estimateCost(loops);

    return {
      startupCost,
      baseCardinality: Math.max(1, runningCost),
    };
  };
}

/**
 * Removes correlated subqueries from conditions.
 * The cost model estimates cost without correlated subqueries since
 * they can't be included in the scanstatus query.
 */
function removeCorrelatedSubqueries(
  condition: Condition,
): NoSubqueryCondition | undefined {
  switch (condition.type) {
    case 'correlatedSubquery':
      // Remove correlated subqueries - we can't estimate their cost via scanstatus
      return undefined;
    case 'simple':
      return condition;
    case 'and': {
      const filtered = condition.conditions
        .map(c => removeCorrelatedSubqueries(c))
        .filter((c): c is NoSubqueryCondition => c !== undefined);
      if (filtered.length === 0) return undefined;
      if (filtered.length === 1) return filtered[0];
      return {type: 'and', conditions: filtered};
    }
    case 'or': {
      const filtered = condition.conditions
        .map(c => removeCorrelatedSubqueries(c))
        .filter((c): c is NoSubqueryCondition => c !== undefined);
      if (filtered.length === 0) return undefined;
      if (filtered.length === 1) return filtered[0];
      return {type: 'or', conditions: filtered};
    }
  }
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
      est: must(stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EST, 1)),
    });
  }

  return loops.sort((a, b) => a.selectId - b.selectId);
}

/**
 * Estimates the cost of a query based on scanstats from sqlite3_stmt_scanstatus_v2
 *
 * Algorithm:
 * - Siblings (same parentId) are pipeline operations that run once
 * - Children (parentId != 0) are nested loops that run once per parent output row
 * - Cost = estimated rows × loops
 * - Startup cost = one-time costs (e.g., sorting) that don't scale with outer loops
 * - Running cost = costs that scale with the number of iterations
 *
 * SQLite reports sorting operations as separate scan loops. When a query requires
 * sorting (ORDER BY on non-indexed column), there will be multiple loops with the
 * same parentId=0, where the later loop is the sort operation.
 *
 * @param scanstats Array of scan statistics with parentId, selectId, and est (estimated rows)
 * @returns Object with startup cost and running cost
 */
function estimateCost(scanstats: ScanstatusLoop[]): {
  startupCost: number;
  runningCost: number;
} {
  // Sort by selectId to process in execution order
  const sorted = [...scanstats].sort((a, b) => a.selectId - b.selectId);

  // Track the total output rows for each selectId
  // This is used to determine loop counts for child operations
  const outputRows = new Map<number, number>();

  let startupCost = 0;
  let runningCost = 0;

  // Identify if there are multiple top-level (parentId=0) operations
  // If so, the first is typically the scan, and subsequent ones are sorts
  const topLevelOps = sorted.filter(s => s.parentId === 0);
  const hasSortOperation = topLevelOps.length > 1;

  let pipelineRows = 1;
  let lastParentId = -1;
  let isFirstTopLevel = true;

  for (const stat of sorted) {
    let loops: number;

    if (stat.parentId === 0) {
      // Top-level operation (sibling): runs once as part of pipeline
      loops = 1;
    } else {
      // Child operation (nested loop): runs once per parent's output row
      loops = outputRows.get(stat.parentId) || 1;
    }

    // Cost is the number of rows processed/examined
    const cost = stat.est * loops * pipelineRows;

    // Classify cost as startup or running
    if (stat.parentId === 0 && hasSortOperation) {
      // Top-level operation with multiple siblings
      if (isFirstTopLevel) {
        // First top-level operation is the scan (running cost)
        runningCost += cost;
        isFirstTopLevel = false;
      } else {
        // Subsequent top-level operations are sorts (startup cost)
        startupCost += cost;
      }
    } else {
      // All other operations are running costs
      runningCost += cost;
    }

    if (stat.parentId !== lastParentId) {
      // New pipeline operation - reset pipeline row count
      pipelineRows = stat.est;
      lastParentId = stat.parentId;
    }

    // Track this operation's total output (for any children)
    outputRows.set(stat.selectId, cost);
  }

  return {startupCost, runningCost};
}
