import type {Condition, Ordering} from '../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from '../../zql/src/planner/planner-connection.ts';
import type {PlannerConstraint} from '../../zql/src/planner/planner-constraint.ts';
import SQLite3Database from '@rocicorp/zero-sqlite3';
import {buildSelectQuery, type NoSubqueryCondition} from './query-builder.ts';
import type {Database, Statement} from './db.ts';
import {compile} from './internal/sql.ts';
import {assert} from '../../shared/src/asserts.ts';

/**
 * Loop information returned by SQLite's scanstatus API.
 */
interface ScanstatusLoop {
  /** Unique identifier for this loop */
  selectid: number;
  /** Parent loop ID, or -1 for root loops */
  parentid: number;
  /** Estimated rows emitted per turn of parent loop */
  est: number;
}

/**
 * Internal node structure for building loop hierarchy.
 */
interface LoopNode {
  loop: ScanstatusLoop;
  children: LoopNode[];
}

/**
 * Cost model configuration
 */
const LOOP_BASE_COST = 1; // Base cost per loop iteration
const ROW_OUTPUT_COST = 1; // Cost per row emitted

/**
 * Creates a SQLite-based cost model for query planning.
 * Uses SQLite's scanstatus API to estimate query costs based on the actual
 * SQLite query planner's analysis.
 *
 * @param db Database instance for preparing statements
 * @param tableName Name of the table being queried
 * @param columns Column definitions for the table
 * @returns ConnectionCostModel function for use with the planner
 */
export function createSQLiteCostModel(
  db: Database,
  tableName: string,
  columns: readonly string[],
): ConnectionCostModel {
  return (
    sort: Ordering,
    filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ): number => {
    // Transform filters to remove correlated subqueries
    // The cost model can't handle correlated subqueries, so we estimate cost
    // without them. This is conservative - actual cost may be higher.
    const noSubqueryFilters = filters
      ? removeCorrelatedSubqueries(filters)
      : undefined;

    // Build the SQL query using the same logic as actual queries
    const query = buildSelectQuery(
      tableName,
      columns,
      constraint,
      noSubqueryFilters,
      sort,
    );

    const sql = compile(query);

    // Prepare statement to get scanstatus information
    const stmt = db.prepare(sql);

    // Get scanstatus loops from the prepared statement
    const loops = getScanstatusLoops(stmt);

    if (loops.length === 0) {
      // No loop information available, return a default cost
      // This could happen for very simple queries or if scanstatus is not available
      return constraint ? 10 : 100;
    }

    // Build hierarchy from loops
    const roots = buildLoopHierarchy(loops);

    // Calculate total cost recursively
    let totalCost = 0;
    for (const root of roots) {
      totalCost += calculateLoopCost(root, 1);
    }

    return Math.max(1, totalCost);
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
 * @param stmt Prepared statement to get scanstatus from
 * @returns Array of loop information, or empty array if scanstatus unavailable
 */
function getScanstatusLoops(stmt: Statement): ScanstatusLoop[] {
  const loops: ScanstatusLoop[] = [];

  // Iterate through query elements by incrementing idx until we get undefined
  // which indicates we've reached the end
  for (let idx = 0; ; idx++) {
    // Try to get SELECTID first - if undefined, we've reached the end
    const selectid = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_SELECTID,
      0,
    );

    if (selectid === undefined) {
      // No more query elements
      break;
    }

    // Get PARENTID and EST for this query element
    const parentid = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_PARENTID,
      0,
    );
    const est = stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EST, 0);

    assert(
      typeof selectid === 'number' &&
        typeof parentid === 'number' &&
        typeof est === 'number',
    );

    loops.push({selectid, parentid, est});
  }

  return loops;
}

/**
 * Builds a hierarchy of loop nodes from flat scanstatus data.
 * Root loops (parentid === -1) are returned as separate trees.
 */
function buildLoopHierarchy(loops: ScanstatusLoop[]): LoopNode[] {
  // Create node map for lookup
  const nodeMap = new Map<number, LoopNode>();
  for (const loop of loops) {
    nodeMap.set(loop.selectid, {loop, children: []});
  }

  // Build parent-child relationships
  const roots: LoopNode[] = [];
  for (const node of nodeMap.values()) {
    if (node.loop.parentid === -1) {
      roots.push(node);
    } else {
      const parent = nodeMap.get(node.loop.parentid);
      if (parent) {
        parent.children.push(node);
      } else {
        // Parent not found - treat as root
        roots.push(node);
      }
    }
  }

  return roots;
}

/**
 * Calculates the total cost of a loop and its children recursively.
 *
 * Cost formula: parent_rows × (LOOP_BASE_COST + ROW_OUTPUT_COST × est)
 *
 * Where:
 * - parent_rows: Total rows from parent loop (1 for root)
 * - est: Estimated rows this loop emits per parent iteration
 * - Children costs are multiplied by this loop's total rows
 * - Sibling costs are summed
 *
 * @param node Loop node to calculate cost for
 * @param parentRows Total rows from parent loop
 * @returns Total cost including all children
 */
function calculateLoopCost(node: LoopNode, parentRows: number): number {
  // Calculate cost for this loop
  const loopCost =
    parentRows * (LOOP_BASE_COST + ROW_OUTPUT_COST * node.loop.est);

  // Total rows this loop produces
  const totalRows = parentRows * node.loop.est;

  // Calculate children costs (they run for each row we produce)
  let childrenCost = 0;
  for (const child of node.children) {
    childrenCost += calculateLoopCost(child, totalRows);
  }

  // Sum this loop's cost with all children
  return loopCost + childrenCost;
}
