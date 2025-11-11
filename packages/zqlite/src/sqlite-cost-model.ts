import type {Condition, Ordering} from '../../zero-protocol/src/ast.ts';
import type {
  ConnectionCostModel,
  CostModelCost,
} from '../../zql/src/planner/planner-connection.ts';
import type {PlannerConstraint} from '../../zql/src/planner/planner-constraint.ts';
import SQLite3Database from '@rocicorp/zero-sqlite3';
import {buildSelectQuery, type NoSubqueryCondition} from './query-builder.ts';
import type {Database, Statement} from './db.ts';
import {compile} from './internal/sql.ts';
import {assert} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {SchemaValue} from '../../zero-types/src/schema-value.ts';
import {SQLiteStatFanout} from './sqlite-stat-fanout.ts';

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
  /** Index name if this loop uses an index, undefined otherwise */
  indexName?: string;
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

    return estimateCost(
      loops,
      (columns: string[]) => fanoutEstimator.getFanout(tableName, columns),
      tableName,
      constraint,
      fanoutEstimator,
    );
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

    const name = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_NAME,
      1,
    ) as string | undefined;

    const parsedIndexName = parseIndexName(name);
    const loop: ScanstatusLoop = {
      selectId: must(selectId),
      parentId: must(
        stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_PARENTID, 1),
      ),
      explain: must(
        stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EXPLAIN, 1),
      ),
      est: must(stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EST, 1)),
    };
    if (parsedIndexName !== undefined) {
      loop.indexName = parsedIndexName;
    }
    loops.push(loop);
  }

  return loops.sort((a, b) => a.selectId - b.selectId);
}

/**
 * Parses index name from scanstatus NAME field.
 * Examples:
 * - "SEARCH TABLE foo USING INDEX bar" -> "bar"
 * - "SEARCH foo USING INDEX bar" -> "bar"
 * - "SCAN TABLE foo" -> undefined
 */
function parseIndexName(name: string | undefined): string | undefined {
  if (!name) return undefined;

  // Match patterns like "USING INDEX index_name" or "USING COVERING INDEX index_name"
  const match = name.match(/USING (?:COVERING )?INDEX (\S+)/i);
  return match ? match[1] : undefined;
}

/**
 * Gets the NULL ratio for constraint columns that appear in an index's prefix.
 *
 * This is used to adjust SQLite's row estimates when NULL values skew the data.
 * We only adjust for equality filters since those exclude NULLs by definition.
 *
 * @param tableName Table being queried
 * @param indexName Index chosen by SQLite
 * @param constraint Constraint with equality filters
 * @param fanoutEstimator Estimator to query stat4
 * @returns NULL ratio (0.0 to 1.0) for the constrained leading columns
 */
function getNullRatioForConstraint(
  tableName: string,
  indexName: string,
  constraint: PlannerConstraint,
  fanoutEstimator: SQLiteStatFanout,
): number {
  // Count how many constraint columns we're filtering on
  // We assume these are the leading columns of the index (since SQLite chose it)
  const constraintColumns = Object.keys(constraint);

  if (constraintColumns.length === 0) {
    return 0;
  }

  // Query NULL ratio for the leading N columns of the index
  // where N = number of equality filters in the constraint
  return fanoutEstimator.getNullRatioForIndex(
    tableName,
    indexName,
    constraintColumns.length,
  );
}

/**
 * Estimates the cost of a query based on scanstats from sqlite3_stmt_scanstatus_v2
 */
function estimateCost(
  scanstats: ScanstatusLoop[],
  fanout: CostModelCost['fanout'],
  tableName: string,
  constraint: PlannerConstraint | undefined,
  fanoutEstimator: SQLiteStatFanout,
): CostModelCost {
  // Sort by selectId to process in execution order
  const sorted = [...scanstats].sort((a, b) => a.selectId - b.selectId);

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

      // Apply NULL-aware adjustment if we have an index and equality constraints
      if (op.indexName && constraint) {
        const nullRatio = getNullRatioForConstraint(
          tableName,
          op.indexName,
          constraint,
          fanoutEstimator,
        );
        if (nullRatio > 0) {
          // Adjust estimate by excluding NULL rows
          // If 75% of rows are NULL, only 25% can match a non-NULL equality filter
          totalRows = Math.ceil(totalRows * (1 - nullRatio));
        }
      }

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
