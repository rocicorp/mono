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

    return estimateCost(loops, db, tableName, filters);
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
      explain: must(
        stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EXPLAIN, 1),
      ),
      est: must(stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_EST, 1)),
    });
  }

  return loops.sort((a, b) => a.selectId - b.selectId);
}

/**
 * Extracts column names that have equality filters applied to them.
 * Only returns columns filtered with '=' or 'IS' operators (positive equality).
 * Skips '!=' and 'IS NOT' as they have inverse selectivity characteristics.
 */
function extractEqualityFilteredColumns(
  condition: Condition | undefined,
): Set<string> {
  const columns = new Set<string>();

  if (!condition) {
    return columns;
  }

  const visit = (cond: Condition) => {
    switch (cond.type) {
      case 'simple':
        // Only track positive equality operations
        if (
          (cond.op === '=' || cond.op === 'IS') &&
          cond.left.type === 'column'
        ) {
          columns.add(cond.left.name);
        }
        break;
      case 'and':
      case 'or':
        for (const c of cond.conditions) {
          visit(c);
        }
        break;
      case 'correlatedSubquery':
        // Skip correlated subqueries - they're not in our scanstatus anyway
        break;
    }
  };

  visit(condition);
  return columns;
}

/**
 * Gets all indexed column names for a table (first column of each index).
 * This includes primary keys, unique constraints, and regular indexes.
 * Only the first column is tracked because equality lookups can only use
 * the leading column of an index.
 */
function getIndexedColumns(db: Database, tableName: string): Set<string> {
  const indexed = new Set<string>();

  try {
    // Query all indexes for this table
    const indexList = db.pragma<{name: string}>(`index_list('${tableName}')`);

    for (const {name: indexName} of indexList) {
      // Get the first column of each index
      const indexInfo = db.pragma<{seqno: number; name: string}>(
        `index_info('${indexName}')`,
      );

      if (indexInfo.length > 0) {
        // Add the first column of the index (case-insensitive)
        indexed.add(indexInfo[0].name.toLowerCase());
      }
    }
  } catch {
    // If pragma fails, return empty set (conservative: no corrections)
  }

  return indexed;
}

/**
 * Estimates the cost of a query based on scanstats from sqlite3_stmt_scanstatus_v2
 *
 * Applies corrections for SQLite's poor selectivity estimates on unindexed equality filters.
 * SQLite assumes 25% selectivity for equality on unindexed columns, while PostgreSQL
 * assumes 0.5% (50x more selective). This function applies a 50x correction per
 * unindexed equality filter.
 */
function estimateCost(
  scanstats: ScanstatusLoop[],
  db: Database,
  tableName: string,
  filters: Condition | undefined,
): {
  rows: number;
  startupCost: number;
} {
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
      firstLoop = false;
    } else {
      if (op.explain.includes('ORDER BY')) {
        totalCost += btreeCost(totalRows);
      }
    }
  }

  // Apply correction for unindexed equality filters
  // SQLite assumes 25% selectivity (1/4) for equality on unindexed columns
  // PostgreSQL assumes 0.5% selectivity (1/200)
  // We apply a 50x correction factor per unindexed equality filter
  const equalityColumns = extractEqualityFilteredColumns(filters);
  const indexedColumns = getIndexedColumns(db, tableName);

  let unindexedEqualityCount = 0;
  for (const col of equalityColumns) {
    if (!indexedColumns.has(col.toLowerCase())) {
      unindexedEqualityCount++;
    }
  }

  // Apply compound correction: divide by 50^count
  if (unindexedEqualityCount > 0) {
    const correctionFactor = Math.pow(50, unindexedEqualityCount);
    totalRows = Math.max(1, Math.floor(totalRows / correctionFactor));
  }

  return {rows: totalRows, startupCost: totalCost};
}

export function btreeCost(rows: number): number {
  // B-Tree construction is ~O(n log n) so we estimate the cost as such.
  // We divide the cost by 10 because sorting in SQLite is ~10x faster
  // than bringing the data into JS and sorting there.
  return (rows * Math.log2(rows)) / 10;
}
