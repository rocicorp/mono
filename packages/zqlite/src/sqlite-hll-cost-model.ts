/**
 * SQLite cost model augmented with HyperLogLog statistics.
 *
 * This cost model extends the standard SQLite cost model by incorporating
 * HyperLogLog-based cardinality estimates for columns that don't have indexes.
 *
 * The augmented model provides better cost estimates for:
 * - Non-indexed columns (where stat4/stat1 have no data)
 * - Junction tables with filters on non-indexed columns
 * - Complex joins with selectivity on non-indexed attributes
 */

import type {Database} from './db.ts';
import type {SchemaValue} from '../../zero-types/src/schema-value.ts';
import type {ConnectionCostModel} from '../../zql/src/planner/planner-connection.ts';
import {createSQLiteCostModel} from './sqlite-cost-model.ts';
import {
  HLLStatsManager,
  type Row,
} from '../../zql/src/planner/stats/hll-stats-manager.ts';
import {
  calculateSelectivity,
  calculateConstraintSelectivity,
} from './selectivity-calculator.ts';

/**
 * Build HyperLogLog statistics by scanning all tables in the database.
 *
 * This function:
 * 1. Discovers all user tables (excluding sqlite internal tables)
 * 2. Scans every row in every table
 * 3. Populates HLL sketches for each column
 *
 * The resulting HLLStatsManager can be persisted via snapshot() and
 * restored later to avoid rescanning the database.
 */
export function buildHLLStats(db: Database): HLLStatsManager {
  const manager = new HLLStatsManager();

  // Get all user tables (exclude sqlite internal tables)
  const tables = db
    .prepare(
      `
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name NOT LIKE 'sqlite_%'
  `,
    )
    .all() as {name: string}[];

  // Scan each table and populate HLL stats
  for (const {name: tableName} of tables) {
    // Get all rows from the table
    const rows = db.prepare(`SELECT * FROM "${tableName}"`).all() as Row[];

    // Add each row to HLL manager
    for (const row of rows) {
      manager.onAdd(tableName, row);
    }
  }

  return manager;
}

/**
 * Create a SQLite cost model augmented with HyperLogLog statistics.
 *
 * This cost model wraps the standard SQLite cost model and uses HLL-based
 * selectivity estimation to calculate row counts, following PostgreSQL patterns.
 *
 * Row estimation:
 * 1. Get base table row count from HLLStatsManager
 * 2. Calculate selectivity from filter conditions using HLL cardinality
 * 3. Estimate rows: baseRowCount * selectivity
 *
 * This approach provides better estimates for non-indexed columns where
 * SQLite's scanstatus has no statistical information.
 *
 * @param db SQLite database instance
 * @param tableSpecs Table specifications with ZQL schemas
 * @param hllManager HyperLogLog statistics manager
 * @returns ConnectionCostModel function with HLL-based row estimation
 */
export function createSQLiteHLLCostModel(
  db: Database,
  tableSpecs: Map<string, {zqlSpec: Record<string, SchemaValue>}>,
  hllManager: HLLStatsManager,
): ConnectionCostModel {
  // Create the standard SQLite cost model (still used for sort costs)
  const baseCostModel = createSQLiteCostModel(db, tableSpecs);

  // Return augmented cost model with HLL-based row estimation
  return (tableName, sort, filters, constraint) => {
    // Get base cost estimate from SQLite (includes sort costs)
    const baseCost = baseCostModel(tableName, sort, filters, constraint);

    // Calculate HLL-based row estimate
    const baseRowCount = hllManager.getRowCount(tableName);

    // If table has no rows, return zero rows
    if (baseRowCount === 0) {
      return {
        ...baseCost,
        rows: 0,
      };
    }

    // Calculate HLL-based selectivity
    const filterSelectivity = filters
      ? calculateSelectivity(filters, tableName, hllManager)
      : 1.0;

    const constraintSelectivity = calculateConstraintSelectivity(
      constraint,
      tableName,
      hllManager,
    );

    const hllSelectivity = filterSelectivity * constraintSelectivity;
    const hllEstimatedRows = Math.max(
      1,
      Math.round(baseRowCount * hllSelectivity),
    );

    // Hybrid approach: Use SQLite's estimate if it shows good selectivity,
    // otherwise use HLL estimate
    const sqliteSelectivity = baseCost.rows / baseRowCount;

    // If SQLite has good selectivity info (< 90% of rows), use it
    // Otherwise, use HLL estimate
    const estimatedRows =
      sqliteSelectivity < 0.9 ? baseCost.rows : hllEstimatedRows;

    // Return cost with chosen row estimate
    return {
      ...baseCost,
      rows: estimatedRows,
    };
  };
}
