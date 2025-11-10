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
 * This cost model wraps the standard SQLite cost model and holds a reference
 * to the HLLStatsManager for future integration with cost estimation.
 *
 * Fallback hierarchy:
 * 1. stat4 (histogram from ANALYZE) - highest accuracy
 * 2. stat1 (averages from ANALYZE) - medium accuracy
 * 3. HLL stats (cardinality estimates) - better than defaults
 * 4. Default assumptions (fanout=3) - last resort
 *
 * For now, this is a simple wrapper that will be extended to incorporate
 * HLL-based estimates in the cost calculation logic.
 */
export function createSQLiteHLLCostModel(
  db: Database,
  tableSpecs: Map<string, {zqlSpec: Record<string, SchemaValue>}>,
  hllManager: HLLStatsManager,
): ConnectionCostModel {
  // Create the standard SQLite cost model
  const baseCostModel = createSQLiteCostModel(db, tableSpecs);

  // For now, just return the base model
  // Future: Augment with HLL stats in the fanout calculation
  // TODO: Integrate hllManager into cost estimation
  void hllManager; // Suppress unused variable warning

  return baseCostModel;
}
