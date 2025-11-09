/**
 * Persistent storage for column statistics.
 *
 * Stores HyperLogLog, Count-Min Sketch, and T-Digest sketches in SQLite
 * for recovery across restarts.
 *
 * Follows the ColumnMetadataStore pattern: uses prepared statements cached
 * in a WeakMap for performance.
 */

import type {Database, Statement} from '../../../../../zqlite/src/db.js';
import type {LogContext} from '@rocicorp/logger';
import {
  ColumnStatistics,
  type ColumnStatisticsJSON,
  type ColumnStatisticsConfig,
} from './column-statistics.js';

/**
 * SQL for creating the column statistics table.
 *
 * This table stores sketches as JSON TEXT columns for flexibility.
 */
export const CREATE_COLUMN_STATISTICS_TABLE = `
  CREATE TABLE IF NOT EXISTS "_zero.column_statistics" (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,

    row_count INTEGER NOT NULL DEFAULT 0,
    null_count INTEGER NOT NULL DEFAULT 0,
    distinct_count INTEGER NOT NULL DEFAULT 0,

    min_value TEXT,
    max_value TEXT,

    hll_sketch TEXT,
    tdigest_sketch TEXT,
    cms_sketch TEXT,

    last_updated INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,

    PRIMARY KEY (table_name, column_name)
  ) WITHOUT ROWID;

  CREATE INDEX IF NOT EXISTS idx_column_statistics_updated
    ON "_zero.column_statistics"(last_updated);

  CREATE INDEX IF NOT EXISTS idx_column_statistics_table
    ON "_zero.column_statistics"(table_name);
`;

/**
 * Efficient column statistics store with prepared statements.
 *
 * Use `StatisticsStore.getInstance(db)` to get a cached instance.
 */
export class StatisticsStore {
  static #instances = new WeakMap<Database, StatisticsStore>();

  readonly #upsertStmt: Statement;
  readonly #selectStmt: Statement;
  readonly #selectTableStmt: Statement;
  readonly #deleteStmt: Statement;
  readonly #deleteTableStmt: Statement;

  private constructor(db: Database, _lc: LogContext) {

    this.#upsertStmt = db.prepare(`
      INSERT INTO "_zero.column_statistics" (
        table_name, column_name, row_count, null_count, distinct_count,
        min_value, max_value, hll_sketch, tdigest_sketch, cms_sketch,
        last_updated, version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(table_name, column_name) DO UPDATE SET
        row_count = excluded.row_count,
        null_count = excluded.null_count,
        distinct_count = excluded.distinct_count,
        min_value = excluded.min_value,
        max_value = excluded.max_value,
        hll_sketch = excluded.hll_sketch,
        tdigest_sketch = excluded.tdigest_sketch,
        cms_sketch = excluded.cms_sketch,
        last_updated = excluded.last_updated,
        version = excluded.version
    `);

    this.#selectStmt = db.prepare(`
      SELECT * FROM "_zero.column_statistics"
      WHERE table_name = ? AND column_name = ?
    `);

    this.#selectTableStmt = db.prepare(`
      SELECT * FROM "_zero.column_statistics"
      WHERE table_name = ?
    `);

    this.#deleteStmt = db.prepare(`
      DELETE FROM "_zero.column_statistics"
      WHERE table_name = ? AND column_name = ?
    `);

    this.#deleteTableStmt = db.prepare(`
      DELETE FROM "_zero.column_statistics"
      WHERE table_name = ?
    `);
  }

  /**
   * Get a cached StatisticsStore instance for a database.
   *
   * Returns `undefined` if the statistics table doesn't exist yet.
   *
   * @param db - SQLite database
   * @param lc - Log context
   * @returns StatisticsStore instance or undefined
   */
  static getInstance(
    db: Database,
    lc: LogContext,
  ): StatisticsStore | undefined {
    // Check cache first
    const cached = StatisticsStore.#instances.get(db);
    if (cached) return cached;

    // Check if table exists
    const tableExists = db
      .prepare(
        `SELECT 1 FROM sqlite_master
         WHERE type='table' AND name='_zero.column_statistics'`,
      )
      .get();

    if (!tableExists) {
      lc.debug?.('Statistics table does not exist yet');
      return undefined;
    }

    // Create and cache instance
    const store = new StatisticsStore(db, lc);
    StatisticsStore.#instances.set(db, store);
    return store;
  }

  /**
   * Save statistics for a column.
   *
   * Uses UPSERT to handle both insert and update cases.
   *
   * @param tableName - Table name
   * @param columnName - Column name
   * @param stats - Column statistics to save
   */
  save(tableName: string, columnName: string, stats: ColumnStatistics): void {
    const json = stats.toJSON();

    this.#upsertStmt.run(
      tableName,
      columnName,
      json.rowCount,
      json.nullCount,
      json.distinctCount,
      json.minValue !== undefined ? JSON.stringify(json.minValue) : null,
      json.maxValue !== undefined ? JSON.stringify(json.maxValue) : null,
      json.hll ? JSON.stringify(json.hll) : null,
      json.tdigest ? JSON.stringify(json.tdigest) : null,
      json.cms ? JSON.stringify(json.cms) : null,
      Date.now(),
      1,
    );
  }

  /**
   * Load statistics for a column.
   *
   * @param tableName - Table name
   * @param columnName - Column name
   * @returns Column statistics JSON or undefined if not found
   */
  load(
    tableName: string,
    columnName: string,
  ): ColumnStatisticsJSON | undefined {
    const row = this.#selectStmt.get(tableName, columnName) as
      | ColumnStatisticsRow
      | undefined;

    if (!row) return undefined;

    return this.rowToJSON(row);
  }

  /**
   * Load all statistics for a table.
   *
   * @param tableName - Table name
   * @returns Map of column name to statistics JSON
   */
  loadTable(tableName: string): Map<string, ColumnStatisticsJSON> {
    const rows = this.#selectTableStmt.all(tableName) as ColumnStatisticsRow[];

    const result = new Map<string, ColumnStatisticsJSON>();

    for (const row of rows) {
      result.set(row.column_name, this.rowToJSON(row));
    }

    return result;
  }

  /**
   * Load all statistics for all tables.
   *
   * @returns Map of "table.column" to statistics JSON
   */
  loadAll(): Map<string, ColumnStatisticsJSON> {
    const rows = this.#selectTableStmt.all() as ColumnStatisticsRow[];
    const result = new Map<string, ColumnStatisticsJSON>();

    for (const row of rows) {
      const key = `${row.table_name}.${row.column_name}`;
      result.set(key, this.rowToJSON(row));
    }

    return result;
  }

  /**
   * Delete statistics for a column.
   *
   * @param tableName - Table name
   * @param columnName - Column name
   */
  delete(tableName: string, columnName: string): void {
    this.#deleteStmt.run(tableName, columnName);
  }

  /**
   * Delete all statistics for a table.
   *
   * @param tableName - Table name
   */
  deleteTable(tableName: string): void {
    this.#deleteTableStmt.run(tableName);
  }

  /**
   * Reconstruct a ColumnStatistics instance from stored JSON.
   *
   * @param json - Serialized statistics
   * @param config - Configuration for the statistics
   * @returns ColumnStatistics instance
   */
  static reconstruct(
    json: ColumnStatisticsJSON,
    config: ColumnStatisticsConfig,
  ): ColumnStatistics {
    return ColumnStatistics.fromJSON(json, config);
  }

  /**
   * Convert a database row to ColumnStatisticsJSON.
   */
  private rowToJSON(row: ColumnStatisticsRow): ColumnStatisticsJSON {
    return {
      version: 1,
      rowCount: row.row_count,
      nullCount: row.null_count,
      distinctCount: row.distinct_count,
      minValue: row.min_value ? JSON.parse(row.min_value) : undefined,
      maxValue: row.max_value ? JSON.parse(row.max_value) : undefined,
      hll: row.hll_sketch ? JSON.parse(row.hll_sketch) : undefined,
      tdigest: row.tdigest_sketch ? JSON.parse(row.tdigest_sketch) : undefined,
      cms: row.cms_sketch ? JSON.parse(row.cms_sketch) : undefined,
    };
  }
}

/**
 * Database row type for column statistics.
 */
type ColumnStatisticsRow = {
  table_name: string;
  column_name: string;
  row_count: number;
  null_count: number;
  distinct_count: number;
  min_value: string | null;
  max_value: string | null;
  hll_sketch: string | null;
  tdigest_sketch: string | null;
  cms_sketch: string | null;
  last_updated: number;
  version: number;
};
