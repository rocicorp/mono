import {HyperLogLog, type HyperLogLogJSON} from './hyperloglog.ts';

/**
 * A row is a record of column names to values.
 */
export type Row = Record<string, Value>;
export type Value = string | number | boolean | null | undefined;

/**
 * Confidence level for cardinality estimates.
 */
export type Confidence = 'high' | 'med' | 'none';

/**
 * Result of a cardinality query.
 */
export interface CardinalityResult {
  /** Estimated number of distinct values */
  cardinality: number;
  /** Confidence level based on sample size */
  confidence: Confidence;
}

/**
 * Result of a fanout query.
 */
export interface FanoutResult {
  /** Average number of rows per distinct value */
  fanout: number;
  /** Confidence level based on sample size */
  confidence: Confidence;
}

/**
 * Snapshot of all HyperLogLog stats for persistence.
 */
export interface HLLSnapshot {
  version: number;
  /** Map of "table:column" -> HyperLogLog JSON */
  sketches: Record<string, HyperLogLogJSON>;
  /** Map of table name -> total row count */
  rowCounts: Record<string, number>;
}

/**
 * Data source for rebuilding stats from scratch.
 * Returns an iterable of (table, row) pairs.
 */
export type DataSource = Iterable<{table: string; row: Row}>;

const SNAPSHOT_VERSION = 1;

// Confidence thresholds
const HIGH_CONFIDENCE_THRESHOLD = 1000;
const MED_CONFIDENCE_THRESHOLD = 100;

/**
 * Manages HyperLogLog-based cardinality statistics for all tables and columns.
 *
 * This class maintains cardinality estimates for every column of every table
 * and can be updated incrementally as rows are added, removed, or edited.
 *
 * Features:
 * - Streaming updates (add/remove/edit)
 * - Per-column cardinality estimates
 * - Fanout calculation (rows per distinct value)
 * - Snapshot/restore for persistence
 * - Full rebuild from data source
 *
 * Limitations:
 * - HyperLogLog cannot remove values, so deletions only update row counts
 * - Periodic rebuilds are needed to maintain accuracy after many deletions
 */
export class HLLStatsManager {
  /**
   * Map of "table:column" -> HyperLogLog sketch
   */
  readonly #sketches = new Map<string, HyperLogLog>();

  /**
   * Map of table -> total row count
   */
  readonly #rowCounts = new Map<string, number>();

  /**
   * Track number of deletions per table to know when to trigger rebuild
   */
  readonly #deletionCounts = new Map<string, number>();

  constructor() {}

  /**
   * Handle a row addition.
   * Updates all column sketches for the table and increments row count.
   */
  onAdd(table: string, row: Row): void {
    // Increment row count
    this.#rowCounts.set(table, (this.#rowCounts.get(table) ?? 0) + 1);

    // Update sketches for all columns
    for (const [column, value] of Object.entries(row)) {
      this.#getOrCreateSketch(table, column).add(value);
    }
  }

  /**
   * Handle a row removal.
   * Decrements row count but cannot update sketches (HyperLogLog limitation).
   * Tracks deletion count for rebuild threshold detection.
   */
  onRemove(table: string, _row: Row): void {
    // Decrement row count
    const currentCount = this.#rowCounts.get(table) ?? 0;
    this.#rowCounts.set(table, Math.max(0, currentCount - 1));

    // Track deletion for rebuild threshold
    this.#deletionCounts.set(table, (this.#deletionCounts.get(table) ?? 0) + 1);
  }

  /**
   * Handle a row edit.
   * Only updates sketches for columns that changed value.
   */
  onEdit(table: string, oldRow: Row, newRow: Row): void {
    // Row count doesn't change on edit

    // Find columns that changed
    const allColumns = new Set([
      ...Object.keys(oldRow),
      ...Object.keys(newRow),
    ]);

    for (const column of allColumns) {
      const oldValue = oldRow[column];
      const newValue = newRow[column];

      // Only update if value actually changed
      if (oldValue !== newValue) {
        // Add new value to sketch
        // Note: We can't remove the old value due to HyperLogLog limitations
        this.#getOrCreateSketch(table, column).add(newValue);
      }
    }
  }

  /**
   * Get cardinality estimate for a specific column.
   * Returns the estimated number of distinct values.
   */
  getCardinality(table: string, column: string): CardinalityResult {
    const key = this.#makeKey(table, column);
    const sketch = this.#sketches.get(key);

    if (!sketch) {
      return {cardinality: 0, confidence: 'none'};
    }

    const cardinality = sketch.count();
    const confidence = this.#getConfidence(cardinality);

    return {cardinality, confidence};
  }

  /**
   * Get fanout estimate for a specific column.
   * Fanout = average number of rows per distinct value.
   */
  getFanout(table: string, column: string): FanoutResult {
    const rowCount = this.#rowCounts.get(table) ?? 0;
    const {cardinality, confidence} = this.getCardinality(table, column);

    if (cardinality === 0 || rowCount === 0) {
      return {fanout: 1, confidence: 'none'};
    }

    const fanout = rowCount / cardinality;
    return {fanout, confidence};
  }

  /**
   * Get total row count for a table.
   */
  getRowCount(table: string): number {
    return this.#rowCounts.get(table) ?? 0;
  }

  /**
   * Get deletion ratio for a table (deletions / total rows).
   * Used to determine if a rebuild is needed.
   */
  getDeletionRatio(table: string): number {
    const deletions = this.#deletionCounts.get(table) ?? 0;
    const rows = this.#rowCounts.get(table) ?? 0;

    if (rows === 0 && deletions > 0) {
      return 1; // All rows deleted
    }

    return deletions / (rows + deletions);
  }

  /**
   * Check if a table should be rebuilt based on deletion ratio.
   */
  shouldRebuild(table: string, threshold = 0.2): boolean {
    return this.getDeletionRatio(table) > threshold;
  }

  /**
   * Export snapshot of all statistics for persistence.
   */
  snapshot(): HLLSnapshot {
    const sketches: Record<string, HyperLogLogJSON> = {};
    const rowCounts: Record<string, number> = {};

    // Export all sketches
    for (const [key, sketch] of this.#sketches) {
      sketches[key] = sketch.toJSON();
    }

    // Export all row counts
    for (const [table, count] of this.#rowCounts) {
      rowCounts[table] = count;
    }

    return {
      version: SNAPSHOT_VERSION,
      sketches,
      rowCounts,
    };
  }

  /**
   * Restore statistics from a snapshot.
   * Clears existing stats before restoring.
   */
  restore(snapshot: HLLSnapshot): void {
    if (snapshot.version !== SNAPSHOT_VERSION) {
      throw new Error(
        `Unsupported snapshot version: ${snapshot.version} (expected ${SNAPSHOT_VERSION})`,
      );
    }

    // Clear existing state
    this.#sketches.clear();
    this.#rowCounts.clear();
    this.#deletionCounts.clear();

    // Restore sketches
    for (const [key, sketchJSON] of Object.entries(snapshot.sketches)) {
      this.#sketches.set(key, HyperLogLog.fromJSON(sketchJSON));
    }

    // Restore row counts
    for (const [table, count] of Object.entries(snapshot.rowCounts)) {
      this.#rowCounts.set(table, count);
    }
  }

  /**
   * Rebuild statistics from scratch using a data source.
   * This is needed periodically after many deletions/edits to maintain accuracy.
   */
  rebuild(dataSource: DataSource): void {
    // Clear all existing stats
    this.#sketches.clear();
    this.#rowCounts.clear();
    this.#deletionCounts.clear();

    // Process all rows
    for (const {table, row} of dataSource) {
      this.onAdd(table, row);
    }
  }

  /**
   * Rebuild statistics for a specific table only.
   */
  rebuildTable(table: string, dataSource: Iterable<Row>): void {
    // Clear stats for this table
    this.#rowCounts.delete(table);
    this.#deletionCounts.delete(table);

    // Remove all sketches for this table
    for (const key of this.#sketches.keys()) {
      if (key.startsWith(`${table}:`)) {
        this.#sketches.delete(key);
      }
    }

    // Rebuild from data source
    for (const row of dataSource) {
      this.onAdd(table, row);
    }
  }

  /**
   * Get all tables currently tracked.
   */
  getTables(): string[] {
    return Array.from(this.#rowCounts.keys());
  }

  /**
   * Get all columns tracked for a specific table.
   */
  getColumns(table: string): string[] {
    const prefix = `${table}:`;
    const columns: string[] = [];

    for (const key of this.#sketches.keys()) {
      if (key.startsWith(prefix)) {
        columns.push(key.slice(prefix.length));
      }
    }

    return columns;
  }

  /**
   * Clear all statistics.
   */
  clear(): void {
    this.#sketches.clear();
    this.#rowCounts.clear();
    this.#deletionCounts.clear();
  }

  /**
   * Get or create a sketch for a table-column pair.
   */
  #getOrCreateSketch(table: string, column: string): HyperLogLog {
    const key = this.#makeKey(table, column);
    let sketch = this.#sketches.get(key);

    if (!sketch) {
      sketch = new HyperLogLog();
      this.#sketches.set(key, sketch);
    }

    return sketch;
  }

  /**
   * Make a key for the sketches map.
   */
  #makeKey(table: string, column: string): string {
    return `${table}:${column}`;
  }

  /**
   * Determine confidence level based on cardinality.
   */
  #getConfidence(cardinality: number): Confidence {
    if (cardinality >= HIGH_CONFIDENCE_THRESHOLD) {
      return 'high';
    } else if (cardinality >= MED_CONFIDENCE_THRESHOLD) {
      return 'med';
    } else {
      return 'none';
    }
  }
}
