import {HyperLogLog, type HyperLogLogJSON} from './hyperloglog.ts';
import {
  CountMinSketch,
  type CountMinSketchJSON,
} from './count-min-sketch.ts';
import {TDigest} from '../../../../../shared/src/tdigest.ts';
import type {JSONValue} from '../../../../../shared/src/bigint-json.ts';

/**
 * Statistics for a single column.
 *
 * Maintains exact counts (rows, nulls) and approximate sketches for:
 * - Distinct value count (HyperLogLog)
 * - Value frequencies (Count-Min Sketch)
 * - Value distribution (T-Digest)
 *
 * Sketches are lazily initialized to save memory when not needed.
 *
 * @example
 * ```typescript
 * const config = {
 *   trackNDV: true,
 *   trackRanges: true,
 *   trackFrequencies: false,
 *   hllPrecision: 14,
 *   tdigestCompression: 100,
 *   cmsEpsilon: 0.01,
 *   cmsDelta: 0.001,
 * };
 *
 * const stats = new ColumnStatistics(config);
 *
 * // Track values
 * stats.update(42);
 * stats.update(100);
 * stats.update(42); // Duplicate
 * stats.update(null);
 *
 * // Query statistics
 * stats.rowCount; // 4
 * stats.nullCount; // 1
 * stats.getDistinctCount(); // ~2 (from HyperLogLog)
 * stats.minValue; // 42
 * stats.maxValue; // 100
 * stats.getPercentile(50); // ~0.5 (median from T-Digest)
 * ```
 */
export class ColumnStatistics {
  // Exact counters
  rowCount = 0;
  nullCount = 0;

  // Min/Max tracking
  minValue: JSONValue | undefined = undefined;
  maxValue: JSONValue | undefined = undefined;

  // Sketches (created on first use)
  private _hll: HyperLogLog | undefined;
  private _tdigest: TDigest | undefined;
  private _cms: CountMinSketch | undefined;

  private readonly config: ColumnStatisticsConfig;

  constructor(config: ColumnStatisticsConfig) {
    this.config = config;
  }

  /**
   * Update statistics with a new value.
   *
   * For INSERT operations, call this method with the column value.
   *
   * @param value - The value to add
   */
  update(value: JSONValue): void {
    this.rowCount++;

    if (value === null) {
      this.nullCount++;
      return; // Don't track null in sketches
    }

    // Update min/max
    this.updateMinMax(value);

    // Update HyperLogLog (always enabled for NDV)
    if (this.config.trackNDV) {
      this.hll.add(value);
    }

    // Update T-Digest (for numeric/timestamp columns)
    if (this.config.trackRanges && this.isNumeric(value)) {
      this.tdigest.add(Number(value));
    }

    // Update Count-Min Sketch (for high-cardinality columns)
    if (this.config.trackFrequencies) {
      this.cms.add(value);
    }
  }

  /**
   * Remove a value from statistics.
   *
   * For DELETE operations, call this method with the column value.
   *
   * Note: HyperLogLog and T-Digest don't support deletion, so distinct count
   * and percentiles may be slightly over-estimated after deletes.
   *
   * @param value - The value to remove
   */
  remove(value: JSONValue): void {
    this.rowCount--;

    if (value === null) {
      this.nullCount--;
      return;
    }

    // Note: Cannot update min/max accurately on delete
    // Would need to track all values or rescan data
    // Accept slight inaccuracy here

    // Note: HyperLogLog and T-Digest don't support deletion
    // We accept slight over-estimation of NDV and percentiles

    // Count-Min Sketch can subtract (but may go negative)
    if (this.config.trackFrequencies) {
      this.cms.add(value, -1);
    }
  }

  /**
   * Get estimated distinct count from HyperLogLog.
   *
   * @returns Estimated number of distinct non-null values
   */
  getDistinctCount(): number {
    if (!this._hll) return 0;
    return this._hll.cardinality();
  }

  /**
   * Get estimated frequency of a specific value from Count-Min Sketch.
   *
   * @param value - The value to query
   * @returns Estimated frequency (may over-estimate)
   */
  getFrequency(value: JSONValue): number {
    if (!this._cms) return 0;
    return this._cms.query(value);
  }

  /**
   * Get cumulative distribution function (CDF) value from T-Digest.
   *
   * Returns the fraction of values ≤ the given value.
   *
   * @param value - The value to query
   * @returns Percentile rank (0-1)
   *
   * @example
   * ```typescript
   * // For query: WHERE age > 18
   * const cdf = stats.getPercentile(18); // 0.25
   * const selectivity = 1 - cdf; // 0.75 (75% of rows match)
   * ```
   */
  getPercentile(value: number): number {
    if (!this._tdigest) return 0;
    return this._tdigest.cdf(value);
  }

  /**
   * Serialize to JSON for persistence.
   *
   * @returns JSON-serializable representation
   */
  toJSON(): ColumnStatisticsJSON {
    return {
      version: 1,
      rowCount: this.rowCount,
      nullCount: this.nullCount,
      distinctCount: this.getDistinctCount(),
      minValue: this.minValue,
      maxValue: this.maxValue,
      hll: this._hll?.toJSON(),
      tdigest: this._tdigest?.toJSON(),
      cms: this._cms?.toJSON(),
    };
  }

  /**
   * Deserialize from JSON.
   *
   * @param json - Previously serialized column statistics
   * @param config - Configuration for the statistics
   * @returns Reconstructed ColumnStatistics instance
   */
  static fromJSON(
    json: ColumnStatisticsJSON,
    config: ColumnStatisticsConfig,
  ): ColumnStatistics {
    if (json.version !== 1) {
      throw new Error(`Unsupported column statistics version: ${json.version}`);
    }

    const stats = new ColumnStatistics(config);
    stats.rowCount = json.rowCount;
    stats.nullCount = json.nullCount;
    stats.minValue = json.minValue;
    stats.maxValue = json.maxValue;

    if (json.hll) {
      stats._hll = HyperLogLog.fromJSON(json.hll as HyperLogLogJSON);
    }
    if (json.tdigest) {
      stats._tdigest = TDigest.fromJSON(json.tdigest as readonly [number, ...number[]]);
    }
    if (json.cms) {
      stats._cms = CountMinSketch.fromJSON(json.cms as CountMinSketchJSON);
    }

    return stats;
  }

  /**
   * Get approximate memory usage in bytes.
   *
   * @returns Total memory usage of all structures
   */
  memoryUsage(): number {
    let total = 32; // Base object overhead

    if (this._hll) {
      total += this._hll.memoryUsage();
    }
    if (this._cms) {
      total += this._cms.memoryUsage();
    }
    if (this._tdigest) {
      // T-Digest: ~40 bytes per centroid
      // Use count() to get number of values (approximate for size estimation)
      total += this._tdigest.count() * 0.1; // Rough estimate: compression/10
    }

    return total;
  }

  // Lazy initialization of sketches

  private get hll(): HyperLogLog {
    if (!this._hll) {
      this._hll = new HyperLogLog(this.config.hllPrecision);
    }
    return this._hll;
  }

  private get tdigest(): TDigest {
    if (!this._tdigest) {
      this._tdigest = new TDigest(this.config.tdigestCompression);
    }
    return this._tdigest;
  }

  private get cms(): CountMinSketch {
    if (!this._cms) {
      this._cms = new CountMinSketch(
        this.config.cmsEpsilon,
        this.config.cmsDelta,
      );
    }
    return this._cms;
  }

  /**
   * Update min/max values.
   */
  private updateMinMax(value: JSONValue): void {
    if (this.minValue === undefined || this.compare(value, this.minValue) < 0) {
      this.minValue = value;
    }
    if (this.maxValue === undefined || this.compare(value, this.maxValue) > 0) {
      this.maxValue = value;
    }
  }

  /**
   * Compare two JSON values for ordering.
   *
   * @returns -1 if a < b, 0 if a === b, 1 if a > b
   */
  private compare(a: JSONValue, b: JSONValue): number {
    // Handle identity
    if (a === b) return 0;

    // Null is smallest
    if (a === null) return -1;
    if (b === null) return 1;

    const typeA = typeof a;
    const typeB = typeof b;

    // Different types: order by type name
    if (typeA !== typeB) {
      return typeA < typeB ? -1 : 1;
    }

    // Same type: use natural ordering
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }

  /**
   * Check if a value is numeric (can be used with T-Digest).
   */
  private isNumeric(value: JSONValue): boolean {
    return typeof value === 'number' || typeof value === 'bigint';
  }
}

/**
 * Configuration for column statistics.
 */
export interface ColumnStatisticsConfig {
  /** Track distinct values with HyperLogLog */
  trackNDV: boolean;

  /** Track value distribution with T-Digest (numeric columns only) */
  trackRanges: boolean;

  /** Track value frequencies with Count-Min Sketch */
  trackFrequencies: boolean;

  /** HyperLogLog precision (4-16, default: 14) */
  hllPrecision: number;

  /** T-Digest compression parameter (default: 100) */
  tdigestCompression: number;

  /** Count-Min Sketch error bound (default: 0.01 for 1%) */
  cmsEpsilon: number;

  /** Count-Min Sketch failure probability (default: 0.001 for 99.9%) */
  cmsDelta: number;
}

/**
 * JSON representation of column statistics.
 */
export interface ColumnStatisticsJSON {
  version: 1;
  rowCount: number;
  nullCount: number;
  distinctCount: number;
  minValue: JSONValue | undefined;
  maxValue: JSONValue | undefined;
  hll?: unknown;
  tdigest?: unknown;
  cms?: unknown;
}
