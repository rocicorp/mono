import type {Database} from '../../../zqlite/src/db.ts';

/**
 * Result of fanout calculation from SQLite statistics.
 */
export interface FanoutResult {
  /**
   * The fanout value (average rows per distinct value of the join column).
   * For non-NULL joins, this represents how many child rows exist per parent key.
   */
  fanout: number;

  /**
   * Source of the fanout calculation.
   * - 'stat4': From sqlite_stat4 histogram (most accurate, excludes NULLs)
   * - 'stat1': From sqlite_stat1 average (includes NULLs, may overestimate)
   * - 'default': Fallback constant when statistics unavailable
   */
  source: 'stat4' | 'stat1' | 'default';

  /**
   * If available, the number of NULL rows found in stat4.
   * Only populated when source is 'stat4'.
   */
  nullCount?: number;
}

/**
 * Sample from sqlite_stat4 histogram.
 */
interface Stat4Sample {
  /** "N1 N2" = rows equal to sample (N1=first col, N2=if composite) */
  neq: string;
  /** "N1 N2" = rows less than sample */
  nlt: string;
  /** "N1 N2" = distinct values less than sample */
  ndlt: string;
  /** The actual sample value (binary encoded) */
  sample: Buffer;
}

/**
 * Computes join fanout factors from SQLite statistics tables.
 *
 * Fanout is the average number of child rows per distinct parent key value,
 * used to estimate join cardinality in query planning.
 *
 * ## Problem
 *
 * sqlite_stat1 includes NULL rows in its calculation, which can significantly
 * overestimate fanout for sparse foreign keys:
 *
 * ```
 * Example: 100 tasks, 20 with project_id, 80 with NULL
 * - stat1 reports: "100 17" → fanout = 17 (WRONG - includes NULLs)
 * - stat4 shows: NULL samples with fanout=80, non-NULL samples with fanout=4
 * - True fanout: 4 (CORRECT)
 * ```
 *
 * ## Solution
 *
 * This class uses sqlite_stat4 histogram to separate NULL and non-NULL samples,
 * providing accurate fanout for non-NULL joins.
 *
 * ## Usage
 *
 * ```typescript
 * const calculator = new SQLiteStatFanout(db);
 *
 * // Get fanout for posts.userId → users.id join
 * const result = calculator.getFanout('posts', 'userId');
 *
 * if (result.source === 'stat4') {
 *   // Accurate: excludes NULLs, samples actual distribution
 *   console.log(`Fanout: ${result.fanout} (from stat4)`);
 * } else if (result.source === 'stat1') {
 *   // Conservative: includes NULLs, may overestimate
 *   console.log(`Fanout: ${result.fanout} (from stat1, includes NULLs)`);
 * } else {
 *   // Fallback: no statistics available
 *   console.log(`Fanout: ${result.fanout} (default estimate)`);
 * }
 * ```
 *
 * ## Requirements
 *
 * - SQLite compiled with ENABLE_STAT4 (most builds include this)
 * - `ANALYZE` command run on the database
 * - Index exists on the join column
 *
 * @see https://sqlite.org/fileformat2.html#stat4tab
 * @see packages/zql/src/planner/SELECTIVITY_PLAN.md
 */
export class SQLiteStatFanout {
  readonly #db: Database;
  readonly #defaultFanout: number;

  /**
   * Cache of fanout results by table and column name.
   * Key format: "tableName:columnName"
   */
  readonly #cache = new Map<string, FanoutResult>();

  /**
   * Creates a new fanout calculator.
   *
   * @param db Database instance
   * @param defaultFanout Default fanout when statistics unavailable (default: 3)
   *                      - 1: Conservative (assumes FK relationships)
   *                      - 3: Moderate (recommended, safe middle ground)
   *                      - 10: SQLite's default (optimistic)
   */
  constructor(db: Database, defaultFanout = 3) {
    this.#db = db;
    this.#defaultFanout = defaultFanout;
  }

  /**
   * Gets the fanout factor for a join column.
   *
   * Fanout = average number of child rows per distinct parent key value.
   *
   * ## Strategy
   *
   * 1. Try sqlite_stat4 (best): Histogram with separate NULL/non-NULL samples
   * 2. Fallback to sqlite_stat1: Average across all rows (includes NULLs)
   * 3. Fallback to default: When no statistics available
   *
   * ## Caching
   *
   * Results are cached per (table, column) pair. Clear the cache if you run
   * ANALYZE to update statistics.
   *
   * @param tableName Table containing the join column
   * @param columnName Column used in the join
   * @returns Fanout result with value and source
   */
  getFanout(tableName: string, columnName: string): FanoutResult {
    const cacheKey = `${tableName}:${columnName}`;
    const cached = this.#cache.get(cacheKey);
    if (cached) {
      return cached;
    }

    // Strategy 1: Try stat4 first (most accurate)
    const stat4Result = this.#getFanoutFromStat4(tableName, columnName);
    if (stat4Result) {
      this.#cache.set(cacheKey, stat4Result);
      return stat4Result;
    }

    // Strategy 2: Fallback to stat1 (includes NULLs)
    const stat1Result = this.#getFanoutFromStat1(tableName, columnName);
    if (stat1Result) {
      this.#cache.set(cacheKey, stat1Result);
      return stat1Result;
    }

    // Strategy 3: Use default
    const defaultResult: FanoutResult = {
      fanout: this.#defaultFanout,
      source: 'default',
    };
    this.#cache.set(cacheKey, defaultResult);
    return defaultResult;
  }

  /**
   * Clears the fanout cache.
   * Call this after running ANALYZE to pick up updated statistics.
   */
  clearCache(): void {
    this.#cache.clear();
  }

  /**
   * Gets fanout from sqlite_stat4 histogram.
   *
   * Queries stat4 samples, decodes to identify NULLs, and returns
   * the median fanout of non-NULL samples.
   *
   * @returns Fanout result or undefined if stat4 unavailable
   */
  #getFanoutFromStat4(
    tableName: string,
    columnName: string,
  ): FanoutResult | undefined {
    try {
      // Find index containing the column
      const indexName = this.#findIndexForColumn(tableName, columnName);
      if (!indexName) {
        return undefined;
      }

      // Query stat4 samples for this index
      const stmt = this.#db.prepare(`
        SELECT neq, nlt, ndlt, sample
        FROM sqlite_stat4
        WHERE tbl = ? AND idx = ?
        ORDER BY nlt
      `);

      const samples = stmt.all(tableName, indexName) as Stat4Sample[];

      if (samples.length === 0) {
        return undefined;
      }

      // Decode samples and separate NULL from non-NULL
      const decodedSamples = samples.map(s => ({
        fanout: parseInt(s.neq.split(' ')[0], 10),
        isNull: this.#decodeSampleIsNull(s.sample),
      }));

      const nullSamples = decodedSamples.filter(s => s.isNull);
      const nonNullSamples = decodedSamples.filter(s => !s.isNull);

      if (nonNullSamples.length === 0) {
        // All samples are NULL - use default
        return undefined;
      }

      // Use median of non-NULL fanouts (more robust than average)
      const fanouts = nonNullSamples.map(s => s.fanout).sort((a, b) => a - b);
      const medianFanout =
        fanouts.length % 2 === 0
          ? Math.floor(
              (fanouts[fanouts.length / 2 - 1] + fanouts[fanouts.length / 2]) /
                2,
            )
          : fanouts[Math.floor(fanouts.length / 2)];

      return {
        fanout: medianFanout,
        source: 'stat4',
        nullCount: nullSamples.length > 0 ? nullSamples[0].fanout : 0,
      };
    } catch {
      // stat4 table may not exist or query may fail
      return undefined;
    }
  }

  /**
   * Gets fanout from sqlite_stat1 average.
   *
   * Note: This includes NULL rows in the calculation and may overestimate
   * fanout for sparse foreign keys.
   *
   * @returns Fanout result or undefined if stat1 unavailable
   */
  #getFanoutFromStat1(
    tableName: string,
    columnName: string,
  ): FanoutResult | undefined {
    try {
      // Find index containing the column
      const indexName = this.#findIndexForColumn(tableName, columnName);
      if (!indexName) {
        return undefined;
      }

      // Query stat1 for this index
      const stmt = this.#db.prepare(`
        SELECT stat
        FROM sqlite_stat1
        WHERE tbl = ? AND idx = ?
      `);

      const result = stmt.get(tableName, indexName) as
        | {stat: string}
        | undefined;

      if (!result) {
        return undefined;
      }

      const parts = result.stat.split(' ');
      if (parts.length < 2) {
        return undefined;
      }

      const fanout = parseInt(parts[1], 10);
      if (isNaN(fanout)) {
        return undefined;
      }

      return {
        fanout,
        source: 'stat1',
      };
    } catch {
      return undefined;
    }
  }

  /**
   * Finds an index that can be used to get statistics for a column.
   *
   * Prefers indexes where the column is the leftmost (first) column,
   * as those have the most accurate statistics.
   *
   * @returns Index name or undefined if no suitable index found
   */
  #findIndexForColumn(
    tableName: string,
    columnName: string,
  ): string | undefined {
    try {
      // Query sqlite_master for indexes on this table
      const stmt = this.#db.prepare(`
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'index'
          AND tbl_name = ?
          AND sql IS NOT NULL
      `);

      const indexes = stmt.all(tableName) as {name: string; sql: string}[];

      // Find indexes containing the column
      // Prefer indexes where column is leftmost (most accurate stats)
      let leftmostIndex: string | undefined;
      let anyIndex: string | undefined;

      for (const {name, sql} of indexes) {
        const sqlLower = sql.toLowerCase();
        const columnLower = columnName.toLowerCase();

        if (sqlLower.includes(columnLower)) {
          anyIndex = name;

          // Check if column is leftmost by looking for pattern: INDEX name ON table(column
          const pattern = new RegExp(
            `\\(\\s*["'\`]?${columnLower}["'\`]?\\s*(?:,|\\))`,
            'i',
          );
          if (pattern.test(sql)) {
            leftmostIndex = name;
            break; // Found leftmost, no need to continue
          }
        }
      }

      return leftmostIndex ?? anyIndex;
    } catch {
      return undefined;
    }
  }

  /**
   * Decodes a sqlite_stat4 sample value to check if it's NULL.
   *
   * SQLite record format (simplified):
   * - Varint: header size
   * - Serial types for each column (one byte each typically)
   * - Actual data
   *
   * Serial type 0 = NULL
   * Serial type 1 = 8-bit int
   * Serial type 2 = 16-bit int
   * Serial type 3 = 24-bit int
   * etc.
   *
   * We only need to check the first column's serial type.
   *
   * @param sample Binary-encoded sample from stat4
   * @returns true if the sample value is NULL
   */
  #decodeSampleIsNull(sample: Buffer): boolean {
    if (sample.length === 0) {
      return true;
    }

    // Read header size (varint - simplified: assume single byte)
    const headerSize = sample[0];

    if (headerSize === 0 || headerSize >= sample.length) {
      return true;
    }

    // Read first serial type (at position 1)
    const serialType = sample[1];

    // Serial type 0 = NULL
    return serialType === 0;
  }
}
