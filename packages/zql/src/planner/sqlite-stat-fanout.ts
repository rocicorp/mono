import type {Database} from '../../../zqlite/src/db.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

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
   * Cache of fanout results by table and columns.
   * Key format: "tableName:col1,col2,col3" (sorted alphabetically)
   */
  readonly #cache = new Map<string, FanoutResult>();

  /**
   * Prepared statements for querying SQLite statistics tables.
   * Prepared once in constructor for performance.
   */
  readonly #stat4Stmt: ReturnType<Database['prepare']>;
  readonly #stat1Stmt: ReturnType<Database['prepare']>;
  readonly #indexStmt: ReturnType<Database['prepare']>;

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

    // Prepare SQL statements once for reuse across multiple getFanout() calls
    this.#stat4Stmt = this.#db.prepare(`
      SELECT neq, nlt, ndlt, sample
      FROM sqlite_stat4
      WHERE tbl = ? AND idx = ?
      ORDER BY nlt
    `);

    this.#stat1Stmt = this.#db.prepare(`
      SELECT stat
      FROM sqlite_stat1
      WHERE tbl = ? AND idx = ?
    `);

    this.#indexStmt = this.#db.prepare(`
      SELECT name, sql
      FROM sqlite_master
      WHERE type = 'index'
        AND tbl_name = ?
        AND sql IS NOT NULL
    `);
  }

  /**
   * Gets the fanout factor for join column(s).
   *
   * Fanout = average number of child rows per distinct parent key value(s).
   *
   * ## Strategy
   *
   * 1. Try sqlite_stat4 (best): Histogram with separate NULL/non-NULL samples
   * 2. Fallback to sqlite_stat1: Average across all rows (includes NULLs)
   * 3. Fallback to default: When no statistics available
   *
   * ## Compound Indexes
   *
   * For multi-column constraints, finds indexes where ALL columns appear as an
   * exact prefix (in order). Uses the appropriate depth in stat1/stat4.
   *
   * Example:
   * - Constraint: `{customerId: undefined, storeId: undefined}`
   * - Matches index: `(customerId, storeId, date)` at depth 2
   * - Uses stat1 parts[2] or stat4 neq[1] for accurate fanout
   *
   * ## Caching
   *
   * Results are cached per (table, columns) combination. Clear the cache if
   * you run ANALYZE to update statistics.
   *
   * @param tableName Table containing the join column(s)
   * @param constraint PlannerConstraint with one or more columns
   * @returns Fanout result with value and source
   */
  getFanout(tableName: string, constraint: PlannerConstraint): FanoutResult {
    // Extract column names from constraint
    const columns = this.#getConstrainedColumns(constraint);

    // Cache key uses sorted columns for consistency
    const cacheKey = `${tableName}:${[...columns].sort().join(',')}`;
    const cached = this.#cache.get(cacheKey);
    if (cached) {
      return cached;
    }

    // Strategy 1: Try stat4 first (most accurate)
    // NOTE: columns are NOT sorted - preserves Object.keys() order from constraint
    // Matching is order-independent (flexible), but we keep original order for consistency
    const stat4Result = this.#getFanoutFromStat4(tableName, columns);
    if (stat4Result) {
      this.#cache.set(cacheKey, stat4Result);
      return stat4Result;
    }

    // Strategy 2: Fallback to stat1 (includes NULLs)
    const stat1Result = this.#getFanoutFromStat1(tableName, columns);
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
   * Extracts column names from constraint.
   *
   * @param constraint PlannerConstraint object
   * @returns Array of column names (unsorted, preserves Object.keys() order)
   */
  #getConstrainedColumns(constraint: PlannerConstraint): string[] {
    return Object.keys(constraint);
  }

  /**
   * Gets fanout from sqlite_stat4 histogram.
   *
   * Queries stat4 samples, decodes to identify NULLs, and returns
   * the median fanout of non-NULL samples.
   *
   * For compound indexes, uses the neq value at the appropriate depth.
   *
   * @param columns Array of column names to get fanout for
   * @returns Fanout result or undefined if stat4 unavailable
   */
  #getFanoutFromStat4(
    tableName: string,
    columns: string[],
  ): FanoutResult | undefined {
    try {
      // Find index containing the columns as a prefix
      const indexInfo = this.#findIndexForColumns(tableName, columns);
      if (!indexInfo) {
        return undefined;
      }

      // Query stat4 samples for this index (using prepared statement)
      const samples = this.#stat4Stmt.all(
        tableName,
        indexInfo.indexName,
      ) as Stat4Sample[];

      if (samples.length === 0) {
        return undefined;
      }

      // Decode samples and separate NULL from non-NULL
      // Use depth-1 for neq array index (depth is 1-based, array is 0-based)
      const neqIndex = indexInfo.depth - 1;
      const decodedSamples = samples.map(s => {
        const neqParts = s.neq.split(' ');
        return {
          fanout: parseInt(neqParts[neqIndex] ?? neqParts[0], 10),
          isNull: this.#decodeSampleIsNull(s.sample),
        };
      });

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
   * For compound indexes, uses the stat value at the appropriate depth.
   *
   * @param columns Array of column names to get fanout for
   * @returns Fanout result or undefined if stat1 unavailable
   */
  #getFanoutFromStat1(
    tableName: string,
    columns: string[],
  ): FanoutResult | undefined {
    try {
      // Find index containing the columns as a prefix
      const indexInfo = this.#findIndexForColumns(tableName, columns);
      if (!indexInfo) {
        return undefined;
      }

      // Query stat1 for this index (using prepared statement)
      const result = this.#stat1Stmt.get(tableName, indexInfo.indexName) as
        | {stat: string}
        | undefined;

      if (!result) {
        return undefined;
      }

      const parts = result.stat.split(' ');
      // Check if we have enough parts for the requested depth
      if (parts.length < indexInfo.depth + 1) {
        return undefined;
      }

      const fanout = parseInt(parts[indexInfo.depth], 10);
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
   * Finds an index that can be used to get statistics for column(s).
   *
   * Uses flexible matching: Finds indexes where ALL columns appear in the
   * first N positions, regardless of order. This works because SQLite statistics
   * at depth N represent the fanout for the combination of the first N columns,
   * and combinations are order-independent.
   *
   * Example:
   * - columns: ['customerId', 'storeId']
   * - Matches: (customerId, storeId, date) at depth 2 ✅
   * - Matches: (storeId, customerId, date) at depth 2 ✅ (flexible order)
   * - Does NOT match: (date, customerId, storeId) ❌ (columns not in first 2 positions)
   * - Does NOT match: (customerId, date, storeId) ❌ (storeId not in first 2 positions)
   *
   * @param columns Array of column names (order-independent for matching)
   * @returns Index info with name and depth, or undefined if no match
   */
  #findIndexForColumns(
    tableName: string,
    columns: string[],
  ): {indexName: string; depth: number} | undefined {
    try {
      // Query sqlite_master for indexes on this table (using prepared statement)
      const indexes = this.#indexStmt.all(tableName) as {
        name: string;
        sql: string;
      }[];

      // Extract column list from index SQL
      for (const {name, sql} of indexes) {
        const indexColumns = this.#extractIndexColumns(sql);
        if (!indexColumns) continue;

        // Check if our columns form a prefix of the index columns
        if (this.#isPrefixMatch(columns, indexColumns)) {
          return {
            indexName: name,
            depth: columns.length,
          };
        }
      }

      return undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Extracts column names from an index CREATE INDEX SQL statement.
   *
   * Handles various formats:
   * - CREATE INDEX idx ON table(col1, col2)
   * - CREATE INDEX idx ON table("col1", 'col2')
   * - CREATE INDEX idx ON table(`col1`, col2)
   *
   * @param sql Index creation SQL
   * @returns Array of column names in order, or undefined if parse fails
   */
  #extractIndexColumns(sql: string): string[] | undefined {
    // Match pattern: INDEX name ON table(columns)
    const match = sql.match(/\([^)]+\)/i);
    if (!match) return undefined;

    // Extract content between parentheses
    const columnsStr = match[0].slice(1, -1); // Remove ( and )

    // Split by comma and clean up each column name
    // Remove quotes, backticks, and whitespace
    const columns = columnsStr.split(',').map(col =>
      col
        .trim()
        .replace(/^["'`]|["'`]$/g, '')
        .toLowerCase(),
    );

    return columns.filter(col => col.length > 0);
  }

  /**
   * Checks if all queryColumns exist in the first N positions of indexColumns,
   * regardless of order.
   *
   * This allows flexible matching: constraint {a, b} matches both index (a, b, c)
   * and index (b, a, c) at depth 2, since both represent the fanout for the
   * combination of columns a and b.
   *
   * Gaps are NOT allowed: constraint {a, c} does NOT match index (a, b, c)
   * because no depth represents just (a, c) without b. Statistics are cumulative
   * from position 0.
   *
   * @param queryColumns Columns we're looking for (from constraint)
   * @param indexColumns Columns in the index (in order)
   * @returns true if all queryColumns exist in indexColumns[0...queryColumns.length-1]
   */
  #isPrefixMatch(queryColumns: string[], indexColumns: string[]): boolean {
    if (queryColumns.length > indexColumns.length) {
      return false;
    }

    // Get the prefix of the index that we're checking against
    const indexPrefix = indexColumns.slice(0, queryColumns.length);

    // Normalize to lowercase for case-insensitive comparison
    const indexPrefixLower = new Set(indexPrefix.map(col => col.toLowerCase()));
    const queryColumnsLower = queryColumns.map(col => col.toLowerCase());

    // Check if ALL query columns exist in the index prefix
    return queryColumnsLower.every(queryCol => indexPrefixLower.has(queryCol));
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
