import type {Database, Statement} from './db.ts';

/**
 * Statistics for a specific index from sqlite_stat1.
 */
export interface IndexStats {
  /** Name of the index */
  indexName: string;
  /** Total number of rows in the index */
  totalRows: number;
  /**
   * Average rows per distinct value for each column position in the index.
   *
   * For an index on (userId, postId):
   * - avgRowsPerDistinct[0] = average rows per distinct userId (fan-out from users to this table)
   * - avgRowsPerDistinct[1] = average rows per distinct (userId, postId) pair (typically 1)
   */
  avgRowsPerDistinct: number[];
}

/**
 * High-performance cache for SQLite index statistics and join fan-out estimation.
 *
 * This class prepares all SQL statements once and eagerly loads all index metadata
 * and statistics to provide O(1) lookups for join fan-out calculations.
 *
 * Three-level caching strategy:
 * - Level 1: Schema metadata (table indexes, index columns) - invalidated by schemaUpdated()
 * - Level 2: Statistics (stat1 data) - invalidated by statsUpdated() or schemaUpdated()
 * - Level 3: Computed fan-outs - invalidated by statsUpdated() or schemaUpdated()
 *
 * @example
 * const cache = new Stat1Cache(db);
 * const fanOut = cache.getJoinFanOut('posts', ['userId']); // Fast lookup
 *
 * // After running ANALYZE:
 * db.exec('ANALYZE');
 * cache.statsUpdated();
 *
 * // After schema changes (CREATE INDEX, DROP INDEX):
 * db.exec('CREATE INDEX idx_posts_projectId ON posts(projectId)');
 * cache.schemaUpdated();
 */
export class Stat1Cache {
  #db: Database;

  // Prepared statements (prepared once, reused forever)
  #getStatStmt: Statement | undefined;
  #checkStatTableStmt!: Statement; // Assigned in #prepareStatements
  #getAllStatsStmt: Statement | undefined;
  #getAllIndexDefinitionsStmt!: Statement; // Assigned in #prepareStatements

  // Level 1: Schema metadata (invalidated by schemaUpdated() only)
  #indexColumns: Map<string, string[]>; // indexName -> column names
  #tableIndexes: Map<string, Set<string>>; // tableName -> Set of index names

  // Level 2: Statistics (invalidated by statsUpdated() or schemaUpdated())
  #indexStats: Map<string, IndexStats>; // 'tableName:indexName' -> stats
  #statTableExists: boolean;

  // Level 3: Memoized computations (invalidated by statsUpdated() or schemaUpdated())
  #fanOutCache: Map<string, number | undefined>; // 'tableName:col1,col2,...' -> fanOut

  constructor(db: Database) {
    this.#db = db;
    this.#indexColumns = new Map();
    this.#tableIndexes = new Map();
    this.#indexStats = new Map();
    this.#fanOutCache = new Map();
    this.#statTableExists = false;

    this.#prepareStatements();
    this.#loadSchemaMetadata();
    this.#loadStats();
  }

  /**
   * Prepare all SQL statements that will be used for queries.
   * These are prepared once and reused for the lifetime of the cache.
   *
   * Note: We prepare statements for sqlite_master immediately, but defer
   * preparation of sqlite_stat1 statements until we confirm the table exists.
   */
  #prepareStatements(): void {
    // Schema metadata statements (always available)
    this.#getAllIndexDefinitionsStmt = this.#db.prepare(`
      SELECT name, tbl_name, sql
      FROM sqlite_master
      WHERE type = 'index' AND sql IS NOT NULL
    `);

    this.#checkStatTableStmt = this.#db.prepare(`
      SELECT 1
      FROM sqlite_master
      WHERE type = 'table' AND name = 'sqlite_stat1'
    `);

    // Stat1 statements will be prepared lazily when needed
    this.#getStatStmt = undefined;
    this.#getAllStatsStmt = undefined;
  }

  /**
   * Ensure statements for sqlite_stat1 are prepared.
   * Only prepares them if the table exists and they haven't been prepared yet.
   */
  #ensureStatStatementsReady(): void {
    if (!this.#statTableExists) {
      return;
    }

    // Prepare stat1 statements if not already prepared
    if (!this.#getStatStmt) {
      this.#getStatStmt = this.#db.prepare(`
        SELECT stat
        FROM sqlite_stat1
        WHERE tbl = ? AND idx = ?
      `);
    }

    if (!this.#getAllStatsStmt) {
      this.#getAllStatsStmt = this.#db.prepare(`
        SELECT tbl, idx, stat
        FROM sqlite_stat1
      `);
    }
  }

  /**
   * Load all index metadata from sqlite_master.
   * Parses index definitions to extract column names and builds lookup maps.
   */
  #loadSchemaMetadata(): void {
    this.#indexColumns.clear();
    this.#tableIndexes.clear();

    const indexes = this.#getAllIndexDefinitionsStmt.all() as Array<{
      name: string;
      tbl_name: string;
      sql: string;
    }>;

    for (const index of indexes) {
      // Parse columns from CREATE INDEX statement
      const columns = this.#parseIndexColumns(index.sql);
      if (columns) {
        this.#indexColumns.set(index.name, columns);
      }

      // Track which indexes belong to each table
      let tableSet = this.#tableIndexes.get(index.tbl_name);
      if (!tableSet) {
        tableSet = new Set();
        this.#tableIndexes.set(index.tbl_name, tableSet);
      }
      tableSet.add(index.name);
    }
  }

  /**
   * Load all statistics from sqlite_stat1.
   * Populates the stats cache for fast lookups.
   */
  #loadStats(): void {
    this.#indexStats.clear();

    // Check if stat1 table exists
    const statTableExists = this.#checkStatTableStmt.get() as
      | {1: number}
      | undefined;
    this.#statTableExists = statTableExists !== undefined;

    if (!this.#statTableExists) {
      return;
    }

    // Prepare stat1 statements if needed
    this.#ensureStatStatementsReady();

    // Statement is guaranteed to be prepared at this point since stat table exists
    if (!this.#getAllStatsStmt) {
      return;
    }

    const stats = this.#getAllStatsStmt.all() as Array<{
      tbl: string;
      idx: string;
      stat: string;
    }>;

    for (const {tbl, idx, stat} of stats) {
      const parts = stat.split(' ').map(n => parseInt(n, 10));

      if (parts.length >= 2 && !parts.some(isNaN)) {
        const key = `${tbl}:${idx}`;
        this.#indexStats.set(key, {
          indexName: idx,
          totalRows: parts[0],
          avgRowsPerDistinct: parts.slice(1),
        });
      }
    }
  }

  /**
   * Parse column names from a CREATE INDEX SQL statement.
   *
   * @param sql CREATE INDEX statement
   * @returns Array of column names, or undefined if parsing fails
   */
  #parseIndexColumns(sql: string): string[] | undefined {
    // Format: CREATE INDEX name ON table(col1, col2 DESC, ...)
    const match = sql.match(/\((.*)\)/);
    if (!match) {
      return undefined;
    }

    // Split on comma and extract just the column name (removing ASC/DESC/COLLATE)
    return match[1].split(',').map(col => col.trim().split(/\s+/)[0]);
  }

  /**
   * Check if targetColumns appears as a prefix of indexColumns.
   *
   * @param indexColumns Columns in the index, in order
   * @param targetColumns Target columns to match
   * @returns true if targetColumns is a prefix (in order) of indexColumns
   */
  #isPrefixMatch(indexColumns: string[], targetColumns: string[]): boolean {
    if (indexColumns.length < targetColumns.length) {
      return false;
    }

    return targetColumns.every((col, i) => indexColumns[i] === col);
  }

  /**
   * Compute the join fan-out factor for a multi-column join.
   * This is the core logic that uses cached metadata to determine fan-out.
   *
   * @param tableName Table name
   * @param joinColumns Join columns in order
   * @returns Fan-out factor, or undefined if no statistics available
   */
  #computeJoinFanOut(
    tableName: string,
    joinColumns: string[],
  ): number | undefined {
    if (joinColumns.length === 0) {
      return undefined;
    }

    const tableIndexes = this.#tableIndexes.get(tableName);
    if (!tableIndexes || tableIndexes.size === 0) {
      return undefined;
    }

    // Phase 1: Look for compound indexes where joinColumns is a prefix
    for (const indexName of tableIndexes) {
      const indexColumns = this.#indexColumns.get(indexName);
      if (!indexColumns) {
        continue;
      }

      if (this.#isPrefixMatch(indexColumns, joinColumns)) {
        const statsKey = `${tableName}:${indexName}`;
        const stats = this.#indexStats.get(statsKey);
        if (!stats) {
          continue;
        }

        // The fan-out for the compound key is at position (joinColumns.length - 1)
        // For index (userId, projectId) with stat "10000 100 1":
        // - stats.avgRowsPerDistinct[0] = 100 (avg rows per userId)
        // - stats.avgRowsPerDistinct[1] = 1 (avg rows per (userId, projectId) pair)
        // If joining on both columns, we want the second value
        const fanOutIndex = joinColumns.length - 1;
        const fanOut = stats.avgRowsPerDistinct[fanOutIndex];

        if (fanOut !== undefined && fanOut > 0) {
          return fanOut;
        }
      }
    }

    // Phase 2: No compound index found, fall back to most selective single column
    const singleColumnFanOuts: number[] = [];

    for (const col of joinColumns) {
      for (const indexName of tableIndexes) {
        const indexColumns = this.#indexColumns.get(indexName);
        if (!indexColumns || indexColumns[0] !== col) {
          // Only consider indexes where this column is the first (leftmost)
          continue;
        }

        const statsKey = `${tableName}:${indexName}`;
        const stats = this.#indexStats.get(statsKey);
        if (!stats) {
          continue;
        }

        const fanOut = stats.avgRowsPerDistinct[0];
        if (fanOut !== undefined && fanOut > 0) {
          singleColumnFanOuts.push(fanOut);
          break; // Found stats for this column, move to next column
        }
      }
    }

    if (singleColumnFanOuts.length === 0) {
      return undefined;
    }

    // Return the minimum (most selective) fan-out
    return Math.min(...singleColumnFanOuts);
  }

  /**
   * Refresh all metadata and statistics after schema changes.
   *
   * Call this after:
   * - CREATE TABLE
   * - DROP TABLE
   * - CREATE INDEX
   * - DROP INDEX
   * - ALTER TABLE (adding/removing columns)
   *
   * This clears all caches and reloads everything from sqlite_master and sqlite_stat1.
   */
  schemaUpdated(): void {
    this.#indexColumns.clear();
    this.#tableIndexes.clear();
    this.#indexStats.clear();
    this.#fanOutCache.clear();
    this.#loadSchemaMetadata();
    this.#loadStats();
  }

  /**
   * Refresh statistics after running ANALYZE.
   *
   * Call this after:
   * - Running ANALYZE
   * - Significant data changes that would affect statistics
   *
   * This clears stats and computed fan-outs but preserves schema metadata.
   */
  statsUpdated(): void {
    this.#indexStats.clear();
    this.#fanOutCache.clear();
    this.#loadStats();
  }

  /**
   * Get the join fan-out factor for a multi-column join.
   *
   * This estimates the average number of child rows per parent row when joining
   * on the specified columns. Uses sqlite_stat1 statistics to determine this.
   *
   * Strategy:
   * 1. Look for compound indexes where joinColumns is a prefix (including supersets)
   * 2. If no compound match, find single-column indexes and return minimum fan-out
   * 3. Return undefined if no statistics available
   *
   * Results are memoized for repeated calls with the same parameters.
   *
   * @param tableName The "many" side table of the relationship
   * @param joinColumns The columns used in the join, in order of preference
   * @returns Average fan-out factor, or undefined if no statistics available
   *
   * @example
   * // Joining users -> posts on (userId, projectId)
   * // Index exists on posts(userId, projectId) with stat "10000 100 1"
   * const fanOut = cache.getJoinFanOut('posts', ['userId', 'projectId']);
   * // fanOut = 1 (exactly one post per user+project combination)
   *
   * @example
   * // Joining on (userId, projectId) but only single-column indexes exist
   * // Index on userId has fan-out 100, index on projectId has fan-out 50
   * const fanOut = cache.getJoinFanOut('posts', ['userId', 'projectId']);
   * // fanOut = 50 (most selective single column)
   *
   * @example
   * // No indexes exist
   * const fanOut = cache.getJoinFanOut('posts', ['userId']);
   * // fanOut = undefined (caller should use default like 3)
   */
  getJoinFanOut(tableName: string, joinColumns: string[]): number | undefined {
    // Check memoization cache
    const cacheKey = `${tableName}:${joinColumns.join(',')}`;
    if (this.#fanOutCache.has(cacheKey)) {
      return this.#fanOutCache.get(cacheKey);
    }

    // Compute using cached metadata
    const result = this.#computeJoinFanOut(tableName, joinColumns);

    // Cache and return
    this.#fanOutCache.set(cacheKey, result);
    return result;
  }
}
