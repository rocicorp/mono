import type {Database} from './db.ts';

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
 * Get the column names for an index in order.
 *
 * @param db Database instance
 * @param indexName Name of the index
 * @returns Array of column names in index order, or undefined if index not found
 *
 * @example
 * // For: CREATE INDEX idx ON posts(userId, createdAt DESC)
 * getIndexColumns(db, 'idx') // returns ['userId', 'createdAt']
 */
export function getIndexColumns(
  db: Database,
  indexName: string,
): string[] | undefined {
  const result = db
    .prepare(
      `
      SELECT sql
      FROM sqlite_master
      WHERE type = 'index' AND name = ?
    `,
    )
    .get(indexName) as {sql: string | null} | undefined;

  if (!result?.sql) {
    return undefined;
  }

  // Parse CREATE INDEX statement to extract column names
  // Format: CREATE INDEX name ON table(col1, col2 DESC, ...)
  const match = result.sql.match(/\((.*)\)/);
  if (!match) {
    return undefined;
  }

  // Split on comma and extract just the column name (removing ASC/DESC/COLLATE)
  // Take first word (column name), ignore modifiers like ASC/DESC/COLLATE
  return match[1].split(',').map(col => col.trim().split(/\s+/)[0]);
}

/**
 * Get statistics for a specific index from sqlite_stat1.
 *
 * Note: Requires ANALYZE to have been run on the database.
 *
 * @param db Database instance
 * @param tableName Table name
 * @param indexName Index name
 * @returns Index statistics, or undefined if no stats available
 *
 * @example
 * // For index on (userId) with stat "10000 100"
 * const stats = getIndexStats(db, 'posts', 'idx_posts_userId');
 * // stats = {indexName: 'idx_posts_userId', totalRows: 10000, avgRowsPerDistinct: [100]}
 */
export function getIndexStats(
  db: Database,
  tableName: string,
  indexName: string,
): IndexStats | undefined {
  // Check if sqlite_stat1 table exists (created by ANALYZE command)
  const statTableExists = db
    .prepare(
      `
      SELECT 1
      FROM sqlite_master
      WHERE type = 'table' AND name = 'sqlite_stat1'
    `,
    )
    .get() as {1: number} | undefined;

  if (!statTableExists) {
    return undefined;
  }

  const result = db
    .prepare(
      `
      SELECT stat
      FROM sqlite_stat1
      WHERE tbl = ? AND idx = ?
    `,
    )
    .get(tableName, indexName) as {stat: string} | undefined;

  if (!result?.stat) {
    return undefined;
  }

  const parts = result.stat.split(' ').map(n => parseInt(n, 10));

  if (parts.length < 2 || parts.some(isNaN)) {
    // Invalid stat format
    return undefined;
  }

  return {
    indexName,
    totalRows: parts[0],
    avgRowsPerDistinct: parts.slice(1),
  };
}

/**
 * Find all indexes on a table.
 *
 * @param db Database instance
 * @param tableName Table name
 * @returns Array of index names
 */
export function findIndexesForTable(db: Database, tableName: string): string[] {
  const indexes = db
    .prepare(
      `
      SELECT name
      FROM sqlite_master
      WHERE type = 'index'
        AND tbl_name = ?
        AND sql IS NOT NULL
    `,
    )
    .all(tableName) as Array<{name: string}>;

  return indexes.map(idx => idx.name);
}

/**
 * Check if targetColumns appears as a prefix of indexColumns.
 *
 * @param indexColumns Columns in the index, in order
 * @param targetColumns Target columns to match
 * @returns true if targetColumns is a prefix (in order) of indexColumns
 *
 * @example
 * isPrefixMatch(['userId', 'projectId'], ['userId', 'projectId']) // true (exact)
 * isPrefixMatch(['userId', 'projectId', 'createdAt'], ['userId', 'projectId']) // true (superset)
 * isPrefixMatch(['userId'], ['userId', 'projectId']) // false (too short)
 * isPrefixMatch(['projectId', 'userId'], ['userId', 'projectId']) // false (wrong order)
 */
function isPrefixMatch(
  indexColumns: string[],
  targetColumns: string[],
): boolean {
  if (indexColumns.length < targetColumns.length) {
    return false;
  }

  return targetColumns.every((col, i) => indexColumns[i] === col);
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
 * @param db Database instance (must have run ANALYZE)
 * @param childTable The "many" side of the relationship
 * @param joinColumns The columns used in the join, in order of preference
 * @returns Average fan-out factor, or undefined if no statistics available
 *
 * @example
 * // Joining users -> posts on (userId, projectId)
 * // Index exists on posts(userId, projectId) with stat "10000 100 1"
 * const fanOut = getJoinFanOut(db, 'posts', ['userId', 'projectId']);
 * // fanOut = 1 (exactly one post per user+project combination)
 *
 * @example
 * // Joining on (userId, projectId) but only single-column indexes exist
 * // Index on userId has fan-out 100, index on projectId has fan-out 50
 * const fanOut = getJoinFanOut(db, 'posts', ['userId', 'projectId']);
 * // fanOut = 50 (most selective single column)
 *
 * @example
 * // No indexes exist
 * const fanOut = getJoinFanOut(db, 'posts', ['userId']);
 * // fanOut = undefined (caller should use default like 3)
 */
export function getJoinFanOut(
  db: Database,
  childTable: string,
  joinColumns: string[],
): number | undefined {
  if (joinColumns.length === 0) {
    return undefined;
  }

  const allIndexes = findIndexesForTable(db, childTable);

  // Phase 1: Look for compound indexes where joinColumns is a prefix
  for (const indexName of allIndexes) {
    const indexColumns = getIndexColumns(db, indexName);
    if (!indexColumns) {
      continue;
    }

    if (isPrefixMatch(indexColumns, joinColumns)) {
      const stats = getIndexStats(db, childTable, indexName);
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
    for (const indexName of allIndexes) {
      const indexColumns = getIndexColumns(db, indexName);
      if (!indexColumns || indexColumns[0] !== col) {
        // Only consider indexes where this column is the first (leftmost)
        continue;
      }

      const stats = getIndexStats(db, childTable, indexName);
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
