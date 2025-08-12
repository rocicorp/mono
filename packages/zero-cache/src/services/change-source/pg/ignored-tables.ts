/**
 * Shared utilities for ignored tables functionality
 */

/**
 * Builds a Set of fully qualified table names to ignore
 * @param tables - Array of fully qualified table names (schema.table)
 * @returns Set for efficient lookup
 */
export function buildIgnoredTablesSet(tables: string[]): Set<string> {
  return new Set(tables);
}

/**
 * Checks if a table should be ignored during replication
 * @param relation - Table with schema and name
 * @param ignoredTables - Set of fully qualified table names
 * @returns true if the table should be ignored
 */
export function isTableIgnored(
  relation: {schema: string; name: string}, 
  ignoredTables: Set<string>
): boolean {
  // Only check for exact schema.table match
  return ignoredTables.has(`${relation.schema}.${relation.name}`);
}