/**
 * Shared utilities for ignored tables functionality
 */

/**
 * Builds a Set of table names to ignore - direct matches only
 * @param tables - Array of table names from configuration
 * @returns Set for efficient lookup
 */
export function buildIgnoredTablesSet(tables: string[]): Set<string> {
  return new Set(tables);
}

/**
 * Checks if a table should be ignored during replication
 * @param relation - Table with schema and name
 * @param ignoredTables - Set of ignored table names
 * @returns true if the table should be ignored
 */
export function isTableIgnored(
  relation: {schema: string; name: string}, 
  ignoredTables: Set<string>
): boolean {
  // Direct match on table name or schema.table
  return ignoredTables.has(relation.name) || 
         ignoredTables.has(`${relation.schema}.${relation.name}`);
}