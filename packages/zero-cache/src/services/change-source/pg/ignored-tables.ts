/**
 * Shared utilities for ignored tables functionality
 */

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
  return ignoredTables.has(`${relation.schema}.${relation.name}`);
}