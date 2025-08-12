/**
 * Shared utilities for ignored tables functionality
 */

/**
 * Builds a Set of table names to ignore, expanding simple names to include public schema
 * @param tables - Array of table names from configuration
 * @returns Set containing both simple and qualified names for efficient lookup
 */
export function buildIgnoredTablesSet(tables: string[]): Set<string> {
  return new Set(
    tables.flatMap(table =>
      table.includes('.') ? [table] : [table, `public.${table}`]
    )
  );
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
  return ignoredTables.has(relation.name) || 
         ignoredTables.has(`${relation.schema}.${relation.name}`);
}