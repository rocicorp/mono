import type {FormatConfig} from '@databases/sql';
import type {SQLQuery} from '@databases/sql';
import {escapeSQLiteIdentifier} from '@databases/escape-identifier';
import {unwrapNamedValue} from './sql.ts';

/**
 * Escapes a SQLite string value by doubling single quotes.
 * SQLite uses single quotes for string literals and doubles them for escaping.
 */
function escapeSQLiteString(str: string): string {
  return `'${str.replace(/'/g, "''")}'`;
}

/**
 * Formats a value for inline inclusion in SQL (not as a placeholder).
 *
 * WARNING: This should ONLY be used for cost estimation in the query planner,
 * not for user-facing queries. All production queries must use parameterized
 * statements to prevent SQL injection.
 *
 * @param value The value to inline into SQL
 * @returns SQL literal representation of the value
 */
function inlineValue(value: unknown): string {
  if (value === null) {
    return 'NULL';
  }
  if (typeof value === 'string') {
    return escapeSQLiteString(value);
  }
  if (typeof value === 'number') {
    return String(value);
  }
  if (typeof value === 'boolean') {
    // SQLite uses 1 and 0 for booleans
    return value ? '1' : '0';
  }
  if (Array.isArray(value)) {
    // For arrays, use JSON representation (same as query-builder.ts does for IN clauses)
    return escapeSQLiteString(JSON.stringify(value));
  }
  // For objects/other JSON types
  return escapeSQLiteString(JSON.stringify(value));
}

/**
 * Format configuration that inlines values directly into SQL instead of using placeholders.
 *
 * This is used ONLY for cost estimation in the SQLite cost model, where we want SQLite's
 * query planner to see actual values to make better decisions about index usage and query plans.
 *
 * Production code must use the standard parameterized format from sql.ts.
 */
const sqliteInlineFormat: FormatConfig = {
  escapeIdentifier: str => escapeSQLiteIdentifier(str),
  formatValue: value => {
    // Unwrap NamedValue to get the underlying value
    const unwrapped = unwrapNamedValue(value);
    // undefined is our signal to use a placeholder
    // IMPORTANT. Changing this will break the planner as it will assume `NULL`
    // for constraints!
    return unwrapped === undefined
      ? {
          placeholder: '?',
          value: unwrapped,
        }
      : {
          placeholder: inlineValue(unwrapped),
          value: undefined, // No binding needed since value is inlined
        };
  },
};

/**
 * Compiles a SQL query with values inlined directly into the SQL string.
 *
 * WARNING: This should ONLY be used for cost estimation in the query planner.
 * Never use this for user-facing queries - always use the standard `compile()`
 * function from sql.ts which uses parameterized queries.
 *
 * @param sql The SQL query to compile
 * @returns SQL string with values inlined
 */
export function compileInline(sql: SQLQuery): string {
  return sql.format(sqliteInlineFormat).text;
}
