/**
 * Escapes the identifier with double quotes, as per:
 *
 * https://www.postgresql.org/docs/16/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
 */
export function id(name: string): string {
  return '"' + name.replace(/"/g, '""').replace(/\./g, '"."') + '"';
}

/**
 * Escapes and comma-separates a list of identifiers.
 */
export function idList(names: Iterable<string>): string {
  return [...names].map(name => id(name)).join(',');
}
