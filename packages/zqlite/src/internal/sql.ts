import {escapeSQLiteIdentifier} from '@databases/escape-identifier';
import type {SQLItem, SQLQuery} from '@databases/sql';
import sql, {SQLItemType} from '@databases/sql';

/**
 * Identifiers are table and column names, so the working set is the size of
 * the schema. The bound is only there so that a client-influenced query shape
 * cannot grow the map without limit; clearing is cheaper than tracking
 * recency for something that refills in a handful of fetches.
 */
const MAX_CACHED_IDENTIFIERS = 10_000;

const escapedIdentifiers = new Map<string, string>();

function escapeIdentifier(name: string): string {
  let escaped = escapedIdentifiers.get(name);
  if (escaped === undefined) {
    escaped = escapeSQLiteIdentifier(name);
    if (escapedIdentifiers.size >= MAX_CACHED_IDENTIFIERS) {
      escapedIdentifiers.clear();
    }
    escapedIdentifiers.set(name, escaped);
  }
  return escaped;
}

/**
 * SQLite-specific replacement for `@databases/sql`'s generic `formatStandard`.
 *
 * It produces byte-identical output, but skips the generic formatter's
 * dedent pass (split/filter/regex/`Math.min` over every line) for the
 * single-line SQL that the query builder emits, and reuses escaped
 * identifiers instead of re-escaping the same column names on every fetch.
 * `TableSource.#fetch` formats a query for every fetch, so this runs on the
 * hot read path.
 */
function formatSQLite(items: readonly SQLItem[]): {
  text: string;
  values: unknown[];
} {
  let text = '';
  const values: unknown[] = [];
  for (const item of items) {
    switch (item.type) {
      case SQLItemType.RAW:
        text += item.text;
        break;
      case SQLItemType.VALUE:
        text += '?';
        values.push(item.value);
        break;
      case SQLItemType.IDENTIFIER:
        text +=
          item.names.length === 1
            ? escapeIdentifier(item.names[0])
            : item.names.map(name => escapeIdentifier(name)).join('.');
        break;
    }
  }
  // Multi-line templates are dedented by the common indent, as
  // `formatStandard` does. Single-line text is unaffected by that pass, since
  // the trailing `trim()` removes the leading whitespace either way.
  if (text.includes('\n') && text.trim()) {
    const lines = text.split('\n');
    const min = Math.min(
      ...lines
        .filter(line => line.trim() !== '')
        .map(line => line.length - line.trimStart().length),
    );
    if (min) {
      text = lines.map(line => line.substring(min)).join('\n');
    }
  }
  return {text: text.trim(), values};
}

export function compile(sql: SQLQuery): string {
  return sql.format(formatSQLite).text;
}

export function format(sql: SQLQuery) {
  return sql.format(formatSQLite);
}

export {sql};
