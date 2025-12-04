import type {SQLQuery, FormatConfig} from '@databases/sql';
import baseSql from '@databases/sql';
import {escapeSQLiteIdentifier} from '@databases/escape-identifier';

// Named placeholder support for query caching

const NAMED_VALUE = Symbol('namedValue');

type NamedValue = {
  [NAMED_VALUE]: true;
  name: string;
  value: unknown;
};

/**
 * Wraps a value with a name for use with named SQL placeholders.
 * Use with `formatNamed()` to produce SQL like `WHERE col = :name`.
 */
export function named(name: string, value: unknown): NamedValue {
  return {[NAMED_VALUE]: true, name, value};
}

function isNamedValue(v: unknown): v is NamedValue {
  return v !== null && typeof v === 'object' && NAMED_VALUE in v;
}

const sqliteFormat: FormatConfig = {
  escapeIdentifier: str => escapeSQLiteIdentifier(str),
  formatValue: value => {
    // Unwrap NamedValue for backwards compatibility with positional format
    if (isNamedValue(value)) {
      return {placeholder: '?', value: value.value};
    }
    return {placeholder: '?', value};
  },
};

export function compile(sql: SQLQuery): string {
  return sql.format(sqliteFormat).text;
}

export function format(sql: SQLQuery) {
  return sql.format(sqliteFormat);
}

export const sql = baseSql.default;

/**
 * Formats a SQL query using named placeholders (`:name` syntax).
 * Returns an object with text and a values Record for use with
 * better-sqlite3's named parameter binding.
 *
 * Values wrapped with `named()` use their specified name.
 * Unwrapped values get auto-generated names like `_p0`, `_p1`, etc.
 */
export function formatNamed(query: SQLQuery): {
  text: string;
  values: Record<string, unknown>;
} {
  const values: Record<string, unknown> = {};

  const config: FormatConfig = {
    escapeIdentifier: str => escapeSQLiteIdentifier(str),
    formatValue: (value, index) => {
      if (isNamedValue(value)) {
        values[value.name] = value.value;
        return {placeholder: `:${value.name}`, value: value.value};
      }
      // For non-named values, use index-based name
      const name = `_p${index}`;
      values[name] = value;
      return {placeholder: `:${name}`, value};
    },
  };

  const result = query.format(config);
  return {text: result.text, values};
}
