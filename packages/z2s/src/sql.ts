import type {SQLQuery, FormatConfig, SQLItem} from '@databases/sql';
import baseSql, {SQLItemType} from '@databases/sql';
import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';
import type {ValueType} from '../../zero-protocol/src/client-schema.ts';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import type {TypeNameToTypeMap} from '../../zero-schema/src/table-schema.ts';

export function formatPg(sql: SQLQuery) {
  const format = new ReusingFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

export function formatPgJson(sql: SQLQuery) {
  const format = new JsonPackedFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) =>
    formatFn(items, format, true),
  );
}

export function formatSqlite(sql: SQLQuery) {
  const format = new ReusingFormat(escapeSQLiteIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

const jsonPack = Symbol('fromJson');
type JsonPackArg = {
  [jsonPack]: true;
  type: ValueType;
  value: unknown;
};
function isJsonPack(value: unknown): value is JsonPackArg {
  return value !== null && typeof value === 'object' && jsonPack in value;
}

export function jsonPackArg<T extends ValueType>(
  type: T,
  value: TypeNameToTypeMap[T],
): SQLQuery {
  return sql.value({[jsonPack]: true, type, value});
}

export function jsonPackArgUnsafe(type: ValueType, value: unknown): SQLQuery {
  return sql.value({[jsonPack]: true, type, value});
}

class ReusingFormat implements FormatConfig {
  readonly #seen: Map<unknown, number> = new Map();
  readonly escapeIdentifier: (str: string) => string;

  constructor(escapeIdentifier: (str: string) => string) {
    this.escapeIdentifier = escapeIdentifier;
  }

  formatValue = (value: unknown) => {
    if (this.#seen.has(value)) {
      return {
        placeholder: `$${this.#seen.get(value)}`,
        value: PREVIOUSLY_SEEN_VALUE,
      };
    }
    this.#seen.set(value, this.#seen.size + 1);
    return {placeholder: `$${this.#seen.size}`, value};
  };
}

class JsonPackedFormat implements FormatConfig {
  readonly #seen: Map<unknown, number> = new Map();
  readonly escapeIdentifier: (str: string) => string;

  constructor(escapeIdentifier: (str: string) => string) {
    this.escapeIdentifier = escapeIdentifier;
  }

  formatValue = (value: unknown) => {
    assert(isJsonPack(value), 'JsonPackedFormat can only take JsonPackArgs.');
    const key = value.value;
    if (this.#seen.has(key)) {
      return {
        placeholder: this.#createPlaceholder(this.#seen.get(key)!, value),
        value: PREVIOUSLY_SEEN_VALUE,
      };
    }
    this.#seen.set(key, this.#seen.size + 1);
    return {
      placeholder: this.#createPlaceholder(this.#seen.size, value),
      value: value.value,
    };
  };

  #createPlaceholder(index: number, value: JsonPackArg) {
    switch (value.type) {
      case 'json':
        return `$1::json->${index - 1}`;
      case 'boolean':
        return `($1::json->>${index - 1})::boolean`;
      case 'number':
        return `($1::json->>${index - 1})::numeric`;
      case 'string':
        return `$1::json->>${index - 1}`;
      case 'date':
      case 'timestamp':
        return `to_timestamp(($1::json->>${index - 1})::bigint / 1000) AT TIME ZONE 'UTC'`;
      case 'null':
        throw new Error('unsupported type');
      default:
        unreachable(value.type);
    }
  }
}

export const sql = baseSql.default;

const PREVIOUSLY_SEEN_VALUE = Symbol('PREVIOUSLY_SEEN_VALUE');
function formatFn(
  items: readonly SQLItem[],
  {escapeIdentifier, formatValue}: FormatConfig,
  jsonPack: boolean = false,
): {
  text: string;
  values: unknown[];
} {
  // Create an empty query object.
  let text = '';
  const values = [];

  const localIdentifiers = new Map<unknown, string>();

  for (const item of items) {
    switch (item.type) {
      // If this is just raw text, we add it directly to the query text.
      case SQLItemType.RAW: {
        text += item.text;
        break;
      }

      // If we got a value SQL item, add a placeholder and add the value to our
      // placeholder values array.
      case SQLItemType.VALUE: {
        const {placeholder, value} = formatValue(item.value, values.length);
        text += placeholder;
        if (value !== PREVIOUSLY_SEEN_VALUE) {
          values.push(value);
        }

        break;
      }

      // If we got an identifier type, escape the strings and get a local
      // identifier for non-string identifiers.
      case SQLItemType.IDENTIFIER: {
        text += item.names
          .map((name): string => {
            if (typeof name === 'string') return escapeIdentifier(name);

            if (!localIdentifiers.has(name))
              localIdentifiers.set(name, `__local_${localIdentifiers.size}__`);

            return escapeIdentifier(localIdentifiers.get(name)!);
          })
          .join('.');
        break;
      }
    }
  }

  if (text.trim()) {
    const lines = text.split('\n');
    const min = Math.min(
      ...lines.filter(l => l.trim() !== '').map(l => /^\s*/.exec(l)![0].length),
    );
    if (min) {
      text = lines.map(line => line.substr(min)).join('\n');
    }
  }
  return {
    text: text.trim(),
    values: jsonPack ? [values] : values,
  };
}
