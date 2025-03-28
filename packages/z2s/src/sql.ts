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

export function formatPgInternalConvert(sql: SQLQuery) {
  const format = new SQLConvertFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

export function formatSqlite(sql: SQLQuery) {
  const format = new ReusingFormat(escapeSQLiteIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

const sqlConvert = Symbol('fromJson');
type SqlConvertArg = {
  [sqlConvert]: true;
  type: ValueType;
  value: unknown;
  plural?: boolean | undefined;

  // collation is passed down given we need to apply it to each element of a plural argument
  collation?: 'ucs_basic' | undefined;
};
function isSqlConvert(value: unknown): value is SqlConvertArg {
  return value !== null && typeof value === 'object' && sqlConvert in value;
}

export function sqlConvertArg<T extends ValueType, P extends boolean = false>(
  type: T,
  value: true extends P ? TypeNameToTypeMap[T][] : TypeNameToTypeMap[T],
  // plural is an explicit argument so we do not get confused by a singular JSON array vs
  // an array of JSON.
  plural?: P,
  collation?: 'ucs_basic',
): SQLQuery {
  return sql.value({
    [sqlConvert]: true,
    type,
    value,
    plural,
    collation,
  } satisfies SqlConvertArg);
}

export function sqlConvertArgUnsafe(
  type: ValueType,
  value: unknown,
  plural?: boolean,
  collation?: 'ucs_basic',
): SQLQuery {
  return sqlConvertArg(type, value as never, plural, collation);
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

function stringify(arg: SqlConvertArg): string {
  if (arg.plural) {
    return JSON.stringify(arg.value);
  }

  switch (arg.type) {
    case 'json':
      return JSON.stringify(arg.value);
    case 'boolean':
      return arg.value ? 'true' : 'false';
    case 'number':
    case 'date':
    case 'timestamp':
      return (arg.value as number).toString();
    case 'string':
      return arg.value as string;
    case 'null':
      return 'null';
    default:
      unreachable(arg.type);
  }
}

class SQLConvertFormat implements FormatConfig {
  readonly #seen: Map<unknown, number> = new Map();
  readonly escapeIdentifier: (str: string) => string;

  constructor(escapeIdentifier: (str: string) => string) {
    this.escapeIdentifier = escapeIdentifier;
  }

  formatValue = (value: unknown) => {
    assert(isSqlConvert(value), 'JsonPackedFormat can only take JsonPackArgs.');
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
      value: stringify(value),
    };
  };

  #createPlaceholder(index: number, value: SqlConvertArg) {
    // Ok, so what is with all the `::text` casts
    // before the final cast?
    // This is to force the statement to describe its arguments
    // as being text. Without the text cast the args are described as
    // being bool/json/numeric/whatever and the bindings try to coerce
    // the inputs to those types.

    const sqlType = pgType(value.type);
    const collate = value.collation ? ` COLLATE "${value.collation}"` : '';

    if (!value.plural) {
      switch (value.type) {
        case 'json':
          // We use JSONB since that can be used as a primary key type
          // whereas JSON cannot. So JSONB covers more cases.
          return `$${index}::text::${sqlType}`;
        case 'boolean':
          return `$${index}::text::${sqlType}`;
        case 'number':
          return `$${index}::text::${sqlType}`;
        case 'string':
          return `$${index}::text ${collate}`;
        case 'date':
        case 'timestamp':
          return `to_timestamp($${index}::text::${sqlType} / 1000.0) AT TIME ZONE 'UTC'`;
        case 'null':
          return 'NULL';
        default:
          unreachable(value.type);
      }
    }

    switch (value.type) {
      case 'json':
      case 'boolean':
      case 'number':
        return `ARRAY(
          SELECT value::${sqlType} FROM jsonb_array_elements_text($${index}::text::jsonb)
        )`;
      case 'string':
        return `ARRAY(
            SELECT value ${collate} FROM jsonb_array_elements_text($${index}::text::jsonb)
          )`;
      case 'date':
      case 'timestamp':
        return `ARRAY(
          SELECT to_timestamp(value::bigint / 1000.0)
          FROM jsonb_array_elements_text($${index}::text::jsonb)
        )::timestamp[]`;
      case 'null':
        throw new Error('unsupported null');
      default:
        unreachable(value.type);
    }
  }
}

function pgType(type: ValueType) {
  switch (type) {
    case 'json':
      return 'jsonb';
    case 'boolean':
      return 'boolean';
    case 'number':
      return 'numeric';
    case 'string':
      return 'text';
    case 'date':
    case 'timestamp':
      return 'bigint';
    case 'null':
      return 'null';
    default:
      unreachable(type);
  }
}

export const sql = baseSql.default;

const PREVIOUSLY_SEEN_VALUE = Symbol('PREVIOUSLY_SEEN_VALUE');
function formatFn(
  items: readonly SQLItem[],
  {escapeIdentifier, formatValue}: FormatConfig,
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
    values,
  };
}
