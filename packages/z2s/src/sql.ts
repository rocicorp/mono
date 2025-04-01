import type {SQLQuery, FormatConfig, SQLItem} from '@databases/sql';
import baseSql, {SQLItemType} from '@databases/sql';
import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import type {ServerColumnSchema} from './schema.ts';
import type {LiteralValue} from '../../zero-protocol/src/ast.ts';

export const Z2S_COLLATION = 'ucs_basic';

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
export type LiteralType = 'boolean' | 'number' | 'string' | 'null';
export type PluralLiteralType = Exclude<LiteralType, 'null'>;

type SqlConvertArg =
  | {
      [sqlConvert]: 'column';
      type: string;
      isEnum: boolean;
      value: unknown;
      plural: boolean;
      isComparison: boolean;
    }
  | {
      [sqlConvert]: 'literal';
      type: LiteralType;
      value: LiteralValue;
      plural: boolean;
    };

function isSqlConvert(value: unknown): value is SqlConvertArg {
  return value !== null && typeof value === 'object' && sqlConvert in value;
}

export function sqlConvertSingularLiteralArg(
  value: string | boolean | number | null,
): SQLQuery {
  const arg: SqlConvertArg = {
    [sqlConvert]: 'literal',
    type: value === null ? 'null' : (typeof value as LiteralType),
    value,
    plural: false,
  };
  return sql.value(arg);
}

export function sqlConvertPluralLiteralArg(
  type: PluralLiteralType,
  value: PluralLiteralType[],
): SQLQuery {
  const arg: SqlConvertArg = {
    [sqlConvert]: 'literal',
    type,
    value,
    plural: true,
  };
  return sql.value(arg);
}

export function sqlConvertColumnArg(
  serverColumnSchema: ServerColumnSchema,
  value: unknown,
  plural: boolean,
  isComparison: boolean,
): SQLQuery {
  return sql.value({
    [sqlConvert]: 'column',
    type: serverColumnSchema.type,
    isEnum: serverColumnSchema.isEnum,
    value,
    plural,
    isComparison,
  });
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

function stringify(arg: SqlConvertArg): string | null {
  if (arg.value === null) {
    return null;
  }
  if (arg.plural) {
    return JSON.stringify(arg.value);
  }
  if (arg[sqlConvert] === 'literal' && arg.type === 'string') {
    return arg.value as string;
  }
  if (
    arg[sqlConvert] === 'column' &&
    (arg.type === 'text' || arg.type === 'char' || arg.type === 'varchar')
  ) {
    return arg.value as string;
  }
  return JSON.stringify(arg.value);
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

  #createPlaceholder(index: number, arg: SqlConvertArg) {
    // Ok, so what is with all the `::text` casts
    // before the final cast?
    // This is to force the statement to describe its arguments
    // as being text. Without the text cast the args are described as
    // being bool/json/numeric/whatever and the bindings try to coerce
    // the inputs to those types.
    if (arg.type === 'null') {
      assert(arg.value === null, "Args of type 'null' must have value null");
      assert(!arg.plural, "Args of type 'null' must not be plural");
      return `$${index}`;
    }

    if (arg[sqlConvert] === 'literal') {
      const collate =
        arg.type === 'string' ? ` COLLATE "${Z2S_COLLATION}"` : '';
      const {value} = arg;
      if (Array.isArray(value)) {
        const elType = pgTypeForLiteralType(arg.type);
        return formatPlural(index, `value::${elType}${collate}`);
      }
      return `$${index}::text::${pgTypeForLiteralType(arg.type)}${collate}`;
    }

    const collate = arg.isComparison ? ` COLLATE "${Z2S_COLLATION}"` : '';
    if (!arg.plural) {
      if (arg.isEnum) {
        if (arg.isComparison) {
          return `$${index}::text ${collate}`;
        }
        return `$${index}::text::${arg.type}`;
      }
      switch (arg.type) {
        case 'date':
        case 'timestamp':
        case 'timestamptz':
        case 'timestamp with time zone':
        case 'timestamp without time zone':
          return `to_timestamp($${index}::text::bigint / 1000.0) AT TIME ZONE 'UTC'`;
        case 'text':
          return `$${index}::text${collate}`;
        case 'char':
        case 'varchar':
          return `$${index}::text::${arg.type}${collate}`;
        // uuid doesn't support collation, so we compare as text
        case 'uuid':
          return arg.isComparison
            ? `$${index}::text ${collate}`
            : `$${index}::text::uuid`;
        default:
          return `$${index}::text::${arg.type}`;
      }
    }

    if (arg.isEnum && arg.isComparison) {
      if (arg.isComparison) {
        return formatPlural(index, `value::text${collate}`);
      }
      return formatPlural(index, `value::${arg.type}`);
    }

    switch (arg.type) {
      case 'date':
      case 'timestamp':
      case 'timestamptz':
      case 'timestamp with time zone':
      case 'timestamp without time zone':
        return formatPlural(index, `to_timestamp(value::bigint / 1000.0)`);
      case 'text':
      case 'char':
      case 'varchar':
        return formatPlural(index, `value::${arg.type}${collate}`);
      // uuid doesn't support collation, so we compare as text
      case 'uuid':
        return arg.isComparison
          ? formatPlural(index, `value::text${collate}`)
          : formatPlural(index, `value::${arg.type}`);
      default:
        return formatPlural(index, `value::${arg.type}`);
    }
  }
}

function formatPlural(index: number, select: string) {
  return `ARRAY(
          SELECT ${select} FROM jsonb_array_elements_text($${index}::text::jsonb)
        )`;
}

function pgTypeForLiteralType(type: Exclude<LiteralType, 'null'>) {
  switch (type) {
    case 'boolean':
      return 'boolean';
    case 'number':
      return 'numeric';
    case 'string':
      return 'text';
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
