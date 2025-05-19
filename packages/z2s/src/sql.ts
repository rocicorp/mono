import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';
import type {FormatConfig, SQLItem, SQLQuery} from '@databases/sql';
import baseSql, {SQLItemType} from '@databases/sql';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {isPgNumberType, isPgStringType} from '../../zero-cache/src/types/pg.ts';
import type {LiteralValue} from '../../zero-protocol/src/ast.ts';
import type {ServerColumnSchema} from './schema.ts';

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

type ColumnSqlConvertArg = {
  [sqlConvert]: 'column';
  type: string;
  value: unknown;
  plural: boolean;
  isEnum: boolean;
  isComparison: boolean;
};

type SqlConvertArg =
  | ColumnSqlConvertArg
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
    plural: plural || serverColumnSchema.isArray,
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
    return arg.value as unknown as string;
  }
  if (
    arg[sqlConvert] === 'column' &&
    (arg.isEnum || isPgStringType(arg.type))
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
        placeholder: createPlaceholder(this.#seen.get(key)!, value),
        value: PREVIOUSLY_SEEN_VALUE,
      };
    }
    this.#seen.set(key, this.#seen.size + 1);
    return {
      placeholder: createPlaceholder(this.#seen.size, value),
      value: stringify(value),
    };
  };
}

function createPlaceholder(index: number, arg: SqlConvertArg) {
  if (arg.type === 'null') {
    assert(arg.value === null, "Args of type 'null' must have value null");
    assert(!arg.plural, "Args of type 'null' must not be plural");
    return `$${index}`;
  }

  if (arg[sqlConvert] === 'literal') {
    const collate = arg.type === 'string' ? ` COLLATE "${Z2S_COLLATION}"` : '';
    const {value} = arg;
    if (Array.isArray(value)) {
      const elType = pgTypeForLiteralType(arg.type);
      return formatPlural(index, `value::${elType}${collate}`);
    }
    return `$${index}::text::${pgTypeForLiteralType(arg.type)}${collate}`;
  }

  const common = formatCommonToSingularAndPlural(index, arg);
  return arg.plural ? formatPlural(index, common) : common;
}

function formatCommonToSingularAndPlural(
  index: number,
  arg: ColumnSqlConvertArg,
) {
  // Ok, so what is with all the `::text` casts
  // before the final cast?
  // This is to force the statement to describe its arguments
  // as being text. Without the text cast the args are described as
  // being bool/json/numeric/whatever and the bindings try to coerce
  // the inputs to those types.
  const valuePlaceholder = arg.plural ? 'value' : `$${index}`;
  switch (arg.type) {
    case 'date':
    case 'timestamp':
    case 'timestamp without time zone':
      return `to_timestamp(${valuePlaceholder}::text::bigint / 1000.0) AT TIME ZONE 'UTC'`;
    case 'timestamptz':
    case 'timestamp with time zone':
      return `to_timestamp(${valuePlaceholder}::text::bigint / 1000.0)`;
    // uuid doesn't support collation, so we compare as text
    case 'uuid':
      return arg.isComparison
        ? `${valuePlaceholder}::text COLLATE "${Z2S_COLLATION}"`
        : `${valuePlaceholder}::text::uuid`;
  }
  if (arg.isEnum) {
    return arg.isComparison
      ? `${valuePlaceholder}::text COLLATE "${Z2S_COLLATION}"`
      : `${valuePlaceholder}::text::"${arg.type}"`;
  }
  if (isPgStringType(arg.type)) {
    // For comparison cast to the general `text` type, not the
    // specific column type (i.e. `arg.type`), because we don't want to
    // force the value being compared to the size/max-size of the column
    // type before comparison.
    return arg.isComparison
      ? `${valuePlaceholder}::text COLLATE "${Z2S_COLLATION}"`
      : `${valuePlaceholder}::text::${arg.type}`;
  }
  if (isPgNumberType(arg.type)) {
    // For comparison cast to `double precision` which uses IEEE 754 (the same
    // representation as JavaScript numbers which will accurately
    // represent any number value from zql) not the specific column type
    // (i.e. `arg.type`), because we don't want to force the value being
    // compared to the range and precision of the column type before comparison.
    return arg.isComparison
      ? `${valuePlaceholder}::text::double precision`
      : `${valuePlaceholder}::text::${arg.type}`;
  }
  return `${valuePlaceholder}::text::${arg.type}`;
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
      // `double precision` uses IEEE 754, the same representation as JavaScript
      // numbers, and so this will accurately represent any number value
      // from zql
      return 'double precision';
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
        // This is a specific addition for Zero as Zero
        // does not support dots in identifiers.
        // If a dot is found, we assume it is a namespace
        // and split the identifier into its parts.
        const names =
          item.names.length === 1 &&
          typeof item.names[0] === 'string' &&
          item.names[0].includes('.')
            ? item.names[0].split('.')
            : item.names;

        text += names
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
