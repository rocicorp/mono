import {escapePostgresIdentifier} from '@databases/escape-identifier';
import type {FormatConfig, SQLItem, SQLQuery} from '@databases/sql';
import sql, {SQLItemType} from '@databases/sql';
import {assert, unreachable} from '../../shared/src/asserts.ts';
import {getOrInsertComputed} from '../../shared/src/map.ts';
import {
  isPgBpcharType,
  isPgNativeStringType,
  isPgNumberType,
  isPgStringType,
  isPgTextRepresentedType,
} from '../../zero-cache/src/types/pg-data-type.ts';
import {id} from '../../zero-cache/src/types/sql.ts';
import type {LiteralValue} from '../../zero-protocol/src/ast.ts';
import type {ServerColumnSchema} from '../../zero-types/src/server-schema.ts';

export function formatPg(sql: SQLQuery) {
  const format = new ReusingFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

export function formatPgInternalConvert(sql: SQLQuery) {
  const format = new SQLConvertFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

const sqlConvert = Symbol('fromJson');

type LiteralType = 'boolean' | 'number' | 'string' | 'null';
export type PluralLiteralType = Exclude<LiteralType, 'null'>;

type ColumnSqlConvertArg = {
  [sqlConvert]: 'column';
  type: string;
  // The Postgres namespace `type` lives in, when known. Used to schema-qualify
  // casts to user-defined types (enums, extension types, ...).
  typeSchema: string | undefined;
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
    typeSchema: serverColumnSchema.typeSchema,
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
  readonly #seen: Map<unknown, Map<string, number>> = new Map();
  #size = 0;
  readonly escapeIdentifier: (str: string) => string;

  constructor(escapeIdentifier: (str: string) => string) {
    this.escapeIdentifier = escapeIdentifier;
  }

  formatValue = (value: unknown) => {
    assert(isSqlConvert(value), 'JsonPackedFormat can only take JsonPackArgs.');
    const key = seenKey(value);
    const byType = this.#seen.get(value.value);
    if (byType?.has(key)) {
      return {
        placeholder: createPlaceholder(byType.get(key)!, value),
        value: PREVIOUSLY_SEEN_VALUE,
      };
    }
    this.#size++;
    if (byType) {
      byType.set(key, this.#size);
    } else {
      this.#seen.set(value.value, new Map([[key, this.#size]]));
    }
    return {
      placeholder: createPlaceholder(this.#size, value),
      value: stringify(value),
    };
  };
}

/**
 * Args with the same value can share a parameter when they are bound the same
 * way. The type name alone does not determine that: an enum is bound as its raw
 * string (see {@link stringify}) even when it shares its name with a type, such
 * as `json`, whose values are JSON encoded. Type names cannot contain NUL, so
 * the suffix cannot collide with another type's name.
 */
function seenKey(arg: SqlConvertArg): string {
  return arg[sqlConvert] === 'column' && arg.isEnum
    ? `${arg.type}\0enum`
    : arg.type;
}

/**
 * Whether a type in `typeSchema` is a built-in type: either the catalog says
 * that it lives in `pg_catalog`, or its namespace is unknown (legacy callers
 * that construct a `ServerColumnSchema` by hand), in which case it is assumed
 * to be built-in.
 */
export function isBuiltinTypeSchema(
  typeSchema: string | undefined,
): typeSchema is 'pg_catalog' | undefined {
  return typeSchema === undefined || typeSchema === 'pg_catalog';
}

/**
 * Renders the cast-target type reference for a (potentially user-defined) type.
 *
 * A type that the catalog places outside `pg_catalog` is schema-qualified
 * (e.g. `"app"."some_enum"`) so that it resolves regardless of the
 * connection's `search_path`. This includes `public`: Postgres searches
 * `pg_catalog` before `search_path`, so an unqualified `public` type with a
 * built-in name (e.g. an enum named `point`) would resolve to the built-in.
 * Built-in types, and types whose namespace is unknown, are left as-is.
 *
 * `quoteName` controls whether the unqualified type name is quoted: enums have
 * always been emitted quoted (e.g. `::"some_enum"`); other types (e.g.
 * `::isbn13`) are emitted bare. Schema-qualified output is always quoted on
 * both segments regardless of `quoteName`.
 *
 * Names are quoted with {@link id} rather than `escapePostgresIdentifier`,
 * which rejects names that Postgres accepts (longer than 60 characters, or
 * containing non-ASCII characters).
 */
function typeCastTarget(arg: ColumnSqlConvertArg, quoteName: boolean): string {
  const {type, typeSchema} = arg;
  if (!isBuiltinTypeSchema(typeSchema)) {
    return `${id(typeSchema)}.${id(type)}`;
  }
  return quoteName ? id(type) : type;
}

function createPlaceholder(index: number, arg: SqlConvertArg) {
  if (arg.type === 'null') {
    assert(arg.value === null, "Args of type 'null' must have value null");
    assert(!arg.plural, "Args of type 'null' must not be plural");
    return `$${index}`;
  }

  if (arg.value === null && arg.plural) {
    return `$${index}`;
  }

  if (arg[sqlConvert] === 'literal') {
    const {value} = arg;
    if (Array.isArray(value)) {
      const elType = pgTypeForLiteralType(arg.type);
      return formatPlural(index, `value::${elType}`);
    }
    const type = pgTypeForLiteralType(arg.type);
    return `$${index}::text${type === 'text' ? '' : `::${type}`}`;
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
  if (arg.isEnum) {
    return `${valuePlaceholder}::text::${typeCastTarget(arg, true)}`;
  }

  // Type names are only unique within a Postgres namespace. Only apply
  // built-in conversion semantics when the catalog confirms that this is a
  // pg_catalog type, or when legacy callers do not provide a namespace. A
  // user-defined type can legally have a built-in name such as `uuid` or
  // `numeric`; those values must be cast to the schema-qualified custom type.
  const usesBuiltinTypeSemantics = isBuiltinTypeSchema(arg.typeSchema);
  let atTimeZone = ` AT TIME ZONE 'UTC'`;
  if (usesBuiltinTypeSemantics) {
    switch (arg.type) {
      case 'timestamptz':
      // @ts-expect-error Fallthrough intended
      case 'timestamp with time zone':
        atTimeZone = '';
      // fallthrough

      case 'date':
      case 'timestamp':
      case 'timestamp without time zone':
        return `to_timestamp(${valuePlaceholder}::text::numeric / 1000.0)${atTimeZone}`;

      case 'timetz':
      // @ts-expect-error Fallthrough intended
      case 'time with time zone':
        atTimeZone = '';
      // fallthrough

      case 'time':
      case 'time without time zone':
        return `(${valuePlaceholder}::text::int * interval'1ms')::time${atTimeZone}`;

      // uuid: cast to native uuid type for proper comparison and index usage
      case 'uuid':
        return `${valuePlaceholder}::text::uuid`;
    }
  }
  if (usesBuiltinTypeSemantics && isPgNativeStringType(arg.type)) {
    if (!arg.isComparison) {
      return `${valuePlaceholder}::text::${arg.type}`;
    }
    // For comparison cast to an unbounded type, not the specific column
    // type (i.e. `arg.type`), because we don't want to force the value
    // being compared to the size/max-size of the column type before
    // comparison.
    //
    // For `char(n)` that is `bpchar` (not `character`, which means
    // `char(1)`), not `text`: Postgres compares `bpchar` to `text` by
    // casting the column to `text`, which prevents it from using the
    // column's indexes.
    // https://www.postgresql.org/docs/current/datatype-character.html
    return `${valuePlaceholder}::text${isPgBpcharType(arg.type) ? '::bpchar' : ''}`;
  }
  if (isPgTextRepresentedType(arg.type)) {
    return `${valuePlaceholder}::text::${typeCastTarget(arg, false)}`;
  }
  if (usesBuiltinTypeSemantics && isPgNumberType(arg.type)) {
    // For comparison cast to `double precision` which uses IEEE 754 (the same
    // representation as JavaScript numbers which will accurately
    // represent any number value from zql) not the specific column type
    // (i.e. `arg.type`), because we don't want to force the value being
    // compared to the range and precision of the column type before comparison.
    return arg.isComparison
      ? `${valuePlaceholder}::text::double precision`
      : `${valuePlaceholder}::text::${arg.type}`;
  }
  return `${valuePlaceholder}::text::${typeCastTarget(arg, false)}`;
}

function formatPlural(index: number, select: string) {
  return `ARRAY(
          SELECT ${select} FROM jsonb_array_elements_text($${index}::text::jsonb)
        )`;
}

export function pgTypeForLiteralType(type: Exclude<LiteralType, 'null'>) {
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

export {sql};

const PREVIOUSLY_SEEN_VALUE = Symbol('PREVIOUSLY_SEEN_VALUE');

const whitespacePrefixRe = /^\s*/;

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

            return escapeIdentifier(
              getOrInsertComputed(
                localIdentifiers,
                name,
                () => `__local_${localIdentifiers.size}__`,
              ),
            );
          })
          .join('.');
        break;
      }
    }
  }

  if (text.trim()) {
    const lines = text.split('\n');
    const min = Math.min(
      ...lines
        .filter(l => l.trim() !== '')
        .map(l => whitespacePrefixRe.exec(l)![0].length),
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
