import type {LogContext} from '@rocicorp/logger';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/constants.ts';
import {
  liteTypeString,
  liteTypeToZqlValueType,
  upstreamDataType,
  type LiteTypeString,
} from '../types/lite.ts';
import {liteTableName} from '../types/names.ts';
import {lit} from '../types/sql.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import {
  type ColumnSpec,
  type IndexSpec,
  type LiteIndexSpec,
  type LiteTableSpec,
  type TableSpec,
} from './specs.ts';

/**
 * Determines if a PostgreSQL column is an enum type.
 * This checks both the element type class (for arrays of enums) and the main type class.
 */
export function isEnumColumn(
  spec: Pick<ColumnSpec, 'pgTypeClass' | 'elemPgTypeClass'>,
): boolean {
  return (spec.elemPgTypeClass ?? spec.pgTypeClass) === PostgresTypeClass.Enum;
}

/**
 * Determines if a PostgreSQL column is an array type.
 * In PostgreSQL's system, array columns have a non-null elemPgTypeClass.
 */
export function isArrayColumn(
  spec: Pick<ColumnSpec, 'elemPgTypeClass'>,
): boolean {
  return spec.elemPgTypeClass !== null && spec.elemPgTypeClass !== undefined;
}

function zeroVersionColumnSpec(defaultVersion: string | undefined): ColumnSpec {
  return {
    pos: Number.MAX_SAFE_INTEGER, // i.e. last
    characterMaximumLength: null,
    dataType: 'text',
    notNull: false,
    dflt: !defaultVersion ? null : `'${defaultVersion}'`,
    elemPgTypeClass: null,
  };
}

export function warnIfDataTypeSupported(
  lc: LogContext,
  liteTypeString: LiteTypeString,
  table: string,
  column: string,
) {
  if (liteTypeToZqlValueType(liteTypeString) === undefined) {
    lc.warn?.(
      `\n\nWARNING: zero does not yet support the "${upstreamDataType(
        liteTypeString,
      )}" data type.\n` +
        `The "${table}"."${column}" column will not be synced to clients.\n\n`,
    );
  }
}

// Numeric literals: integers and decimals, optionally negative
const NUMERIC_LITERAL_REGEX = /^-?\d+(\.\d+)?$/;

// Boolean literals (PG emits lowercase)
const BOOLEAN_LITERAL_REGEX = /^(true|false)$/;

// Quoted string with type cast to a simple scalar type: 'value'::typename
// For strings and certain incarnations of primitives (e.g. integers greater
// than 2^31-1, Postgres' nodeToString() represents the values as type-casted
// 'string' values, e.g. `'2147483648'::bigint`, `'foo'::text`.
// Only matches simple type names (word characters) - array types like
// `::text[]` won't match and will trigger backfill.
//
// The quoted part must be exactly one string literal: a quote inside it can
// only appear as the `''` escape, so the expression cannot contain anything
// but the literal and its cast. The default is interpolated into DDL (SQLite
// has no parameters in `ALTER TABLE ... ADD ... DEFAULT`), so the literal is
// re-quoted from its decoded value rather than copied from the expression.
const QUOTED_STRING_WITH_CAST_REGEX = /^'((?:[^']|'')*)'::\w+$/;

// A default that spells an integer. Postgres quotes a negative one, and one
// beyond int4, e.g. `'-1'::integer` and `'2147483648'::bigint`.
const INTEGER_DEFAULT_REGEX = /^(?:-?\d+|'-?\d+'::\w+)$/;

// A default that spells a number, as itself or as an integer or numeric
// constant, e.g. `1.5`, `'-1.5'::numeric` and `'-2'::integer`. Postgres writes
// these the same way in any session. It writes a constant of another type, e.g.
// `'0.1'::real`, with the settings of the session that adds the column.
const NUMBER_DEFAULT_REGEX =
  /^(?:(-?\d+(?:\.\d+)?)|'(-?\d+(?:\.\d+)?)'::(?:integer|bigint|numeric))$/;

function numberInDefault(dflt: string): string | undefined {
  const match = NUMBER_DEFAULT_REGEX.exec(dflt);
  return match ? (match[1] ?? match[2]) : undefined;
}

// `numeric(p, s)` rounds a number to s digits after the point.
function numericKeepsDefault(dflt: string, spec: ColumnSpec): boolean {
  const number = numberInDefault(dflt);
  if (number === undefined) {
    return false;
  }
  // The typmod of a numeric column, which has none if it is unconstrained.
  const typmod = spec.characterMaximumLength;
  if (typmod === null || typmod === undefined) {
    return true;
  }
  // The scale is in the low 11 bits of the typmod less 4, signed.
  const scale = (((typmod - 4) & 0x7ff) ^ 0x400) - 0x400;
  return (number.split('.')[1]?.length ?? 0) <= scale;
}

const float4 = new Float32Array(1);
const float4Bits = new Uint32Array(float4.buffer);

// Whether x, a positive normal number, is halfway between two float4s.
function isHalfwayBetweenFloat4s(x: number): boolean {
  const below = Math.fround(x);
  if (below === x) {
    return false;
  }
  float4[0] = below;
  float4Bits[0] += x > below ? 1 : -1;
  return Math.abs(x - below) === Math.abs(float4[0] - x);
}

const LEADING_ZEROS_REGEX = /^0+/;
const TRAILING_ZEROS_REGEX = /0+$/;

// A `real` keeps a decimal of at most 6 significant digits (FLT_DIG): it is the
// shortest decimal that reads back as its float4, which is what Postgres writes.
// The exception is one halfway between two float4s, which Postgres rounds to the
// even one and then writes in full, e.g. 3e10 as 3.0000001e+10.
function realKeepsDefault(dflt: string): boolean {
  const number = numberInDefault(dflt);
  if (number === undefined) {
    return false;
  }
  const digits = number
    .replace('-', '')
    .replace('.', '')
    .replace(LEADING_ZEROS_REGEX, '')
    .replace(TRAILING_ZEROS_REGEX, '');
  const x = Math.abs(Number(number));
  return (
    digits.length <= 6 &&
    (x === 0 || (x >= 1.2e-38 && x <= 3.4e38 && !isHalfwayBetweenFloat4s(x)))
  );
}

// Postgres pads a `char(n)` to n characters.
function bpcharDefault(dflt: string, spec: ColumnSpec): string | undefined {
  const match = QUOTED_STRING_WITH_CAST_REGEX.exec(dflt);
  if (!match || !dflt.endsWith('::bpchar')) {
    return undefined;
  }
  const length = spec.characterMaximumLength;
  if (length === null || length === undefined) {
    return dflt;
  }
  const value = match[1].replaceAll("''", "'");
  const padding = length - [...value].length;
  return padding < 0
    ? undefined
    : `${lit(value + ' '.repeat(padding))}::bpchar`;
}

/**
 * Returns the default that makes the replica store a new column's value in
 * existing rows as replication would, or `undefined` if there is none, given a
 * default that {@link mapPostgresToLiteDefault} accepts.
 */
type NewColumnDefault = (dflt: string, spec: ColumnSpec) => string | undefined;

const keepIf =
  (keeps: (dflt: string, spec: ColumnSpec) => boolean): NewColumnDefault =>
  (dflt, spec) =>
    keeps(dflt, spec) ? dflt : undefined;

// The defaults of new columns that are not backfilled, by the column's type. A
// new column of any other type, or with any other default, is backfilled, which
// reads the values Postgres stored: it rounds a `numeric(p, s)` and the replica
// parses a `date`, for example.
const NEW_COLUMN_DEFAULTS = new Map<string, NewColumnDefault>([
  // Stored as the text Postgres writes.
  ...['text', 'varchar', 'name', 'uuid', 'json', 'jsonb', 'inet', 'cidr'].map(
    type => [type, keepIf(() => true)] as const,
  ),
  ['bool', keepIf(dflt => BOOLEAN_LITERAL_REGEX.test(dflt))],
  ['int2', keepIf(dflt => INTEGER_DEFAULT_REGEX.test(dflt))],
  ['int4', keepIf(dflt => INTEGER_DEFAULT_REGEX.test(dflt))],
  ['int8', keepIf(dflt => INTEGER_DEFAULT_REGEX.test(dflt))],
  ['numeric', keepIf(numericKeepsDefault)],
  ['float4', keepIf(realKeepsDefault)],
  ['float8', keepIf(dflt => numberInDefault(dflt) !== undefined)],
  ['bpchar', bpcharDefault],
]);

// Empty array constructor syntax: ARRAY[]::text[], ARRAY[]::integer[], etc.
// Maps to '[]' (JSON empty array) in SQLite.
const EMPTY_ARRAY_CONSTRUCTOR_REGEX = /^ARRAY\s*\[\s*\]::\w+\[\]$/i;

// Empty array literal syntax: '{}'::text[], '{}'::integer[], etc.
// Maps to '[]' (JSON empty array) in SQLite.
const EMPTY_ARRAY_LITERAL_REGEX = /^'\{\}'::\w+\[\]$/;

// Conservative allowlist approach for SQLite ADD COLUMN defaults.
// We only allow patterns we know are safe. Everything else triggers
// backfill from PostgreSQL, which correctly handles complex defaults.
//
// Note: We don't validate that the default value matches the column type
// (e.g., that a numeric literal is used with a numeric column). PostgreSQL
// already enforces this at schema definition time - you can't define
// `ALTER TABLE foo ADD bar TEXT DEFAULT 123` in PG. So we trust that any
// default we receive from the replication stream is type-compatible with
// whatever we map the type to in SQLite.
//
// Example: `true`/`false` literals can only appear as defaults for boolean
// columns in PG, so we don't need to check the column type before converting
// to 1/0.
//
// See: https://www.sqlite.org/lang_altertable.html#altertabaddcol
//
// Exported for testing.
export function mapPostgresToLiteDefault(
  table: string,
  column: string,
  defaultExpression: string | null | undefined,
): string | null {
  if (!defaultExpression) {
    return null;
  }

  // Numeric literals pass through unchanged
  if (NUMERIC_LITERAL_REGEX.test(defaultExpression)) {
    return defaultExpression;
  }

  // Boolean literals convert to SQLite's 1/0
  if (BOOLEAN_LITERAL_REGEX.test(defaultExpression)) {
    return defaultExpression === 'true' ? '1' : '0';
  }

  // Quoted strings with type casts: extract just the quoted part
  const match = QUOTED_STRING_WITH_CAST_REGEX.exec(defaultExpression);
  if (match) {
    return lit(match[1].replaceAll("''", "'"));
  }

  // Empty arrays: ARRAY[]::type[] or '{}'::type[] → '[]'
  if (
    EMPTY_ARRAY_CONSTRUCTOR_REGEX.test(defaultExpression) ||
    EMPTY_ARRAY_LITERAL_REGEX.test(defaultExpression)
  ) {
    return "'[]'";
  }

  // Everything else triggers backfill
  throw new UnsupportedColumnDefaultError(
    `Unsupported default value for ${table}.${column}: ${defaultExpression}`,
  );
}

export function mapPostgresToLiteColumn(
  table: string,
  column: {name: string; spec: ColumnSpec},
  ignoreDefault?: 'ignore-default',
): ColumnSpec {
  const {pos, dataType, notNull, dflt, elemPgTypeClass = null} = column.spec;

  // PostgreSQL includes [] in dataType for array types (e.g., 'int4[]',
  // 'int4[][]'). liteTypeString() appends attributes:
  // "varchar[]|NOT_NULL|TEXT_ARRAY", "my_enum[][]|TEXT_ENUM|TEXT_ARRAY"
  const liteType = liteTypeString(
    dataType,
    notNull,
    isEnumColumn(column.spec),
    isArrayColumn(column.spec),
  );

  return {
    pos,
    dataType: liteType,
    characterMaximumLength: null,
    // Note: NOT NULL constraints are always ignored for SQLite (replica) tables.
    // 1. They are enforced by the replication stream.
    // 2. We need nullability for columns with defaults to support
    // write permissions on the "proposed mutation" state. Proposed
    // mutations are written to SQLite in a `BEGIN CONCURRENT` transaction in mutagen.
    // Permission policies are run against that state (to get their ruling) then the
    // transaction is rolled back.
    notNull: false,
    // Note: DEFAULT constraints are ignored when creating new tables, but are
    //       necessary for adding columns to tables with existing rows.
    dflt:
      ignoreDefault === 'ignore-default'
        ? null
        : mapPostgresToLiteDefault(table, column.name, dflt),
    elemPgTypeClass,
  };
}

/**
 * Returns the default for the `add-column` of a new column, or throws an
 * {@link UnsupportedColumnDefaultError} if the column must be backfilled
 * instead. The default is the column's, or one that
 * {@link mapPostgresToLiteDefault} maps to the value Postgres gave existing
 * rows, e.g. a `char(n)` padded to n characters.
 *
 * This only accepts the defaults in {@link NEW_COLUMN_DEFAULTS}. It is stricter
 * than {@link mapPostgresToLiteDefault} because only the change source can
 * schedule a backfill. A replica has to apply an `add-column` that is already
 * in the change log, with no backfill to follow, so what it accepts can't be
 * narrowed without stopping replication.
 */
export function newColumnDefault(
  table: string,
  column: {name: string; spec: ColumnSpec},
): string | null | undefined {
  mapPostgresToLiteColumn(table, column);
  const {spec} = column;
  if (!spec.dflt) {
    return spec.dflt;
  }
  const dflt =
    isArrayColumn(spec) || isEnumColumn(spec)
      ? // The only array default mapPostgresToLiteDefault() accepts is an
        // empty one, which is stored as `[]`, and an enum is stored as its
        // label.
        spec.dflt
      : NEW_COLUMN_DEFAULTS.get(spec.dataType)?.(spec.dflt, spec);
  if (dflt === undefined) {
    throw new UnsupportedColumnDefaultError(
      `Unsupported default value for ${table}.${column.name}: ${spec.dflt}`,
    );
  }
  return dflt;
}

export function mapPostgresToLite(
  t: TableSpec,
  defaultVersion?: string,
): LiteTableSpec {
  // PRIMARY KEYS are not written to the replica. Instead, we rely
  // UNIQUE indexes, including those created for upstream PRIMARY KEYs.
  const {schema: _, primaryKey: _dropped, ...liteSpec} = t;
  const name = liteTableName(t);
  return {
    ...liteSpec,
    name,
    columns: {
      ...Object.fromEntries(
        Object.entries(t.columns).map(([col, spec]) => [
          col,
          // `ignore-default` for create table statements because
          // there are no rows to set the default for.
          mapPostgresToLiteColumn(name, {name: col, spec}, 'ignore-default'),
        ]),
      ),
      [ZERO_VERSION_COLUMN_NAME]: zeroVersionColumnSpec(defaultVersion),
    },
  };
}

export function mapPostgresToLiteIndex(index: IndexSpec): LiteIndexSpec {
  const {schema, tableName, name, ...liteIndex} = index;
  return {
    tableName: liteTableName({schema, name: tableName}),
    name: liteTableName({schema, name}),
    ...liteIndex,
  };
}

export class UnsupportedColumnDefaultError extends Error {
  readonly name = 'UnsupportedColumnDefaultError';
}
