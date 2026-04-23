import type {LogContext} from '@rocicorp/logger';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/constants.ts';
import {
  liteTypeString,
  liteTypeToZqlValueType,
  upstreamDataType,
  type LiteTypeString,
} from '../types/lite.ts';
import {liteTableName} from '../types/names.ts';
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
const QUOTED_STRING_WITH_CAST_REGEX = /^('.*')::(\w+)$/;

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
    return match[1];
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
