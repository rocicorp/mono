import type {LogContext} from '@rocicorp/logger';
import type {ColumnMetadata} from '../services/change-source/column-metadata.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/constants.ts';
import {dataTypeToZqlValueType} from '../types/lite.ts';
import {liteTableName} from '../types/names.ts';
import * as PostgresTypeClass from './postgres-type-class-enum.ts';
import {
  type ColumnSpec,
  type IndexSpec,
  type LiteIndexSpec,
  type LiteTableSpec,
  type TableSpec,
} from './specs.ts';

function zeroVersionColumnSpec(defaultVersion: string | undefined): ColumnSpec {
  return {
    pos: Number.MAX_SAFE_INTEGER, // i.e. last
    metadata: {
      upstreamType: 'text',
      isNotNull: false,
      isEnum: false,
      isArray: false,
      characterMaxLength: null,
    },
    notNull: false,
    dflt: !defaultVersion ? null : `'${defaultVersion}'`,
    elemPgTypeClass: null,
  };
}

export function warnIfDataTypeSupported(
  lc: LogContext,
  metadata: ColumnMetadata,
  table: string,
  column: string,
) {
  if (dataTypeToZqlValueType(metadata) === undefined) {
    lc.warn?.(
      `\n\nWARNING: zero does not yet support the "${metadata.upstreamType}" data type.\n` +
        `The "${table}"."${column}" column will not be synced to clients.\n\n`,
    );
  }
}

// As per https://www.sqlite.org/lang_altertable.html#altertabaddcol,
// expressions with parentheses are disallowed ...
const SIMPLE_TOKEN_EXPRESSION_REGEX = /^[^'()]+$/; // e.g. true, false, 1234, 1234.5678

// as well as current_time, current_date, and current_timestamp ...
const UNSUPPORTED_TOKENS = /\b(current_time|current_date|current_timestamp)\b/i;

// For strings and certain incarnations of primitives (e.g. integers greater
// than 2^31-1, Postgres' nodeToString() represents the values as type-casted
// 'string' values, e.g. `'2147483648'::bigint`, `'foo'::text`.
//
// These type-qualifiers must be removed, as SQLite doesn't understand or
// care about them.
const STRING_EXPRESSION_REGEX = /^('.*')::[^']+$/;

// Exported for testing.
export function mapPostgresToLiteDefault(
  table: string,
  column: string,
  metadata: ColumnMetadata,
  defaultExpression: string | null | undefined,
) {
  if (!defaultExpression) {
    return null;
  }
  if (UNSUPPORTED_TOKENS.test(defaultExpression)) {
    throw new UnsupportedColumnDefaultError(
      `Cannot ADD a column with CURRENT_TIME, CURRENT_DATE, or CURRENT_TIMESTAMP`,
    );
  }
  if (SIMPLE_TOKEN_EXPRESSION_REGEX.test(defaultExpression)) {
    if (dataTypeToZqlValueType(metadata) === 'boolean') {
      return defaultExpression === 'true' ? '1' : '0';
    }
    return defaultExpression;
  }
  const match = STRING_EXPRESSION_REGEX.exec(defaultExpression);
  if (!match) {
    throw new UnsupportedColumnDefaultError(
      `Unsupported default value for ${table}.${column}: ${defaultExpression}`,
    );
  }
  return match[1];
}

export function mapPostgresToLiteColumn(
  table: string,
  column: {name: string; spec: ColumnSpec},
  ignoreDefault?: 'ignore-default',
): ColumnSpec {
  const {
    pos,
    metadata: upstreamMetadata,
    pgTypeClass,
    notNull,
    dflt,
    elemPgTypeClass = null,
  } = column.spec;

  // Build ColumnMetadata directly from upstream column spec
  // PostgreSQL includes [] in dataType for array types (e.g., 'int4[]', 'int4[][]')
  const isArray = upstreamMetadata.upstreamType.includes('[]');
  const metadata: ColumnMetadata = {
    upstreamType: upstreamMetadata.upstreamType,
    isNotNull: notNull ?? false,
    isEnum: (elemPgTypeClass ?? pgTypeClass) === PostgresTypeClass.Enum,
    isArray,
    characterMaxLength: upstreamMetadata.characterMaxLength ?? null,
  };

  return {
    pos,
    metadata,
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
        : mapPostgresToLiteDefault(table, column.name, metadata, dflt),
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
