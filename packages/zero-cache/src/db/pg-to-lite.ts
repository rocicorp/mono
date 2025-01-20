import type {LogContext} from '@rocicorp/logger';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/replication-state.js';
import {
  dataTypeToZqlValueType,
  liteTypeString,
  upstreamDataType,
  type LiteTypeString,
} from '../types/lite.js';
import {liteTableName} from '../types/names.js';
import * as PostgresTypeClass from './postgres-type-class-enum.js';
import {
  type ColumnSpec,
  type IndexSpec,
  type LiteIndexSpec,
  type LiteTableSpec,
  type TableSpec,
} from './specs.js';

export const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  pos: Number.MAX_SAFE_INTEGER, // i.e. last
  characterMaximumLength: null,
  dataType: 'text',
  notNull: false,
  dflt: null,
};

export function warnIfDataTypeSupported(
  lc: LogContext,
  liteTypeString: LiteTypeString,
  table: string,
  column: string,
) {
  if (dataTypeToZqlValueType(liteTypeString) === undefined) {
    lc.warn?.(
      `\n\nWARNING: zero does not yet support the "${upstreamDataType(
        liteTypeString,
      )}" data type.\n` +
        `The "${table}"."${column}" column will not be synced to clients.\n\n`,
    );
  }
}

// e.g. true, false, 1234, 1234.5678
const SIMPLE_TOKEN_EXPRESSION_REGEX = /^([^']+)$/;

// For strings and certain incarnations of primitives (e.g. integers greater
// than 2^31-1, Postgres' nodeToString() represents the values as type-casted
// 'string' values, e.g. `'2147483648'::bigint`, `'foo'::text`.
//
// These type-qualifiers must be removed, as SQLite doesn't understand or
// care about them.
const STRING_EXPRESSION_REGEX = /^('.*')::[^']+$/;

function mapPostgresToLiteDefault(
  table: string,
  column: string,
  dataType: string,
  defaultExpression: string | null | undefined,
) {
  if (!defaultExpression) {
    return null;
  }
  if (SIMPLE_TOKEN_EXPRESSION_REGEX.test(defaultExpression)) {
    if (dataTypeToZqlValueType(dataType) === 'boolean') {
      return defaultExpression === 'true' ? '1' : '0';
    }
    return defaultExpression;
  }
  const match = STRING_EXPRESSION_REGEX.exec(defaultExpression);
  if (!match) {
    throw new Error(
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
  const {pos, dataType, pgTypeClass, notNull, dflt} = column.spec;
  return {
    pos,
    dataType: liteTypeString(
      dataType,
      notNull,
      pgTypeClass === PostgresTypeClass.Enum,
    ),
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
        : mapPostgresToLiteDefault(table, column.name, dataType, dflt),
  };
}

export function mapPostgresToLite(t: TableSpec): LiteTableSpec {
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
      [ZERO_VERSION_COLUMN_NAME]: ZERO_VERSION_COLUMN_SPEC,
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
