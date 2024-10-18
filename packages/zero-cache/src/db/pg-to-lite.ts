import type {LogContext} from '@rocicorp/logger';
import {ZERO_VERSION_COLUMN_NAME} from '../services/replicator/schema/replication-state.js';
import {dataTypeToZqlValueType} from '../types/lite.js';
import {liteTableName} from '../types/names.js';
import type {
  ColumnSpec,
  IndexSpec,
  LiteIndexSpec,
  LiteTableSpec,
  TableSpec,
} from './specs.js';

export const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  pos: Number.MAX_SAFE_INTEGER, // i.e. last
  characterMaximumLength: null,
  dataType: 'text',
  notNull: true,
  dflt: null,
};

export function warnIfDataTypeSupported(
  lc: LogContext,
  pgDataType: string,
  table: string,
  column: string,
) {
  if (dataTypeToZqlValueType(pgDataType) === undefined) {
    lc.warn?.(
      `\n\nWARNING: zero does not yet support the "${pgDataType}" data type.\n` +
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
): ColumnSpec {
  const {pos, dataType, notNull, dflt} = column.spec;
  return {
    pos,
    dataType,
    characterMaximumLength: null,
    notNull,
    dflt: mapPostgresToLiteDefault(table, column.name, dataType, dflt),
  };
}

export function mapPostgresToLite(t: TableSpec): LiteTableSpec {
  const {schema: _, ...liteSpec} = t;
  const name = liteTableName(t);
  return {
    ...liteSpec,
    name,
    columns: {
      ...Object.fromEntries(
        Object.entries(t.columns).map(([col, spec]) => [
          col,
          mapPostgresToLiteColumn(name, {name: col, spec}),
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
