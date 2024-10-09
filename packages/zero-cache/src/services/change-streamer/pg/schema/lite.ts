import type {LogContext} from '@rocicorp/logger';
import {dataTypeToZqlValueType} from '../../../../types/lite.js';
import {liteTableName} from '../../../../types/names.js';
import type {ColumnSpec, TableSpec} from '../../../../types/specs.js';
import {ZERO_VERSION_COLUMN_NAME} from '../../../replicator/schema/replication-state.js';

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
    lc.info?.(
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
  defaultExpression: string | null,
) {
  if (defaultExpression === null) {
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

export function mapPostgresToLite(t: TableSpec): TableSpec {
  const name = liteTableName(t);
  return {
    ...t,
    schema: '', // SQLite does not support schemas
    name,
    columns: {
      ...Object.fromEntries(
        Object.entries(t.columns).map(
          ([col, {pos, dataType, notNull, dflt}]) => [
            col,
            {
              pos,
              dataType,
              characterMaximumLength: null,
              notNull,
              dflt: mapPostgresToLiteDefault(name, col, dataType, dflt),
            } satisfies ColumnSpec,
          ],
        ),
      ),
      [ZERO_VERSION_COLUMN_NAME]: ZERO_VERSION_COLUMN_SPEC,
    },
  };
}
