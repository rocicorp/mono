import {ZERO_VERSION_COLUMN_NAME} from 'zero-cache/src/services/replicator/schema/replication-state.js';
import type {LiteDataType} from 'zero-cache/src/types/lite.js';
import {liteTableName} from 'zero-cache/src/types/names.js';
import type {ColumnSpec, TableSpec} from 'zero-cache/src/types/specs.js';

export const ZERO_VERSION_COLUMN_SPEC: ColumnSpec = {
  pos: Number.MAX_SAFE_INTEGER, // i.e. last
  characterMaximumLength: null,
  dataType: 'TEXT',
  notNull: true,
  dflt: null,
};

export function checkDataTypeSupported(pgDataType: string) {
  mapPostgresToLiteDataType(pgDataType); // Throws on unsupported data types.
}

function mapPostgresToLiteDataType(pgDataType: string): LiteDataType {
  switch (pgDataType) {
    case 'smallint':
    case 'integer':
    case 'int':
    case 'int2':
    case 'int4':
    case 'int8':
    case 'bigint':
    case 'smallserial':
    case 'serial':
    case 'serial2':
    case 'serial4':
    case 'serial8':
    case 'bigserial':
      return 'INTEGER';
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
    case 'float':
    case 'float4':
    case 'float8':
      return 'REAL';
    case 'bytea':
      return 'BLOB';
    case 'character':
    case 'character varying':
    case 'text':
    case 'varchar':
      return 'TEXT';
    case 'bool':
    case 'boolean':
      return 'BOOL';
    // case 'date':
    // case 'time':
    // case 'timestamp':
    // case 'timestamp with time zone':
    // case 'timestamp without time zone':
    // case 'time with time zone':
    // case 'time without time zone':
    //   return 'INTEGER';
    default:
      if (pgDataType.endsWith('[]')) {
        throw new Error(`Array types are not supported: ${pgDataType}`);
      }
      throw new Error(`The "${pgDataType}" data type is not supported`);
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
  liteDataType: LiteDataType,
  defaultExpression: string | null,
) {
  if (defaultExpression === null) {
    return null;
  }
  if (SIMPLE_TOKEN_EXPRESSION_REGEX.test(defaultExpression)) {
    if (liteDataType === 'BOOL') {
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
          ([col, {pos, dataType: pgType, notNull, dflt}]) => {
            const dataType = mapPostgresToLiteDataType(pgType);
            return [
              col,
              {
                pos,
                dataType,
                characterMaximumLength: null,
                notNull,
                dflt: mapPostgresToLiteDefault(name, col, dataType, dflt),
              } satisfies ColumnSpec,
            ];
          },
        ),
      ),
      [ZERO_VERSION_COLUMN_NAME]: ZERO_VERSION_COLUMN_SPEC,
    },
  };
}
