import {RowValue} from './row-key.js';

export const INTEGER = 'INTEGER';
export const REAL = 'REAL';
export const BLOB = 'BLOB';
export const TEXT = 'TEXT';
export const BOOL = 'BOOL'; // Custom type that we manually support.

export function mapPostgresToLiteDataType(pgDataType: string): string {
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
      return INTEGER;
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
    case 'float':
    case 'float4':
    case 'float8':
      return REAL;
    case 'bytea':
      return BLOB;
    case 'character':
    case 'character varying':
    case 'text':
    case 'varchar':
      return TEXT;
    case 'bool':
    case 'boolean':
      return BOOL;
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

export function liteValues(row: RowValue) {
  return Object.values(row).map(v => (typeof v !== 'boolean' ? v : v ? 1 : 0));
}
