import type {SchemaValue, ValueType} from 'zql/src/zql/ivm/schema.js';
import type {RowValue} from './row-key.js';

export const INTEGER = 'INTEGER';
export const REAL = 'REAL';
export const BLOB = 'BLOB';
export const TEXT = 'TEXT';
export const BOOL = 'BOOL';

export type LiteDataType = 'INTEGER' | 'REAL' | 'BLOB' | 'TEXT' | 'BOOL'; // Custom type that we manually support.

export function liteValues(row: RowValue) {
  return Object.values(row).map(v => (typeof v !== 'boolean' ? v : v ? 1 : 0));
}

export function mapLiteDataTypeToZqlSchemaValue(
  liteDataType: string,
): SchemaValue {
  return {type: mapLiteDataTypeToZqlValueType(liteDataType)};
}

function mapLiteDataTypeToZqlValueType(liteDataType: string): ValueType {
  switch (liteDataType) {
    case INTEGER:
      return 'number';
    case REAL:
      return 'number';
    case TEXT:
      return 'string';
    case BOOL:
      return 'boolean';
    default: // Note: BLOB is not supported.
      throw new Error(`Unsupported data type ${liteDataType}`);
  }
}
