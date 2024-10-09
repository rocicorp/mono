import {PreciseDate} from '@google-cloud/precise-date';
import {assert} from '../../../shared/src/asserts.js';
import type {SchemaValue, ValueType} from '../../../zql/src/zql/ivm/schema.js';
import {stringify, type JSONValue} from './bigint-json.js';
import type {PostgresValueType} from './pg.js';
import type {RowValue} from './row-key.js';

/** Javascript value types supported by better-sqlite3. */
export type LiteValueType = number | bigint | string | null | Uint8Array;

export function liteValues(row: RowValue): LiteValueType[] {
  return Object.values(row).map(liteValue);
}

/**
 * Postgres values types that are supported by SQLite are stored as-is.
 * This includes Uint8Arrays for the `bytea` / `BLOB` type.
 * * `boolean` values are converted to `0` or `1` integers.
 * * `PreciseDate` values are converted to epoch microseconds.
 * * JSON and Array values are stored as `JSON.stringify()` strings.
 *
 * Note that this currently does not handle the `bytea[]` type, but that's
 * already a pretty questionable type.
 */
export function liteValue(val: PostgresValueType): LiteValueType {
  if (val instanceof Uint8Array) {
    return val;
  }
  const obj = toLiteValue(val);
  return obj && typeof obj === 'object' ? stringify(obj) : obj;
}

function toLiteValue(val: JSONValue): Exclude<JSONValue, boolean> {
  switch (typeof val) {
    case 'string':
    case 'number':
    case 'bigint':
      return val;
    case 'boolean':
      return val ? 1 : 0;
  }
  if (val === null) {
    return val;
  }
  if (val instanceof PreciseDate) {
    return val.getFullTime() / 1000n; // nanoseconds to microseconds
  }
  if (Array.isArray(val)) {
    return val.map(v => toLiteValue(v));
  }
  assert(
    val.constructor?.name === 'Object',
    `Unexpected object type ${val.constructor?.name}`,
  );
  return val; // JSON
}

export function mapLiteDataTypeToZqlSchemaValue(
  liteDataType: string,
): SchemaValue {
  return {type: mapLiteDataTypeToZqlValueType(liteDataType)};
}

function mapLiteDataTypeToZqlValueType(dataType: string): ValueType {
  const type = dataTypeToZqlValueType(dataType);
  if (type === undefined) {
    throw new Error(`Unsupported data type ${dataType}`);
  }
  return type;
}

/**
 * Returns the value type for the `pgDataType` if it is supported by ZQL.
 * (Note that `pgDataType` values are stored as-is in the SQLite column defs).
 *
 * For types not supported by ZQL, returns `undefined`.
 */
export function dataTypeToZqlValueType(
  pgDataType: string,
): ValueType | undefined {
  switch (pgDataType.toLowerCase()) {
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
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
    case 'float':
    case 'float4':
    case 'float8':
      return 'number';

    case 'character':
    case 'character varying':
    case 'text':
    case 'varchar':
      return 'string';

    case 'bool':
    case 'boolean':
      return 'boolean';

    // TODO: Add support for these.
    // case 'bytea':
    // case 'date':
    // case 'time':
    // case 'timestamp':
    // case 'timestamp with time zone':
    // case 'timestamp without time zone':
    // case 'time with time zone':
    // case 'time without time zone':
    default:
      return undefined;
  }
}
