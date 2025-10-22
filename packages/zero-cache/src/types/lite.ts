import {assert} from '../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../shared/src/bigint-json.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../../zero-schema/src/table-schema.ts';
import type {LiteTableSpec} from '../db/specs.ts';
import type {ColumnMetadata} from '../services/change-source/column-metadata.ts';
import {
  dataTypeToZqlValueType as upstreamDataTypeToZqlValueType,
  type PostgresValueType,
} from './pg.ts';
import type {RowValue} from './row-key.ts';

/** Javascript value types supported by better-sqlite3. */
export type LiteValueType = number | bigint | string | null | Uint8Array;

export type LiteRow = Readonly<Record<string, LiteValueType>>;
export type LiteRowKey = LiteRow; // just for API readability

function columnMetadata(col: string, table: LiteTableSpec): ColumnMetadata {
  const spec = table.columns[col];
  assert(spec, `Unknown column ${col} in table ${table.name}`);
  return spec.metadata;
}

export const JSON_STRINGIFIED = 's';
export const JSON_PARSED = 'p';

export type JSONFormat = typeof JSON_STRINGIFIED | typeof JSON_PARSED;

/**
 * Creates a LiteRow from the supplied RowValue. A copy of the `row`
 * is made only if a value conversion is performed.
 */
export function liteRow(
  row: RowValue,
  table: LiteTableSpec,
  jsonFormat: JSONFormat,
): {row: LiteRow; numCols: number} {
  let copyNeeded = false;
  let numCols = 0;

  for (const key in row) {
    numCols++;
    const val = row[key];
    const liteVal = liteValue(val, columnMetadata(key, table), jsonFormat);
    if (val !== liteVal) {
      copyNeeded = true;
      break;
    }
  }
  if (!copyNeeded) {
    return {row: row as unknown as LiteRow, numCols};
  }
  // Slow path for when a conversion is needed.
  numCols = 0;
  const converted: Record<string, LiteValueType> = {};
  for (const key in row) {
    numCols++;
    converted[key] = liteValue(row[key], columnMetadata(key, table), jsonFormat);
  }
  return {row: converted, numCols};
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
export function liteValue(
  val: PostgresValueType,
  metadata: ColumnMetadata,
  jsonFormat: JSONFormat,
): LiteValueType {
  if (val instanceof Uint8Array || val === null) {
    return val;
  }
  const valueType = dataTypeToZqlValueType(metadata);
  if (valueType === 'json') {
    if (jsonFormat === JSON_STRINGIFIED && typeof val === 'string') {
      // JSON and JSONB values are already strings if the JSON was not parsed.
      return val;
    }
    // Non-JSON/JSONB values will always appear as objects / arrays.
    return stringify(val);
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
  if (Array.isArray(val)) {
    return val.map(v => toLiteValue(v));
  }
  assert(
    val.constructor?.name === 'Object',
    `Unhandled object type ${val.constructor?.name}`,
  );
  return val; // JSON
}

export function mapLiteDataTypeToZqlSchemaValue(
  metadata: ColumnMetadata,
): SchemaValue {
  return {type: mapLiteDataTypeToZqlValueType(metadata)};
}

function mapLiteDataTypeToZqlValueType(metadata: ColumnMetadata): ValueType {
  const type = dataTypeToZqlValueType(metadata);
  if (type === undefined) {
    throw new Error(`Unsupported data type ${metadata.upstreamType}`);
  }
  return type;
}

/**
 * Legacy type alias for pipe-delimited column type strings.
 * Kept for backward compatibility with old database schemas that don't have
 * the metadata table yet.
 *
 * The format of the type string is the original upstream type, followed
 * by any number of attributes, each of which begins with the `|` character,
 * and optionally ending with `[]` to indicate an array type.
 * The current list of attributes are:
 * * `|NOT_NULL` to indicate that the upstream column does not allow nulls
 * * `|TEXT_ENUM` to indicate an enum that should be treated as a string
 * * `[]` suffix to indicate an array type
 *
 * Note: The legacy `|TEXT_ARRAY` attribute is still supported for backwards
 * compatibility but new data uses the `[]` suffix instead.
 *
 * Examples:
 * * `int8`
 * * `int8|NOT_NULL`
 * * `timestamp with time zone`
 * * `timestamp with time zone|NOT_NULL`
 * * `nomz|TEXT_ENUM`
 * * `nomz|NOT_NULL|TEXT_ENUM`
 * * `int8[]`
 * * `int8|NOT_NULL[]`
 * * `nomz|TEXT_ENUM[]`
 */
export type LiteTypeString = string;

/**
 * Returns the value type for the column metadata if it is supported by ZQL.
 *
 * For types not supported by ZQL, returns `undefined`.
 */
export function dataTypeToZqlValueType(
  metadata: ColumnMetadata,
): ValueType | undefined {
  // Extract base type, removing array notation if present
  const baseType = metadata.upstreamType.replace(/\[\]$/, '');
  return upstreamDataTypeToZqlValueType(
    baseType.toLowerCase(),
    metadata.isEnum,
    metadata.isArray,
  );
}
