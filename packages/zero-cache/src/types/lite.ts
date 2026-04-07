import {assert} from '../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../shared/src/bigint-json.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../../zero-schema/src/table-schema.ts';
import type {ColumnSpec, LiteTableSpec} from '../db/specs.ts';
import {dataTypeToZqlValueType as upstreamDataTypeToZqlValueType} from './pg-data-type.ts';
import type {PostgresValueType} from './pg.ts';
import type {RowValue} from './row-key.ts';

/** Javascript value types supported by better-sqlite3. */
export type LiteValueType = number | bigint | string | null | Uint8Array;

export type LiteRow = Readonly<Record<string, LiteValueType>>;
export type LiteRowKey = LiteRow; // just for API readability

function columnType(col: string, table: LiteTableSpec) {
  const spec = table.columns[col];
  assert(spec, `Unknown column ${col} in table ${table.name}`);
  return spec.dataType;
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
    const liteVal = liteValue(val, columnType(key, table), jsonFormat);
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
    converted[key] = liteValue(row[key], columnType(key, table), jsonFormat);
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
  pgType: string,
  jsonFormat: JSONFormat,
): LiteValueType {
  if (val instanceof Uint8Array || val === null) {
    return val;
  }
  const valueType = liteTypeToZqlValueType(pgType);
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
  liteDataType: LiteTypeString,
): SchemaValue {
  return {type: mapLiteDataTypeToZqlValueType(liteDataType)};
}

function mapLiteDataTypeToZqlValueType(dataType: LiteTypeString): ValueType {
  const type = liteTypeToZqlValueType(dataType);
  if (type === undefined) {
    throw new Error(`Unsupported data type ${dataType}`);
  }
  return type;
}

// Note: Includes the "TEXT" substring for SQLite type affinity
const TEXT_ENUM_ATTRIBUTE = '|TEXT_ENUM';
const NOT_NULL_ATTRIBUTE = '|NOT_NULL';
export const TEXT_ARRAY_ATTRIBUTE = '|TEXT_ARRAY';

/**
 * The `LiteTypeString` utilizes SQLite's loose type system to encode
 * auxiliary information about the upstream column (e.g. type and
 * constraints) that does not necessarily affect how SQLite handles the data,
 * but nonetheless determines how higher level logic handles the data.
 *
 * The format of the type string is the original upstream type, followed
 * by any number of attributes, each of which begins with the `|` character.
 * The current list of attributes are:
 * * `|NOT_NULL` to indicate that the upstream column does not allow nulls
 * * `|TEXT_ENUM` to indicate an enum that should be treated as a string
 * * `|TEXT_ARRAY` to indicate an array
 *
 * Examples:
 * * `int8`
 * * `int8|NOT_NULL`
 * * `timestamp with time zone`
 * * `timestamp with time zone|NOT_NULL`
 * * `nomz|TEXT_ENUM`
 * * `nomz|NOT_NULL|TEXT_ENUM`
 * * `int8[]` - Legacy (read support)
 * * `int8[]|TEXT_ARRAY`
 * * `int8|TEXT_ARRAY[]` - Legacy (read support)
 * * `int8[]|TEXT_ARRAY[]` - Legacy (read support)
 * * `int8|NOT_NULL[]` - Legacy (read support)
 * * `nomz[]|TEXT_ENUM|TEXT_ARRAY`
 * * `nomz|TEXT_ENUM[]` - Legacy (read support)
 * * `nomz|TEXT_ENUM|TEXT_ARRAY[]` - Legacy (read support)
 */
export type LiteTypeString = string;

/**
 * Formats a {@link LiteTypeString}.
 */
export function liteTypeString(
  upstreamDataType: string,
  notNull: boolean | null | undefined,
  textEnum: boolean,
  textArray: boolean,
): LiteTypeString {
  let typeString = upstreamDataType;
  assert(typeString.indexOf('|') === -1, 'Upstream type should not contain |');
  if (notNull) {
    typeString += NOT_NULL_ATTRIBUTE;
  }
  if (textEnum) {
    typeString += TEXT_ENUM_ATTRIBUTE;
  }
  if (textArray) {
    typeString += TEXT_ARRAY_ATTRIBUTE;
  }
  return typeString;
}

export function upstreamDataType(liteTypeString: LiteTypeString) {
  const delim = liteTypeString.indexOf('|');
  return delim > 0 ? liteTypeString.substring(0, delim) : liteTypeString;
}

export function nullableUpstream(liteTypeString: LiteTypeString) {
  return !liteTypeString.includes(NOT_NULL_ATTRIBUTE);
}

/**
 * Returns the value type for the `pgDataType` if it is supported by ZQL.
 * (Note that `pgDataType` values are stored as-is in the SQLite column defs).
 *
 * For types not supported by ZQL, returns `undefined`.
 */
export function liteTypeToZqlValueType(
  liteTypeString: LiteTypeString,
): ValueType | undefined {
  return upstreamDataTypeToZqlValueType(
    upstreamDataType(liteTypeString).toLowerCase(),
    liteTypeString.includes(TEXT_ENUM_ATTRIBUTE),
    isArray(liteTypeString),
  );
}

export function isEnum(liteTypeString: LiteTypeString) {
  return liteTypeString.includes(TEXT_ENUM_ATTRIBUTE);
}

export function isArray(liteTypeString: LiteTypeString) {
  return (
    liteTypeString.includes(TEXT_ARRAY_ATTRIBUTE) ||
    // Before we added `|TEXT_ARRAY`, we just used `[]` suffix to indicate arrays.
    liteTypeString.includes('[]')
  );
}

export function assertValidLiteColumnSpec(spec: ColumnSpec) {
  const {dataType} = spec;
  assert(
    dataType.includes(TEXT_ARRAY_ATTRIBUTE) === dataType.includes('[]'),
    () =>
      `TEXT_ARRAY_ATTRIBUTE and [] must be consistent in dataType: ${dataType}`,
  );
  assert(
    dataType.includes('[]') === (spec.elemPgTypeClass !== null),
    () =>
      `[] in dataType (${dataType}) must match elemPgTypeClass presence (${spec.elemPgTypeClass})`,
  );

  // and no [] after |
  assert(!/^.+\|.*\[\]/.test(dataType), `Invalid dataType ${dataType}`);
}
