import {assert} from '../../../shared/src/asserts.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../../zero-schema/src/table-schema.ts';
import type {LiteTableSpec} from '../db/specs.ts';
import {stringify, type JSONValue} from './bigint-json.ts';
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

export function liteValues(
  row: RowValue,
  table: LiteTableSpec,
  jsonFormat: JSONFormat,
): LiteValueType[] {
  return Object.entries(row).map(([col, val]) =>
    liteValue(val, columnType(col, table), jsonFormat),
  );
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
  const valueType = dataTypeToZqlValueType(pgType);
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
  const type = dataTypeToZqlValueType(dataType);
  if (type === undefined) {
    throw new Error(`Unsupported data type ${dataType}`);
  }
  return type;
}

// Note: Includes the "TEXT" substring for SQLite type affinity
const TEXT_ENUM_ATTRIBUTE = '|TEXT_ENUM';
const NOT_NULL_ATTRIBUTE = '|NOT_NULL';

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
 *
 * Examples:
 * * `int8`
 * * `int8|NOT_NULL`
 * * `timestamp with time zone`
 * * `timestamp with time zone|NOT_NULL`
 * * `nomz|TEXT_ENUM`
 * * `nomz|NOT_NULL|TEXT_ENUM`
 */
export type LiteTypeString = string;

/**
 * Formats a {@link LiteTypeString}.
 */
export function liteTypeString(
  upstreamDataType: string,
  notNull: boolean | null | undefined,
  textEnum: boolean,
): LiteTypeString {
  let typeString = upstreamDataType;
  if (notNull) {
    typeString += NOT_NULL_ATTRIBUTE;
  }
  if (textEnum) {
    typeString += TEXT_ENUM_ATTRIBUTE;
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
export function dataTypeToZqlValueType(
  liteTypeString: LiteTypeString,
): ValueType | undefined {
  switch (upstreamDataType(liteTypeString).toLowerCase()) {
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

    case 'date':
    case 'timestamp':
    case 'timestamptz':
    case 'timestamp with time zone':
    case 'timestamp without time zone':
      // Timestamps are represented as epoch milliseconds (at microsecond resolution using floating point),
      // and DATEs are represented as epoch milliseconds of UTC midnight of the date.
      return 'number';

    case 'bpchar':
    case 'character':
    case 'character varying':
    case 'text':
    case 'uuid':
    case 'varchar':
      return 'string';

    case 'bool':
    case 'boolean':
      return 'boolean';

    case 'json':
    case 'jsonb':
      return 'json';

    // TODO: Add support for these.
    // case 'bytea':
    default:
      if (liteTypeString.includes(TEXT_ENUM_ATTRIBUTE)) {
        return 'string';
      }
      return undefined;
  }
}
