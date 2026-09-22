import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  getCodec,
  type SchemaValue,
} from '../../../zero-types/src/schema-value.ts';
import type {SourceSchema} from './schema.ts';

/**
 * Whether any column in a `columns` record carries a codec, memoized per
 * record object. Column records are long-lived (they belong to the schema),
 * and the read/write paths consult this once per row, so the common codec-free
 * case is a single `WeakMap` lookup rather than a scan of every column.
 */
const columnsCodecCache = new WeakMap<Record<string, SchemaValue>, boolean>();

/** Returns `true` if any column in `columns` carries a codec. */
export function columnsHaveCodecs(
  columns: Record<string, SchemaValue>,
): boolean {
  let result = columnsCodecCache.get(columns);
  if (result === undefined) {
    result = false;
    for (const name in columns) {
      if (getCodec(columns[name]) !== undefined) {
        result = true;
        break;
      }
    }
    columnsCodecCache.set(columns, result);
  }
  return result;
}

/**
 * Returns a copy of `row` with each codec column's `decode` applied. Used on
 * the read path when entries are inserted into the view so consumers see
 * app-typed values (e.g. `Date`) instead of the raw stored values. Returns
 * the input unchanged when no column carries a codec, or when every codec
 * column in the row is `null`/`undefined`.
 *
 * `null`/`undefined` values are passed through without invoking `decode`.
 */
export function decodeRowFields(row: Row, schema: SourceSchema): Row {
  const {columns} = schema;
  if (!columnsHaveCodecs(columns)) {
    return row;
  }
  let result: Record<string, unknown> | undefined;
  for (const key in row) {
    const value = row[key];
    // oxlint-disable-next-line eqeqeq
    const codec = value == null ? undefined : getCodec(columns[key]);
    if (codec) {
      result ??= {...row};
      result[key] = codec.decode(value);
    }
  }
  return (result ?? row) as Row;
}

/**
 * Returns a copy of `row` with each codec column's `encode` applied. Used on
 * the write path (insert/update) and for `start` rows so that everything
 * downstream sees the stored (encoded) JSON value. `null`/`undefined` pass
 * through. When the table has no codecs the input is returned unchanged.
 */
export function encodeRow<T extends Record<string, unknown>>(
  row: T,
  columns: Record<string, SchemaValue>,
): T {
  if (!columnsHaveCodecs(columns)) {
    return row;
  }
  let result: Record<string, unknown> | undefined;
  for (const key in row) {
    const value = row[key];
    // oxlint-disable-next-line eqeqeq
    const codec = value == null ? undefined : getCodec(columns[key]);
    if (codec) {
      result ??= {...row};
      result[key] = codec.encode(value);
    }
  }
  return (result ?? row) as T;
}

/**
 * Encodes a single value for `column`, or returns it unchanged if the column
 * has no codec or the value is `null`/`undefined`.
 */
export function encodeValue(
  value: unknown,
  column: SchemaValue | undefined,
): unknown {
  // oxlint-disable-next-line eqeqeq
  if (value == null) {
    return value;
  }
  const codec = getCodec(column);
  return codec ? codec.encode(value) : value;
}
