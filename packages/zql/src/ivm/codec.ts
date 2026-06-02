import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  getCodec,
  type SchemaValue,
} from '../../../zero-types/src/schema-value.ts';
import type {SourceSchema} from './schema.ts';

/**
 * Whether any column reachable from `schema` (including through
 * relationships) carries a codec. Memoized per schema so the common
 * codec-free case is a single `WeakMap` lookup, and so that the read/write
 * paths can take a zero-copy fast path when there are no codecs.
 */
const codecCache = new WeakMap<SourceSchema, boolean>();

/**
 * Returns `true` if any column reachable from `schema` (including through
 * relationships) carries a codec. Result is memoized per schema object so the
 * common codec-free case is a single WeakMap lookup.
 */
export function schemaHasCodecs(schema: SourceSchema): boolean {
  return computeSchemaHasCodecs(schema, new Set());
}

function computeSchemaHasCodecs(
  schema: SourceSchema,
  visiting: Set<SourceSchema>,
): boolean {
  const cached = codecCache.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  // Guard against cyclic (e.g. self-referential) relationships.
  if (visiting.has(schema)) {
    return false;
  }
  visiting.add(schema);

  let result = columnsHaveCodecs(schema.columns);
  if (!result) {
    for (const child of Object.values(schema.relationships)) {
      if (computeSchemaHasCodecs(child, visiting)) {
        result = true;
        break;
      }
    }
  }

  visiting.delete(schema);
  codecCache.set(schema, result);
  return result;
}

/** Returns `true` if any column in `columns` carries a codec. */
export function columnsHaveCodecs(
  columns: Record<string, SchemaValue>,
): boolean {
  for (const name in columns) {
    if (getCodec(columns[name]) !== undefined) {
      return true;
    }
  }
  return false;
}

/**
 * Returns a copy of `row` with each codec column's `decode` applied. Used on
 * the read path when entries are inserted into the view so consumers see
 * app-typed values (e.g. `Date`) instead of the raw stored values. Returns
 * the input unchanged when no column carries a codec.
 *
 * `null`/`undefined` values are passed through without invoking `decode`.
 */
export function decodeRowFields(row: Row, schema: SourceSchema): Row {
  if (!columnsHaveCodecs(schema.columns)) {
    return row;
  }
  let result: Record<string, unknown> | undefined;
  for (const key in row) {
    const value = row[key];
    // oxlint-disable-next-line eqeqeq
    const codec = value == null ? undefined : getCodec(schema.columns[key]);
    if (codec) {
      if (result === undefined) {
        result = {...row};
      }
      result[key] = codec.decode(value as never);
    }
  }
  return (result ?? row) as Row;
}

/**
 * Returns a copy of `row` with each codec column's `encode` applied. Used on
 * the write path (insert/update) and for `where` literals so that everything
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
export function encodeValue(value: unknown, column: SchemaValue): unknown {
  // oxlint-disable-next-line eqeqeq
  if (value == null) {
    return value;
  }
  const codec = getCodec(column);
  return codec ? codec.encode(value) : value;
}
