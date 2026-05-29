import {
  getCodec,
  type SchemaValue,
} from '../../../zero-types/src/schema-value.ts';
import type {SourceSchema} from './schema.ts';
import type {Entry, EntryList, Format, View} from './view.ts';

/**
 * Whether any column reachable from `schema` (including through
 * relationships) carries a codec. Memoized per schema so the common
 * codec-free case is a single `WeakMap` lookup, and so that the read/write
 * paths can take a zero-copy fast path when there are no codecs.
 */
const codecCache = new WeakMap<SourceSchema, boolean>();

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
 * Returns a decoded copy of `data` (a query result tree), running each
 * codec column's `decode` on its stored value. Subtrees that contain no
 * codecs are returned by reference (no copy). When the whole tree is
 * codec-free the input is returned unchanged.
 *
 * `null`/`undefined` values are passed through without invoking `decode`.
 */
export function decodeView(
  data: View,
  schema: SourceSchema,
  format: Format,
): View {
  if (data === undefined || !schemaHasCodecs(schema)) {
    return data;
  }
  if (format.singular) {
    return decodeEntry(data as Entry, schema, format);
  }
  return (data as EntryList).map(entry => decodeEntry(entry, schema, format));
}

function decodeEntry(
  entry: Entry,
  schema: SourceSchema,
  format: Format,
): Entry {
  const result: Record<string, unknown> = {};
  for (const key in entry) {
    const childFormat = format.relationships[key];
    if (childFormat !== undefined) {
      const childSchema = schema.relationships[key];
      result[key] = childSchema
        ? decodeView(entry[key] as View, childSchema, childFormat)
        : entry[key];
      continue;
    }

    const value = entry[key];
    const codec = isNullish(value) ? undefined : getCodec(schema.columns[key]);
    result[key] = codec ? codec.decode(value) : value;
  }
  return result as Entry;
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
    const codec = isNullish(value) ? undefined : getCodec(columns[key]);
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
  if (isNullish(value) || column === undefined) {
    return value;
  }
  const codec = getCodec(column);
  return codec ? codec.encode(value) : value;
}

function isNullish(value: unknown): boolean {
  return value === null || value === undefined;
}
