import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import {
  getCodec,
  type SchemaValue,
} from '../../../zero-types/src/schema-value.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';

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
export function decodeRowFields(
  row: Row,
  columns: Record<string, SchemaValue>,
): Row {
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
 * Decodes the codec columns of a fully materialized query result (the shape
 * produced by `tx.run()` / `query.run()`: a row or an array of rows, with
 * related rows nested under their relationship alias). The IVM views decode
 * as entries are built, but results that come straight from SQL (the server
 * side `tx.run()`) never pass through a view, so this walks the result tree
 * using the query's AST and format instead. Returns the input unchanged when
 * no table in the tree carries a codec.
 */
export function decodeQueryResult(
  result: unknown,
  ast: AST,
  format: Format,
  schema: Schema,
): unknown {
  // oxlint-disable-next-line eqeqeq
  if (result == null) {
    return result;
  }
  if (Array.isArray(result)) {
    let copy: unknown[] | undefined;
    for (let i = 0; i < result.length; i++) {
      const decoded = decodeQueryResult(result[i], ast, format, schema);
      if (decoded !== result[i]) {
        copy ??= [...result];
        copy[i] = decoded;
      }
    }
    return copy ?? result;
  }

  const columns = schema.tables[ast.table]?.columns;
  const row = result as Row;
  let decoded: Record<string, unknown> | undefined =
    columns && columnsHaveCodecs(columns)
      ? (decodeRowFields(row, columns) as Record<string, unknown>)
      : undefined;
  if (decoded === row) {
    decoded = undefined;
  }

  for (const related of ast.related ?? []) {
    const alias = related.subquery.alias;
    if (alias === undefined || !(alias in row)) {
      continue;
    }
    const childFormat = format.relationships[alias];
    if (childFormat === undefined) {
      continue;
    }
    // A hidden relationship is a junction edge: the materialized rows under
    // `alias` belong to the far table, i.e. the nested subquery.
    const childAST = related.hidden
      ? related.subquery.related?.[0]?.subquery
      : related.subquery;
    if (childAST === undefined) {
      continue;
    }
    const child = decodeQueryResult(row[alias], childAST, childFormat, schema);
    if (child !== row[alias]) {
      decoded ??= {...row};
      decoded[alias] = child;
    }
  }
  return decoded ?? row;
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
