import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import {
  getCodec,
  type Codec,
  type SchemaValue,
} from '../../../zero-types/src/schema-value.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';

type CodecColumn = readonly [name: string, codec: Codec<unknown, unknown>];

/**
 * The codec columns of a `columns` record, memoized per record object. Column
 * records are long-lived (they belong to the schema), and the read/write paths
 * consult this once per row, so the common codec-free case is a single
 * `WeakMap` lookup rather than a scan of every column, and the codec case only
 * visits the codec columns instead of every key of every row.
 */
const codecColumnsCache = new WeakMap<
  Record<string, SchemaValue>,
  readonly CodecColumn[]
>();

function codecColumnsOf(
  columns: Record<string, SchemaValue>,
): readonly CodecColumn[] {
  let result = codecColumnsCache.get(columns);
  if (result === undefined) {
    const found: CodecColumn[] = [];
    for (const name in columns) {
      const codec = getCodec(columns[name]);
      if (codec !== undefined) {
        found.push([name, codec]);
      }
    }
    result = found;
    codecColumnsCache.set(columns, result);
  }
  return result;
}

/** Returns `true` if any column in `columns` carries a codec. */
export function columnsHaveCodecs(
  columns: Record<string, SchemaValue>,
): boolean {
  return codecColumnsOf(columns).length > 0;
}

/**
 * Returns a copy of `row` with `direction` of each codec column's codec
 * applied to its value, or the input unchanged when no codec column of `row`
 * holds a value. `null`/`undefined` values are passed through without invoking
 * the codec.
 */
function mapCodecColumns<T extends Record<string, unknown>>(
  row: T,
  columns: Record<string, SchemaValue>,
  direction: 'decode' | 'encode',
): T {
  let result: Record<string, unknown> | undefined;
  for (const [name, codec] of codecColumnsOf(columns)) {
    const value = row[name];
    // oxlint-disable-next-line eqeqeq
    if (value != null) {
      result ??= {...row};
      result[name] = codec[direction](value);
    }
  }
  return (result ?? row) as T;
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
  return mapCodecColumns(row, columns, 'decode');
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
  return mapCodecColumns(row, columns, 'encode');
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

  const row = result as Row;
  const columns = schema.tables[ast.table]?.columns;
  const decodedRow = columns ? decodeRowFields(row, columns) : row;
  // `decoded` is only set once a copy has been made.
  let decoded: Record<string, unknown> | undefined =
    decodedRow === row ? undefined : (decodedRow as Record<string, unknown>);

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
