import {compareUTF8} from 'compare-utf8';
import * as v from 'shared/src/valita.js';
import XXH from 'xxhashjs'; // TODO: Use xxhash-wasm

/**
 * Encodes the operations over filtered columns for which invalidation
 * is supported. Currently, only value equality is supported, but more
 * complex operations such as array containment or JSON functions may
 * be added in the future.
 */
export const filterOperationSchema = v.literal('=');

const rawFilterSpecSchema = v.object({
  schema: v.readonly(v.string()),
  table: v.readonly(v.string()),

  /**
   * Encodes one or more AND'ed column filters. Each column name is mapped
   * to the corresponding filter operation in the query. Currently, only the
   * `=` operation is supported, but this can be extended in the future to
   * support other operations such as array containment or JSON operations.
   *
   * For example, the query:
   *
   * ```
   * SELECT ... FROM table WHERE col1 = ... AND col2 = ...
   * ```
   *
   * is encoded as `filteredColumns: {col1: '=', col2: '='}`
   *
   * An `OR` query should be represented by multiple filter specs, one for
   * each side of the `OR` operator. For example:
   *
   * ```
   * SELECT ... FROM table WHERE col1 = ... OR (col2 = ... AND col3 = ...)
   * ```
   *
   * is encoded as two filter specs:
   *
   * `filteredColumns: {col1: '='}`
   * `filteredColumns: {col2: '=', col3: '='}`
   *
   * Note that this means that nested `OR` trees can result in 2^(num-ORs)
   * filter specs. To avoid filter spec blowup, the application should cap the
   * number of filter specs produced by a query to a limit and fall back to
   * a flat list of single column filter specs when the limit is exceeded.
   * (i.e. conservatively representing the filter expression as an OR of the
   *  transitive set of filtered columns).
   */
  filteredColumns: v.readonlyRecord(filterOperationSchema),

  /**
   * The columns selected by the query, or absent for `SELECT *`. This is used
   * to suppress invalidation tags for UPDATE changes in which none of the
   * filtered, selected, or key columns changed.
   */
  selectedColumns: v.readonlyArray(v.string()).optional(),
});

export type InvalidationFilterSpec = v.Infer<typeof rawFilterSpecSchema>;

/**
 * The NormalizedInvalidationFilterSpec uses a deterministic sorting order of
 * object keys such that all semantically equivalent specs have an identical
 * stringified representation. An additional `id` hash is added to uniquely
 * identify the spec.
 */
export const normalizedFilterSpecSchema =
  rawFilterSpecSchema.map(normalizeFilterSpec);

export type NormalizedInvalidationFilterSpec = v.Infer<
  typeof normalizedFilterSpecSchema
>;

export function normalizeFilterSpec(val: InvalidationFilterSpec) {
  // Normalized field ordering.
  const normalized: InvalidationFilterSpec = {
    schema: val.schema,
    table: val.table,
    filteredColumns: Object.fromEntries(
      Object.entries(val.filteredColumns).sort(([a], [b]) => compareUTF8(a, b)),
    ),
  };
  if (val.selectedColumns) {
    normalized.selectedColumns = [...val.selectedColumns].sort(compareUTF8);
  }
  return {
    id: XXH.h64(SEED).update(JSON.stringify(normalized)).digest().toString(36),
    ...normalized,
  };
}

export function parseFilterSpec(
  spec: unknown,
): NormalizedInvalidationFilterSpec {
  return v.parse(spec, normalizedFilterSpecSchema, 'passthrough');
}

/**
 * Any change that affects a table produces a Table Tag that signifies that
 * "something in the table changed". This is a catch-all for queries that do not
 * otherwise have a more specific filter spec.
 */
const tableTagSchema = v.object({
  schema: v.readonly(v.string()),
  table: v.readonly(v.string()),
});

export type TableTag = v.Infer<typeof tableTagSchema>;

/**
 * The Full Table Tag signifies that "everything in the table changed". This is
 * used to represent table truncation. All queries must be checked against their
 * respective Full Table Tag.
 */
const fullTableTagSchema = v.object({
  ...tableTagSchema.shape,

  allRows: v.literal(true),
});

export type FullTableTag = v.Infer<typeof fullTableTagSchema>;

/**
 * A Row Tag represents a specific Filter Spec's view of a change to a row.
 * For each changed row, a Row Tag is produced for the `filteredColumns` of
 * every registered Filter Spec for that table.
 */
const rowTagSchema = v.object({
  ...tableTagSchema.shape,

  /**
   * The names and JSON-encoded values of the `filteredColumns` of the
   * InvalidationFilterSpec, i.e. `{[columnName]: [bigintJsonStringifiedValue]}`.
   */
  filteredColumns: v.readonlyRecord(v.string()),

  /**
   * The `selectedColumns` array from the spec. This is used to distinguish
   * invalidation tags for specs that have the same filtered columns but different
   * selected columns.
   */
  selectedColumns: v.readonlyArray(v.string()).optional(),
});

export type RowTag = v.Infer<typeof rowTagSchema>;

export type InvalidationTag = TableTag | FullTableTag | RowTag;

const SEED = 0x1234567890;

/**
 * @returns The hex-encoded invalidation hash for the given `tag`.
 */
export function invalidationHash(tag: InvalidationTag): string {
  const hasher = XXH.h64().init(SEED).update(tag.schema).update(tag.table);

  if ('allRows' in tag) {
    // FullTableTag
    hasher.update('|allRows|');
  } else {
    if ('filteredColumns' in tag) {
      // RowTag
      const filteredColumns = Object.entries(tag.filteredColumns);
      if (filteredColumns.length) {
        hasher.update('|filteredColumns|'); // Delimiter
        filteredColumns
          .sort(([a], [b]) => compareUTF8(a, b))
          .forEach(([col, val]) => {
            hasher.update(col).update(val);
          });
      }
    }
    if ('selectedColumns' in tag && tag.selectedColumns) {
      hasher.update('|selectedColumns|'); // Delimiter
      [...tag.selectedColumns]
        .sort(compareUTF8)
        .forEach(col => hasher.update(col));
    }
  }
  const hex = hasher.digest().toString(16);
  // Pad to whole byte lengths to ensure roundtrip integrity when
  // serialized to/from BYTEA / Buffer.
  return hex.length % 2 ? '0' + hex : hex;
}
