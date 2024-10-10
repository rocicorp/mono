import {h64WithReverse} from '../../../shared/src/h64-with-reverse.js';
import {stringify, type JSONValue} from './bigint-json.js';

export type ColumnType = {readonly typeOid: number};
export type RowKeyType = Readonly<Record<string, ColumnType>>;
export type RowKey = Readonly<Record<string, JSONValue>>;

export type RowID = Readonly<{schema: string; table: string; rowKey: RowKey}>;

// Aliased for documentation purposes when dealing with full rows vs row keys.
// The actual structure of the objects is the same.
export type RowType = RowKeyType;
export type RowValue = RowKey;

/**
 * Returns the `RowKey` such that key iteration produces a sorted sequence. If the
 * keys are already sorted, the input is returned as is.
 */
export function normalizedKeyOrder(rowKey: RowKey) {
  let last = '';
  for (const col in rowKey) {
    if (last > col) {
      return Object.fromEntries(
        Object.entries(rowKey).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      );
    }
    last = col;
  }
  // This case iterates over columns and avoids object allocations, which is
  // expected to be the common case (e.g. single column key).
  return rowKey;
}

/**
 * Returns a normalized string suitable for representing a row key in a form
 * that can be used as a Map key.
 */
export function rowKeyString(key: RowKey): string {
  return stringify(tuples(key));
}

function tuples(key: RowKey) {
  return Object.entries(normalizedKeyOrder(key)).flat();
}

const rowIDHashes = new WeakMap<RowID, string>();

/**
 * A RowIDHash is a 128-bit column-order-agnostic hash of the schema, table name, and
 * column name / value tuples of a row key. It serves as a compact identifier for
 * a row in the database that:
 *
 * * is guaranteed to fit within the constraints of the CVR store (Durable Object
 *   storage keys cannot exceed 2KiB)
 * * can be used to compactly encode (and lookup) the rows of query results for CVR
 *   bookkeeping.
 *
 * The hash is encoded in `base36`, with the maximum 128-bit value being 25 characters long.
 */
export function rowIDHash(id: RowID): string {
  let hash = rowIDHashes.get(id);
  if (hash) {
    return hash;
  }

  const str = stringify([id.schema, id.table, ...tuples(id.rowKey)]);
  hash = h64WithReverse(str);
  rowIDHashes.set(id, hash);
  return hash;
}
