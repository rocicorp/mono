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
 *
 * Note that the value type is parameterized as `V` so that this method can be used
 * for both (pg) RowKeys and LiteRowKeys.
 */
export function normalizedKeyOrder<V>(
  rowKey: Readonly<Record<string, V>>,
): Readonly<Record<string, V>> {
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

const rowIDStrings = new WeakMap<RowID, string>();

/**
 * A normalized string representation of a {@link RowID} suitable to use
 * as a Map key. Use {@link rowIDHash} if you need string keys of bounded
 * length.
 */
export function rowIDString(id: RowID): string {
  let val = rowIDStrings.get(id);
  if (val) {
    return val;
  }
  val = stringify([id.schema, id.table, ...tuples(id.rowKey)]);
  rowIDStrings.set(id, val);
  return val;
}
