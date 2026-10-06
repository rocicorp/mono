import {assert} from '../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../shared/src/bigint-json.ts';
import {getOrInsertComputed} from '../../../shared/src/map.ts';

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
  let empty = true;
  for (const col in rowKey) {
    empty = false;
    if (last > col) {
      const entries = Object.entries(rowKey).sort(([a], [b]) =>
        a < b ? -1 : a > b ? 1 : 0,
      );
      assert(entries.length > 0, 'empty row key');
      return Object.fromEntries(entries);
    }
    last = col;
  }
  assert(!empty, 'empty row key');
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
 * as a Map key.
 */
export function rowIDString(id: RowID): string {
  return getOrInsertComputed(rowIDStrings, id, rowIDStringUncached);
}

function rowIDStringUncached(id: RowID): string {
  return stringify([id.schema, id.table, ...tuples(id.rowKey)]);
}
