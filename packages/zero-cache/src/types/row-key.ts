import type postgres from 'postgres';
import {stringify, type JSONValue} from './bigint-json.js';
import {h64} from './xxhash.js';

export type ColumnType = {readonly typeOid: number};
export type RowKeyType = Readonly<Record<string, ColumnType>>;
export type RowKey = Readonly<
  Record<string, postgres.SerializableParameter<JSONValue>>
>;

export type RowID = Readonly<{schema: string; table: string; rowKey: RowKey}>;

// Aliased for documentation purposes when dealing with full rows vs row keys.
// The actual structure of the objects is the same.
export type RowType = RowKeyType;
export type RowValue = RowKey;

/**
 * Returns a normalized string suitable for representing a row key in a form
 * that can be used as a Map key.
 */
export function rowKeyString(key: RowKey): string {
  return stringify(tuples(key));
}

function tuples(key: RowKey) {
  return Object.entries(key)
    .sort(([col1], [col2]) => (col1 < col2 ? -1 : col1 > col2 ? 1 : 0))
    .flat();
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

  // xxhash only computes 64-bit values. Run it on the forward and reverse string
  // to get better collision resistance.
  const forward = h64(str);
  const backward = h64(reverse(str));
  const full = (forward << 64n) + backward;
  hash = full.toString(36);
  rowIDHashes.set(id, hash);
  return hash;
}

function reverse(str: string): string {
  let reversed = '';
  for (let i = str.length - 1; i >= 0; i--) {
    reversed += str[i];
  }
  return reversed;
}
