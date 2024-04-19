import {compareUTF8} from 'compare-utf8';
import type postgres from 'postgres';
import xxh from 'xxhashjs'; // TODO: Use xxhash-wasm
import {stringify} from './bigint-json.js';

export type ColumnType = {typeOid: number};
export type RowKeyType = Record<string, ColumnType>;
export type RowKey = Record<string, postgres.SerializableParameter>;

// Aliased for documentation purposes when dealing with full rows vs row keys.
// The actual structure of the objects is the same.
export type RowType = RowKeyType;
export type RowValue = RowKey;

/**
 * Returns a normalized string suitable for representing a row key in a form
 * that can be used as a Map key.
 */
export function rowKeyString(key: RowKey): string {
  const tuples = Object.entries(key)
    .sort(([col1], [col2]) => compareUTF8(col1, col2))
    .flat();

  // xxhash only computes 64-bit values. Run it on the forward and reverse string
  // to get better collision resistance.
  return stringify(tuples);
}

/**
 * A RowKeyHash is a 128-bit order-agnostic hash of the column name / value tuples of a
 * row key. It serves as a compact representation of the primary key of a row that:
 *
 * * is guaranteed to fit within the constraints of the CVR store (Durable Object
 *   storage keys cannot exceed 2KiB)
 *
 * * can be used for efficient multiple-key-column OR queries using
 *   `WHERE (col1, col2, ...) IN ((...), (...))` queries.
 *
 * The hash is encoded in `base64url`, with the maximum 128-bit value being 22 characters long.
 */
export function rowKeyHash(key: RowKey): string {
  const str = rowKeyString(key);

  // xxhash only computes 64-bit values. Run it on the forward and reverse string
  // to get better collision resistance.
  const forward = BigInt(xxh.h64().update(str).digest().toString());
  const backward = BigInt(xxh.h64().update(reverse(str)).digest().toString());
  const full = (forward << 64n) + backward;
  return Buffer.from(full.toString(16), 'hex').toString('base64url');
}

function reverse(str: string): string {
  let reversed = '';
  for (let i = str.length - 1; i >= 0; i--) {
    reversed += str[i];
  }
  return reversed;
}
