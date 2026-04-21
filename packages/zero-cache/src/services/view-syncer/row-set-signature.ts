import {h64} from '../../../../shared/src/hash.ts';
import {rowIDString} from '../../types/row-key.ts';
import type {RowID} from './schema/types.ts';

/**
 * The hash of a row ID used as the unit XOR'd into a query's
 * {@link rowSetSignature}. Includes schema + table + rowKey, so the hash is
 * unique across tables in the same query.
 */
export function rowIDSignatureUnit(id: RowID): bigint {
  return h64(rowIDString(id));
}

/**
 * Parses a hex-encoded signature back to its bigint form. Empty / undefined
 * is the identity (`0n`).
 */
export function parseSignature(hex: string | undefined | null): bigint {
  if (!hex) {
    return 0n;
  }
  return BigInt('0x' + hex);
}

/**
 * Serializes a bigint signature to lowercase hex. `0n` serializes to `'0'`.
 */
export function formatSignature(sig: bigint): string {
  return sig.toString(16);
}
