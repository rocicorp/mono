// Packs a map from small non-negative integers to booleans into bytes, two
// bits per key, four keys per byte. Key n uses the two bits at 2 * (n % 4) in
// byte floor(n / 4): the low bit says the key is present and the high bit is
// its value. Unlike a bit set, this tells a missing key apart from `false`.

import {assert} from './asserts.ts';

const PRESENT = 0b01;
const TRUE = 0b10;

function byteIndex(key: number): number {
  return key >> 2;
}

function shift(key: number): number {
  return (key & 3) * 2;
}

export function packBooleanMap(map: ReadonlyMap<number, boolean>): Uint8Array {
  let size = 0;
  for (const key of map.keys()) {
    assert(
      Number.isInteger(key) && key >= 0,
      'Key must be a non-negative integer',
    );
    size = Math.max(size, byteIndex(key) + 1);
  }
  const bytes = new Uint8Array(size);
  for (const [key, value] of map) {
    bytes[byteIndex(key)] |= (value ? PRESENT | TRUE : PRESENT) << shift(key);
  }
  return bytes;
}

/**
 * Reads `keys` from `bytes`. Keys not in `keys` are ignored, as is a value bit
 * without its present bit.
 */
export function unpackBooleanMap<K extends number>(
  bytes: Uint8Array,
  keys: Iterable<K>,
): Map<K, boolean> {
  const map = new Map<K, boolean>();
  for (const key of keys) {
    const bits = (bytes[byteIndex(key)] ?? 0) >> shift(key);
    if (bits & PRESENT) {
      map.set(key, (bits & TRUE) !== 0);
    }
  }
  return map;
}
