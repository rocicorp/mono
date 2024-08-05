import {randomUint64} from 'shared/src/random-uint64.js';

/**
 * Returns a random 18 character string encoded in base32 suitable as a client
 * ID.
 */
export function makeClientID(): string {
  const length = 18;
  const high = randomUint64();
  const low = randomUint64();
  const combined = (high << 64n) | low;
  return combined.toString(32).slice(-length).padStart(length, '0');
}
