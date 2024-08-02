import {randomUint64} from 'shared/src/random-uint64.js';

/**
 * Generates a random string with 64 bits entropy. The string is using base32
 * [0-9a-v] and is 13 characters long.
 */
export function makeRandomID(): string {
  return randomUint64().toString(32).padStart(13, '0');
}
