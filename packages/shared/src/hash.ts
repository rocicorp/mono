import {xxHash32} from 'js-xxhash';

export const h32 = (s: string) => xxHash32(s, 0);
export const h64 = (s: string) => hash(s, 2);
export const h128 = (s: string) => hash(s, 4);

/**
 * xxHash32 only computes 32-bit values. Run it n times with different seeds to
 * get a larger hash with better collision resistance.
 */
function hash(str: string, words: number): bigint {
  let hash = 0n;
  for (let i = 0; i < words; i++) {
    hash = (hash << 32n) + BigInt(xxHash32(str, i));
  }
  return hash;
}
