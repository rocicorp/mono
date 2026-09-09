/**
 * The two arithmetic steps at the core of xxHash32, shared by everything here
 * that builds a hash out of it.
 *
 * xxHash32 is defined over a byte stream, but its round and avalanche steps
 * are just 32-bit word arithmetic and are reusable on their own. `hash.ts`
 * feeds them UTF-8 bytes to produce digests bit-compatible with the reference
 * implementation; `zero-protocol`'s AST hash feeds them words taken straight
 * from a structure, never materializing bytes at all. Same mixing, different
 * ingestion.
 *
 * These constants and steps are frozen: both callers' digests are persisted or
 * compared against stored values, so changing anything here silently changes
 * hashes across the system. `hash.test.ts` pins the byte-oriented side against
 * the js-xxhash reference; `query-hash-visitor.test.ts` pins the word-oriented
 * side against a corpus.
 */

export const PRIME32_1 = 2654435761;
export const PRIME32_2 = 2246822519;
export const PRIME32_3 = 3266489917;
export const PRIME32_4 = 668265263;
export const PRIME32_5 = 374761393;

/**
 * One xxHash32 round: fold a 32-bit word into an accumulator.
 *
 * `Math.imul` is the whole multiply. Reference JS ports write it as two 16-bit
 * half-multiplies recombined; that computes the same low 32 bits by a longer
 * route, which is why moving to `Math.imul` left every digest unchanged.
 */
export function round32(acc: number, word: number): number {
  acc = (acc + Math.imul(word, PRIME32_2)) | 0;
  acc = (acc << 13) | (acc >>> 19);
  return Math.imul(acc, PRIME32_1);
}

/**
 * xxHash32's final mix, spreading every input bit across the whole word.
 * Returns an unsigned 32-bit number.
 */
export function avalanche32(acc: number): number {
  acc ^= acc >>> 15;
  acc = Math.imul(acc, PRIME32_2);
  acc ^= acc >>> 13;
  acc = Math.imul(acc, PRIME32_3);
  acc ^= acc >>> 16;
  return acc >>> 0;
}
