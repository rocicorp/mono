import {assert} from './asserts.js';

export function encode(n: bigint, alphabet: string): string {
  if (n === 0n) {
    return '0';
  }
  let result = '';
  const base = BigInt(alphabet.length);
  while (n > 0n) {
    result = alphabet[Number(n % base)] + result;
    n = n / base;
  }
  return result;
}

export function buildLookup(alphabet: string): Uint8Array {
  assert(alphabet.length < 256, 'Alphabet too long');
  const lookup = new Uint8Array(256);
  for (let i = 0; i < alphabet.length; i++) {
    const charCode = alphabet.charCodeAt(i);
    lookup[charCode] = i;
  }
  return lookup;
}

/**
 * The reverse of encode
 */
export function decode(s: string, base: bigint, lookup: Uint8Array): bigint {
  assert(s.length > 0, 'Empty string');
  let result = 0n;
  for (let i = 0; i < s.length; i++) {
    const num = lookup[s.charCodeAt(i)];
    result = result * base + BigInt(num);
  }
  return result;
}
