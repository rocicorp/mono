import * as baseN from './base-n.js';

const alphabet = '0123456789abcdefghijklmnopqrstuv';

export function encode(n: bigint): string {
  return baseN.encode(n, alphabet);
}

const lookup = baseN.buildLookup(alphabet);
const base = BigInt(alphabet.length);

export function decode(s: string): bigint {
  return baseN.decode(s, base, lookup);
}
