import {getRandomValues} from './random-values.ts';

export function randomUint64(): bigint {
  // Two random 32-bit words rather than a BigUint64Array, which Hermes did
  // not have as of September 2024. The result is the same either way.
  const [high, low] = getRandomValues(new Uint32Array(2));
  return (BigInt(high) << 32n) | BigInt(low);
}
