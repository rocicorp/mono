export function randomUint64(): bigint {
  return crypto.getRandomValues(new BigUint64Array(1))[0];
}
