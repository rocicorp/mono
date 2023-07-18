const alphabet =
  '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

export function encode(n: bigint): string {
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
