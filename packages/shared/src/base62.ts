const characters =
  '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

export function encode(n: bigint): string {
  if (n === 0n) {
    return '0';
  }
  let result = '';
  while (n > 0n) {
    result = characters[Number(n % 62n)] + result;
    n = n / 62n;
  }
  return result;
}
