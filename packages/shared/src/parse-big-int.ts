// Until there's BigInt.fromString(val, radix) ... https://github.com/tc39/proposal-number-fromstring
export function parseBigInt(val: string, radix: number): bigint {
  const base = BigInt(radix);
  let result = 0n;
  for (let i = 0; i < val.length; i++) {
    result *= base;
    result += BigInt(parseInt(val[i], radix));
  }
  return result;
}
