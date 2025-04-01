import {describe, expect, test} from 'vitest';
import {
  versionFromLexi,
  type LexiVersion,
} from '../../../types/lexi-version.ts';
import {
  fromBigInt,
  fromLexiVersion,
  toBigInt,
  toLexiVersion,
  type LSN,
} from './lsn.ts';

describe('lsn to/from LexiVersion', () => {
  type Case = [LSN, LexiVersion, bigint];
  const cases: Case[] = [
    ['0/0', '00', 0n],
    ['0/A', '0a', 10n],
    ['16/B374D848', '718sh0nk8', 97500059720n],
    ['FFFFFFFF/FFFFFFFF', 'c3w5e11264sgsf', 2n ** 64n - 1n],
  ];
  test.each(cases)('convert(%s <=> %s)', (lsn, lexi, ver) => {
    expect(toLexiVersion(lsn)).toBe(lexi);
    expect(toBigInt(lsn)).toBe(ver);
    expect(fromBigInt(ver)).toBe(lsn);
    expect(versionFromLexi(lexi).toString()).toBe(ver.toString());
    expect(fromLexiVersion(lexi)).toBe(lsn);
  });
});
