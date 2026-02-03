import {describe, expect, test} from 'vitest';
import {type LexiVersion} from '../../../types/lexi-version.ts';
import {
  fromBigInt,
  fromStateVersionString,
  toBigInt,
  toStateVersionString,
  type LSN,
} from './lsn.ts';

describe('lsn to/from LexiVersion', () => {
  type Case = [LSN, LexiVersion, bigint];
  const cases: Case[] = [
    ['0/0', '00', 0n],
    ['0/0', '00.01', 0n],
    ['0/A', '0a', 10n],
    ['16/B374D848', '718sh0nk8', 97500059720n],
    ['16/B374D848', '718sh0nk8.123', 97500059720n],
    ['FFFFFFFF/FFFFFFFF', 'c3w5e11264sgsf', 2n ** 64n - 1n],
  ];
  test.each(cases)('convert(%s <=> %s)', (lsn, str, ver) => {
    if (str.includes('.')) {
      expect(toStateVersionString(lsn)).toBe(
        str.substring(0, str.indexOf('.')),
      );
    } else {
      expect(toStateVersionString(lsn)).toBe(str);
    }
    expect(toBigInt(lsn)).toBe(ver);
    expect(fromBigInt(ver)).toBe(lsn);
    expect(fromStateVersionString(str)).toBe(lsn);
  });
});
