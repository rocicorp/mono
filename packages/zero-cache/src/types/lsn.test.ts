import {expect, test} from 'vitest';
import {versionFromLexi, type LexiVersion} from './lexi-version.js';
import {toLexiVersion, type LSN} from './lsn.js';

test('lsn to LexiVersion', () => {
  type Case = [LSN, LexiVersion, bigint];
  const cases: Case[] = [
    ['0/0', '00', 0n],
    ['0/A', '0a', 10n],
    ['16/B374D848', '718sh0nk8', 97500059720n],
    ['FFFFFFFF/FFFFFFFF', 'c3w5e11264sgsf', 2n ** 64n - 1n],
  ];
  for (const [lsn, lexi, ver] of cases) {
    expect(toLexiVersion(lsn)).toBe(lexi);
    expect(versionFromLexi(lexi).toString()).toBe(ver.toString());
  }
});
