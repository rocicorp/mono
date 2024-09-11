import {expect, test} from 'vitest';
import {versionFromLexi, type LexiVersion} from './lexi-version.js';
import {fromLexiVersion, toLexiVersion, type LSN} from './lsn.js';

test('lsn to/from LexiVersion', () => {
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
    expect(fromLexiVersion(lexi)).toBe(lsn);

    if (ver > 0n) {
      for (const offset of [0, -1, 3, -7, 100]) {
        const offsetLexi = toLexiVersion(lsn, offset);
        expect(versionFromLexi(offsetLexi).toString()).toBe(
          (ver + BigInt(offset)).toString(),
        );
      }
    }
  }
});

test('lsn to/from LexiVersion with offset', () => {
  expect(toLexiVersion('16/B374D848')).toBe('718sh0nk8');
  expect(toLexiVersion('16/B374D848', -1)).toBe('718sh0nk7');
  expect(toLexiVersion('16/B374D848', -2)).toBe('718sh0nk6');
  expect(toLexiVersion('16/B374D848', 1)).toBe('718sh0nk9');
  expect(toLexiVersion('16/B374D848', 2)).toBe('718sh0nka');

  expect(fromLexiVersion('718sh0nk8')).toBe('16/B374D848');
  expect(fromLexiVersion('718sh0nk8', -1)).toBe('16/B374D847');
  expect(fromLexiVersion('718sh0nk8', -2)).toBe('16/B374D846');
  expect(fromLexiVersion('718sh0nk8', 1)).toBe('16/B374D849');
  expect(fromLexiVersion('718sh0nk8', 2)).toBe('16/B374D84A');
});
