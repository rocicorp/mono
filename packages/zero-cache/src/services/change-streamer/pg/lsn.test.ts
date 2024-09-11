import {expect, test} from 'vitest';
import {
  versionFromLexi,
  type LexiVersion,
} from 'zero-cache/src/types/lexi-version.js';
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
  }
});

test('lsn to/from LexiVersion with offset', () => {
  expect(toLexiVersion('16/B374D848', 'commit')).toBe('718sh0nk8');
  expect(toLexiVersion('16/B374D848', 'begin')).toBe('718sh0nk9');
  expect(toLexiVersion('16/B374D848', 'insert')).toBe('718sh0nka');

  expect(fromLexiVersion('718sh0nk8', 'commit')).toBe('16/B374D848');
  expect(fromLexiVersion('718sh0nk8', 'begin')).toBe('16/B374D847');
  expect(fromLexiVersion('718sh0nk8', 'insert')).toBe('16/B374D846');
});
