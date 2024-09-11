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
    ['0/A', '114', 10n],
    ['16/B374D848', '74z5w2m8w', 97500059720n],
    ['FFFFFFFF/FFFFFFFF', 'cfklk448oj5v5o', 2n ** 64n - 1n],
  ];
  for (const [lsn, lexi, ver] of cases) {
    expect(toLexiVersion(lsn)).toBe(lexi);
    expect(versionFromLexi(lexi).toString()).toBe((ver << 2n).toString());
    expect(fromLexiVersion(lexi)).toBe(lsn);
  }
});

test('lsn to/from LexiVersion with offset', () => {
  expect(toLexiVersion('16/B374D848', 'commit')).toBe('74z5w2m8w');
  expect(toLexiVersion('16/B374D848', 'begin')).toBe('74z5w2m8x');
  expect(toLexiVersion('16/B374D848', 'insert')).toBe('74z5w2m8y');

  expect(fromLexiVersion('74z5w2m8w')).toBe('16/B374D848');
  expect(fromLexiVersion('74z5w2m8x')).toBe('16/B374D848');
  expect(fromLexiVersion('74z5w2m8y')).toBe('16/B374D848');
});
