import {test, expect} from '@jest/globals';
import {versionToLexi, versionFromLexi} from './lexi-version.js';
import {compareVersions} from './version.js';

test('LexiVersion encoding', () => {
  type Case = [number, string];
  const cases: Case[] = [
    [0, '00'],
    [10, '0a'],
    [35, '0z'],
    [36, '110'],
    [46655, '2zzz'],
    [2 ** 32, '61z141z4'],
    [Number.MAX_SAFE_INTEGER, 'a2gosa7pa2gv'],
  ];
  for (const [num, lexi] of cases) {
    expect(versionToLexi(num)).toBe(lexi);
    expect(versionFromLexi(lexi)).toBe(num);
  }
});

test('LexiVersion sorting', () => {
  // A few explicit tests.
  expect(versionToLexi(35).localeCompare(versionToLexi(36))).toBe(-1);
  expect(versionToLexi(36).localeCompare(versionToLexi(35))).toBe(1);
  expect(versionToLexi(1000).localeCompare(versionToLexi(9))).toBe(1);
  expect(versionToLexi(89).localeCompare(versionToLexi(1234))).toBe(-1);
  expect(versionToLexi(238).localeCompare(versionToLexi(238))).toBe(0);

  // Random fuzz tests.
  for (let i = 0; i < 50; i++) {
    const v1 = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
    const v2 = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);

    const lexiV1 = versionToLexi(v1);
    const lexiV2 = versionToLexi(v2);

    expect(compareVersions(v1, v2)).toEqual(lexiV1.localeCompare(lexiV2));
  }
});
