import {expect, test} from 'vitest';
import {
  max,
  min,
  versionFromLexi,
  versionToLexi,
  type AtLeastOne,
  type LexiVersion,
} from './lexi-version.ts';

test('LexiVersion encoding', () => {
  type Case = [number | bigint, string];
  const cases: Case[] = [
    [0, '00'],
    [10, '0a'],
    [35, '0z'],
    [36, '110'],
    [46655, '2zzz'],
    [2 ** 32, '61z141z4'],
    [Number.MAX_SAFE_INTEGER, 'a2gosa7pa2gv'],
    [2n ** 64n, 'c3w5e11264sgsg'],
    [2n ** 75n, 'e65gym2kbgwjf668'],
    [2n ** 128n, 'of5lxx1zz5pnorynqglhzmsp34'],
    [2n ** 160n, 'utwj4yidkw7a8pn4g709kzmfoaol3x8g'],
    [2n ** 186n, 'zx6sp2h09v22524mnljo7dsm6cz9iehtq4xds'],
    [36n ** 36n - 1n, 'z'.repeat(37)],
  ];
  for (const [num, lexi] of cases) {
    expect(versionToLexi(num)).toBe(lexi);
    expect(versionFromLexi(lexi).toString()).toBe(num.toString());
  }
});

test('min max', () => {
  expect(min('01')).toBe('01');
  expect(max('01')).toBe('01');

  expect(min('01', '02')).toBe('01');
  expect(max('01', '02')).toBe('02');

  expect(min('01', '03', '02')).toBe('01');
  expect(min('02', '03', '01')).toBe('01');
  expect(max('01', '03', '02')).toBe('03');
  expect(max('02', '01', '03')).toBe('03');

  expect(min('04', '01', '03', '02')).toBe('01');
  expect(max('04', '01', '03', '02')).toBe('04');
  expect(min('04', '01', '03', '02', '00')).toBe('00');
  expect(max('04', '01', '03', '02', '05')).toBe('05');

  const array = ['02', '04', '01', '03', '02'];
  expect(min(...(array as AtLeastOne<LexiVersion>))).toBe('01');
  expect(max(...(array as AtLeastOne<LexiVersion>))).toBe('04');
});

test('LexiVersion sorting', () => {
  // A few explicit tests.
  expect(versionToLexi(35).localeCompare(versionToLexi(36))).toBe(-1);
  expect(versionToLexi(36).localeCompare(versionToLexi(35))).toBe(1);
  expect(min(versionToLexi(36), versionToLexi(35), versionToLexi(37))).toBe(
    versionToLexi(35),
  );
  expect(max(versionToLexi(34), versionToLexi(36), versionToLexi(35))).toBe(
    versionToLexi(36),
  );

  expect(versionToLexi(1000).localeCompare(versionToLexi(9))).toBe(1);
  expect(min(versionToLexi(1000), versionToLexi(9))).toBe(versionToLexi(9));
  expect(max(versionToLexi(1000), versionToLexi(9))).toBe(versionToLexi(1000));

  expect(versionToLexi(89).localeCompare(versionToLexi(1234))).toBe(-1);
  expect(min(versionToLexi(89), versionToLexi(1234))).toBe(versionToLexi(89));
  expect(max(versionToLexi(89), versionToLexi(1234))).toBe(versionToLexi(1234));

  expect(versionToLexi(238).localeCompare(versionToLexi(238))).toBe(0);
  expect(min(versionToLexi(238), versionToLexi(238))).toBe(versionToLexi(238));
  expect(max(versionToLexi(238), versionToLexi(238))).toBe(versionToLexi(238));

  const cmp = (a: number, b: number) =>
    a === b ? 0 : (a - b) / Math.abs(a - b);

  // Random fuzz tests.
  for (let i = 0; i < 50; i++) {
    const v1 = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
    const v2 = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);

    const lexiV1 = versionToLexi(v1);
    const lexiV2 = versionToLexi(v2);

    expect(cmp(v1, v2)).toEqual(lexiV1.localeCompare(lexiV2));
    expect(versionToLexi(Math.min(v1, v2))).toBe(min(lexiV1, lexiV2));
    expect(versionToLexi(Math.min(v1, v2))).toBe(min(lexiV2, lexiV1));
    expect(versionToLexi(Math.max(v1, v2))).toBe(max(lexiV1, lexiV2));
    expect(versionToLexi(Math.max(v1, v2))).toBe(max(lexiV2, lexiV1));
  }
});

test('LexiVersion encode sanity checks', () => {
  for (const badVersion of [
    -1, // negative
    0.5, // decimal
    Number.MAX_SAFE_INTEGER * 2, // not safe
    2n ** 187n, // Too large
  ]) {
    expect(() => versionToLexi(badVersion)).toThrowError();
  }
});

test('LexiVersion decode sanity checks', () => {
  for (const badVersion of [
    'not a ! number',
    '0', // too short
    '20', // length too long
    '3cis1k', // length too short
  ]) {
    expect(() => versionFromLexi(badVersion)).toThrowError();
  }
});
