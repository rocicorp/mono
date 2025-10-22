import {expect, test} from 'vitest';
import {getSizeOfEntry, getSizeOfValue} from './size-of-value.ts';

test('getSizeOfValue', () => {
  expect(getSizeOfValue(null)).toBe(1);
  expect(getSizeOfValue(true)).toBe(1);
  expect(getSizeOfValue(false)).toBe(1);

  expect(getSizeOfValue('')).toBe(5);
  expect(getSizeOfValue('abc')).toBe(8);

  expect(getSizeOfValue(0)).toBe(5);
  expect(getSizeOfValue(42)).toBe(5);
  expect(getSizeOfValue(-42)).toBe(5);

  expect(getSizeOfValue(2 ** 7 - 1)).toBe(5);
  expect(getSizeOfValue(-(2 ** 7 - 1))).toBe(5);
  expect(getSizeOfValue(2 ** 7)).toBe(5);
  expect(getSizeOfValue(-(2 ** 7))).toBe(5);

  expect(getSizeOfValue(2 ** 14 - 1)).toBe(5);
  expect(getSizeOfValue(-(2 ** 14 - 1))).toBe(5);
  expect(getSizeOfValue(2 ** 14)).toBe(5);
  expect(getSizeOfValue(-(2 ** 14))).toBe(5);

  expect(getSizeOfValue(2 ** 21 - 1)).toBe(5);
  expect(getSizeOfValue(-(2 ** 21 - 1))).toBe(5);
  expect(getSizeOfValue(2 ** 21)).toBe(5);
  expect(getSizeOfValue(-(2 ** 21))).toBe(5);

  expect(getSizeOfValue(2 ** 28 - 1)).toBe(5);
  expect(getSizeOfValue(-(2 ** 28 - 1))).toBe(5);
  expect(getSizeOfValue(2 ** 28)).toBe(5);
  expect(getSizeOfValue(-(2 ** 28))).toBe(5);

  expect(getSizeOfValue(2 ** 31 - 1)).toBe(6);
  expect(getSizeOfValue(-(2 ** 31))).toBe(6);
  expect(getSizeOfValue(2 ** 31)).toBe(9); // not smi
  expect(getSizeOfValue(-(2 ** 31) - 1)).toBe(9); // not smi

  expect(getSizeOfValue(0.1)).toBe(9);

  expect(getSizeOfValue([])).toBe(1 + 5);
  expect(getSizeOfValue([0])).toBe(6 + 5);
  expect(getSizeOfValue(['abc'])).toBe(1 + 4 + 8 + 1);
  expect(getSizeOfValue([0, 1, 2])).toBe(1 + 4 + 3 * 5 + 1);
  expect(getSizeOfValue([null, true, false])).toBe(1 + 4 + 3 * 1 + 1);

  expect(getSizeOfValue({})).toBe(1 + 4 + 1);
  expect(getSizeOfValue({abc: 'def'})).toBe(1 + 4 + 8 + 8 + 1);

  // Object with undefined property values.
  expect(getSizeOfValue({a: undefined, b: 1})).toBe(getSizeOfValue({b: 1}));
});

test('getSizeOfEntry', () => {
  const t = (key: unknown, value: unknown) => {
    expect(getSizeOfEntry(key, value)).toBe(getSizeOfValue([key, value, 1234]));
  };

  t('a', 1);
  t('a', 'b');
  t('a', true);
  t('a', false);
  t('aa', []);
});
