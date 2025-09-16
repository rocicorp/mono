/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test} from 'vitest';
import {getSizeOfEntry, getSizeOfValue} from './size-of-value.ts';

test('getSizeOfValue', () => {
  expect(getSizeOfValue(null)).to.equal(1);
  expect(getSizeOfValue(true)).to.equal(1);
  expect(getSizeOfValue(false)).to.equal(1);

  expect(getSizeOfValue('')).to.equal(5);
  expect(getSizeOfValue('abc')).to.equal(8);

  expect(getSizeOfValue(0)).to.equal(5);
  expect(getSizeOfValue(42)).to.equal(5);
  expect(getSizeOfValue(-42)).to.equal(5);

  expect(getSizeOfValue(2 ** 7 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 7 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 7)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 7))).to.equal(5);

  expect(getSizeOfValue(2 ** 14 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 14 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 14)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 14))).to.equal(5);

  expect(getSizeOfValue(2 ** 21 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 21 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 21)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 21))).to.equal(5);

  expect(getSizeOfValue(2 ** 28 - 1)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 28 - 1))).to.equal(5);
  expect(getSizeOfValue(2 ** 28)).to.equal(5);
  expect(getSizeOfValue(-(2 ** 28))).to.equal(5);

  expect(getSizeOfValue(2 ** 31 - 1)).to.equal(6);
  expect(getSizeOfValue(-(2 ** 31))).to.equal(6);
  expect(getSizeOfValue(2 ** 31)).to.equal(9); // not smi
  expect(getSizeOfValue(-(2 ** 31) - 1)).to.equal(9); // not smi

  expect(getSizeOfValue(0.1)).to.equal(9);

  expect(getSizeOfValue([])).to.equal(1 + 5);
  expect(getSizeOfValue([0])).to.equal(6 + 5);
  expect(getSizeOfValue(['abc'])).to.equal(1 + 4 + 8 + 1);
  expect(getSizeOfValue([0, 1, 2])).to.equal(1 + 4 + 3 * 5 + 1);
  expect(getSizeOfValue([null, true, false])).to.equal(1 + 4 + 3 * 1 + 1);

  expect(getSizeOfValue({})).to.equal(1 + 4 + 1);
  expect(getSizeOfValue({abc: 'def'})).to.equal(1 + 4 + 8 + 8 + 1);

  // Object with undefined property values.
  expect(getSizeOfValue({a: undefined, b: 1})).to.equal(getSizeOfValue({b: 1}));
});

test('getSizeOfEntry', () => {
  const t = (key: unknown, value: unknown) => {
    expect(getSizeOfEntry(key, value)).to.equal(
      getSizeOfValue([key, value, 1234]),
    );
  };

  t('a', 1);
  t('a', 'b');
  t('a', true);
  t('a', false);
  t('aa', []);
});
