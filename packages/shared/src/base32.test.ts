import {expect, test} from '@jest/globals';
import {decode, encode} from './base32.js';

test('it should encode base32', () => {
  expect(encode(0n)).toBe('0');
  expect(encode(1n)).toBe('1');
  expect(encode(9n)).toBe('9');
  expect(encode(10n)).toBe('a');
  expect(encode(31n)).toBe('v');
  expect(encode(32n)).toBe('10');
  expect(encode(33n)).toBe('11');
  expect(encode(63n)).toBe('1v');
  expect(encode(2n ** 31n - 1n)).toBe('1vvvvvv');
  expect(encode(0x7fff_ffffn)).toBe('1vvvvvv');
  expect(encode(2n ** 31n)).toBe('2000000');
  expect(encode(2n ** 64n - 1n)).toBe('fvvvvvvvvvvvv');
  expect(encode(0xffff_ffff_ffff_ffffn)).toBe('fvvvvvvvvvvvv');
});

test('it should decode base32', () => {
  expect(decode('0')).toBe(0n);
  expect(decode('1')).toBe(1n);
  expect(decode('9')).toBe(9n);
  expect(decode('a')).toBe(10n);
  expect(decode('v')).toBe(31n);
  expect(decode('10')).toBe(32n);
  expect(decode('11')).toBe(33n);
  expect(decode('1v')).toBe(63n);
  expect(decode('1vvvvvv')).toBe(2n ** 31n - 1n);
  expect(decode('1vvvvvv')).toBe(0x7fff_ffffn);
  expect(decode('2000000')).toBe(2n ** 31n);
  expect(decode('fvvvvvvvvvvvv')).toBe(2n ** 64n - 1n);
  expect(decode('fvvvvvvvvvvvv')).toBe(0xffff_ffff_ffff_ffffn);
});
