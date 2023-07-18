import {expect, test} from '@jest/globals';
import {decode, encode} from './base62.js';

test('it should encode base62', () => {
  expect(encode(0n)).toBe('0');
  expect(encode(1n)).toBe('1');
  expect(encode(9n)).toBe('9');
  expect(encode(10n)).toBe('A');
  expect(encode(35n)).toBe('Z');
  expect(encode(36n)).toBe('a');
  expect(encode(61n)).toBe('z');
  expect(encode(62n)).toBe('10');
  expect(encode(2n ** 31n - 1n)).toBe('2LKcb1');
  expect(encode(0x7fff_ffffn)).toBe('2LKcb1');
  expect(encode(2n ** 64n - 1n)).toBe('LygHa16AHYF');
  expect(encode(0xffff_ffff_ffff_ffffn)).toBe('LygHa16AHYF');
});

test('it should encode base62', () => {
  expect(decode('0')).toBe(0n);
  expect(decode('1')).toBe(1n);
  expect(decode('9')).toBe(9n);
  expect(decode('A')).toBe(10n);
  expect(decode('Z')).toBe(35n);
  expect(decode('a')).toBe(36n);
  expect(decode('z')).toBe(61n);
  expect(decode('10')).toBe(62n);
  expect(decode('2LKcb1')).toBe(2n ** 31n - 1n);
  expect(decode('2LKcb1')).toBe(0x7fff_ffffn);
  expect(decode('LygHa16AHYF')).toBe(2n ** 64n - 1n);
  expect(decode('LygHa16AHYF')).toBe(0xffff_ffff_ffff_ffffn);
});
