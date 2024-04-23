import {expect, test} from 'vitest';
import {encode} from './base62.js';

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
