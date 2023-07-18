import {expect, test} from '@jest/globals';
import {buildLookup, decode, encode} from './base-n.js';

test('it should encode base16', () => {
  const alphabet = '0123456789abcdef';
  expect(encode(0n, alphabet)).toBe('0');
  expect(encode(1n, alphabet)).toBe('1');
  expect(encode(9n, alphabet)).toBe('9');
  expect(encode(10n, alphabet)).toBe('a');
  expect(encode(15n, alphabet)).toBe('f');
  expect(encode(16n, alphabet)).toBe('10');
  expect(encode(31n, alphabet)).toBe('1f');
  expect(encode(32n, alphabet)).toBe('20');
});

test('it should decode base16', () => {
  const alphabet = '0123456789abcdef';
  const lookup = buildLookup(alphabet);
  expect(decode('0', 16n, lookup)).toBe(0x0n);
  expect(decode('1', 16n, lookup)).toBe(0x1n);
  expect(decode('9', 16n, lookup)).toBe(0x9n);
  expect(decode('a', 16n, lookup)).toBe(0xan);
  expect(decode('f', 16n, lookup)).toBe(0xfn);
  expect(decode('10', 16n, lookup)).toBe(0x10n);
  expect(decode('1f', 16n, lookup)).toBe(0x1fn);
  expect(decode('20', 16n, lookup)).toBe(0x20n);
});
