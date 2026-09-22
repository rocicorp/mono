import fc from 'fast-check';
import {xxHash32} from 'js-xxhash';
import {expect, test} from 'vitest';
import {h128, h32, h64} from './hash.ts';

/**
 * The original implementation: fold `words` independent `xxHash32` passes over
 * the string. `hash.ts` now does this in a single pass over a single UTF-8
 * encoding; these tests pin the output to the original.
 */
function referenceHash(str: string, words: number): bigint {
  let hash = 0n;
  for (let i = 0; i < words; i++) {
    hash = (hash << 32n) + BigInt(xxHash32(str, i));
  }
  return hash;
}

const INTERESTING = [
  '',
  'a',
  'abc',
  '0123456789abcde', // 15 bytes: no stripe
  '0123456789abcdef', // 16 bytes: exactly one stripe
  '0123456789abcdefg', // stripe + tail
  '0123456789abcdefghijklmnopqrstu', // stripe + 4-byte lanes + tail
  'ä'.repeat(17), // 2-byte UTF-8
  '€'.repeat(11), // 3-byte UTF-8
  '😀'.repeat(7), // surrogate pairs
  'a\0b', // embedded NUL
  JSON.stringify({table: 'issue', where: null, limit: 100}),
  'x'.repeat(1000),
];

test('h32 is xxHash32 with seed 0', () => {
  for (const s of INTERESTING) {
    expect(h32(s)).toBe(xxHash32(s, 0));
  }
});

test('h64 and h128 match the multi-pass reference', () => {
  for (const s of INTERESTING) {
    expect(h64(s)).toBe(referenceHash(s, 2));
    expect(h128(s)).toBe(referenceHash(s, 4));
  }
});

test('all three match the reference for arbitrary strings', () => {
  fc.assert(
    fc.property(fc.string({maxLength: 300}), s => {
      expect(h32(s)).toBe(xxHash32(s, 0));
      expect(h64(s)).toBe(referenceHash(s, 2));
      expect(h128(s)).toBe(referenceHash(s, 4));
    }),
    {numRuns: 500},
  );
});

test('all three match the reference for arbitrary unicode', () => {
  fc.assert(
    fc.property(fc.fullUnicodeString({maxLength: 300}), s => {
      expect(h32(s)).toBe(xxHash32(s, 0));
      expect(h64(s)).toBe(referenceHash(s, 2));
      expect(h128(s)).toBe(referenceHash(s, 4));
    }),
    {numRuns: 500},
  );
});

test('successive calls do not share state', () => {
  const a = h128('some string');
  h64('a different string');
  h32('a third string');
  h128('yet another');
  expect(h128('some string')).toBe(a);
});
