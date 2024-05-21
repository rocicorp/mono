import fc from 'fast-check';
import {expect, test} from 'vitest';
import {parseBigInt} from './parse-big-int.js';

const cases = [
  ['0', 10, 0n],
  ['1', 10, 1n],
  ['10', 10, 10n],
  ['10', 16, 16n],
  ['100', 10, 100n],
  ['100', 16, 256n],
  ['10', 36, 36n],
  ['100', 36, 1296n],
] as const;

test.each(cases)('parseBigInt(%s, %s, %d)', (s, radix, expected) => {
  const actual = parseBigInt(s, radix);
  expect(actual).toBe(expected);
});

test('random using fast check', () => {
  fc.assert(
    fc.property(
      fc.bigInt({min: 0n}),
      fc.integer({min: 2, max: 36}),
      (n, radix) => {
        const s = n.toString(radix);
        const actual = parseBigInt(s, radix);
        expect(actual).toBe(n);
      },
    ),
  );
});
