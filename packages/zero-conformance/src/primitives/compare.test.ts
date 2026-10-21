import {test} from 'vitest';
import type {Value} from '../../../zero-protocol/src/data.ts';
import {compareValues, valuesEqual} from '../../../zql/src/ivm/data.ts';
import {interestingStrings, Random, randomString} from '../random.ts';
import {emitVectors} from '../vectors.ts';

const sign = (n: number) => (n < 0 ? -1 : n > 0 ? 1 : 0);

test('string-compare', () => {
  const r = new Random(0xc0de);
  const pairs: [string, string][] = [];
  for (const a of interestingStrings) {
    for (const b of interestingStrings) {
      pairs.push([a, b]);
    }
  }
  for (let i = 0; i < 200; i++) {
    pairs.push([randomString(r), randomString(r)]);
  }
  emitVectors(
    'string-compare',
    'string-compare',
    'compareUTF8 (code point order) and JS `<` (UTF-16 code unit order) signs.',
    pairs.map(([a, b]) => ({
      a,
      b,
      // compareValues compares strings with compare-utf8.
      utf8: sign(compareValues(a, b)),
      utf16: a < b ? -1 : a > b ? 1 : 0,
    })),
  );
});

test('compare-values', () => {
  const values: Value[] = [
    null,
    true,
    false,
    0,
    -0,
    1,
    -1,
    0.5,
    1e21,
    -1e-7,
    2 ** 53,
    '',
    'a',
    'B',
    'é',
    '\u{1f600}',
    '',
  ];
  const cases = [];
  for (const a of values) {
    for (const b of values) {
      let result: number | 'throws';
      try {
        result = sign(compareValues(a, b));
      } catch {
        result = 'throws';
      }
      cases.push({a, b, compare: result, equal: valuesEqual(a, b)});
    }
  }
  emitVectors(
    'compare-values',
    'compare-values',
    'zql compareValues (sign, or "throws" for mismatched types) and valuesEqual.',
    cases,
  );
});
