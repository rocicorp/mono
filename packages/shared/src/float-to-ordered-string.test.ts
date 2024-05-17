import fc from 'fast-check';
import {expect, test} from 'vitest';
import {
  decodeFloat64AsString,
  encodeFloat64AsString,
} from './float-to-ordered-string.js';

const cases = [
  [-0, 'Uzzzzzzzzzw='],
  [0, 'V----------='],
  [1, 'jz---------='],
  [2, 'k----------='],
  [3, 'k-V--------='],
  [4, 'k0---------='],
  [-1, 'F-zzzzzzzzw='],
  [-2, 'Ezzzzzzzzzw='],
  [-3, 'EzUzzzzzzzw='],
  [-4, 'Eyzzzzzzzzw='],
  [3.141592653589793, 'k-ZWypG3AGV='],
  [NaN, 'zzV--------='],
  [NaN, 'zzV--------='],
  [Infinity, 'zz---------='],
  [-Infinity, '--zzzzzzzzw='],
  [Number.MAX_SAFE_INTEGER, 'knzzzzzzzzw='],
  [Number.MIN_SAFE_INTEGER, 'EB---------='],
  [Number.MIN_VALUE, 'V---------3='],
  [Number.MAX_VALUE, 'zyzzzzzzzzw='],
] as const;

const reversedCases = cases.map(([a, b]) => [b, a] as const);

test.each(cases)('encode %f -> %s', (n, expected) => {
  expect(encodeFloat64AsString(n)).toBe(expected);
});

test.each(reversedCases)('decode %s -> %f', (s, expected) => {
  expect(decodeFloat64AsString(s)).toBe(expected);
});

test('random with fast-check', () => {
  fc.assert(
    fc.property(fc.float(), fc.float(), (a, b) => {
      const as = encodeFloat64AsString(a);
      const bs = encodeFloat64AsString(b);

      const a2 = decodeFloat64AsString(as);
      const b2 = decodeFloat64AsString(bs);

      expect(a2).toBe(a);
      expect(b2).toBe(b);

      if (Object.is(a, b)) {
        expect(as).toBe(bs);
      } else {
        expect(as).not.toBe(bs);
        if (!Number.isNaN(a) && !Number.isNaN(b)) {
          expect(as < bs).toBe(a < b);
        }
      }
    }),
  );
});
