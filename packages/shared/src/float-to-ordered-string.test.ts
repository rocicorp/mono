import fc from 'fast-check';
import {expect, test} from 'vitest';
import {
  decodeFloat64AsString,
  encodeFloat64AsString,
} from './float-to-ordered-string.js';

const cases = [
  [-0, '1y2p0ij32e8e7'],
  [0, '1y2p0ij32e8e8'],
  [1, '2x2t6dniqybcw'],
  [2, '2x41irsmllclc'],
  [3, '2x4noyv6iwv7k'],
  [4, '2x59v5xqg8dts'],
  [-1, '0z2kunendu5fj'],
  [-2, '0z1ci99jj7473'],
  [-3, '0z0qc26zlvlkv'],
  [-4, '0z045v4fok2yn'],
  [3.141592653589793, '2x4qtzjh93rx4'],
  [NaN, '3w4rutzm7gy68'],
  [NaN, '3w4rutzm7gy68'],
  [Infinity, '3w45omx2a5fk0'],
  [-Infinity, '0018ce53un18f'],
  [Number.MAX_SAFE_INTEGER, '2yw3f766uv4sf'],
  [Number.MIN_SAFE_INTEGER, '0x9altvz9xc00'],
  [Number.MIN_VALUE, '1y2p0ij32e8e9'],
  [Number.MAX_VALUE, '3w45omx2a5fjz'],
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
