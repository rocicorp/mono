import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {
  fromBase64url,
  fromBase64urlWithAtob,
  toBase64url,
  toBase64urlWithBtoa,
} from './base64.ts';

// toBase64url and fromBase64url use the standard methods in the browser
// project and btoa/atob in the Node project (Node 22 has no standard
// methods).
describe.each([
  {name: 'toBase64url', to: toBase64url, from: fromBase64url},
  {name: 'btoa', to: toBase64urlWithBtoa, from: fromBase64urlWithAtob},
])('$name', ({to, from}) => {
  test.each([
    {bytes: [], encoded: '', padded: ''},
    {bytes: [0], encoded: 'AA', padded: 'AA=='},
    {bytes: [0xfb, 0xff], encoded: '-_8', padded: '-_8='},
    {bytes: [1, 2, 3], encoded: 'AQID', padded: 'AQID'},
  ])('$bytes', ({bytes, encoded, padded}) => {
    const array = new Uint8Array(bytes);
    expect(to(array)).toBe(encoded);
    expect(from(encoded)).toEqual(array);
    expect(from(padded)).toEqual(array);
  });

  test('round-trips', () => {
    fc.assert(
      fc.property(fc.uint8Array(), bytes => {
        expect(from(to(bytes))).toEqual(bytes);
      }),
    );
  });
});

test('btoa agrees with toBase64url', () => {
  fc.assert(
    fc.property(fc.uint8Array(), bytes => {
      const encoded = toBase64url(bytes);
      expect(toBase64urlWithBtoa(bytes)).toBe(encoded);
      expect(fromBase64urlWithAtob(encoded)).toEqual(fromBase64url(encoded));
    }),
  );
});

test.each(['+/8=', 'A', 'Dw=', 'Dw=x', '=Dw', 'AAAA=', '!', ' Dw', 'D\nw'])(
  'rejects %j',
  encoded => {
    expect(() => fromBase64url(encoded)).toThrow(SyntaxError);
  },
);
