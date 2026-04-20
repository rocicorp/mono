import {describe, expect, test} from 'vitest';
import {
  formatSignature,
  parseSignature,
  rowIDSignatureUnit,
  xorSignature,
} from './row-set-signature.ts';
import type {RowID} from './schema/types.ts';

function rowID(table: string, id: string): RowID {
  return {schema: 'public', table, rowKey: {id}};
}

describe('row-set-signature', () => {
  test('formatSignature serializes bigints as hex', () => {
    expect(formatSignature(0n)).toEqual('0');
    expect(formatSignature(0xabcdn)).toEqual('abcd');
  });

  test('parseSignature round-trips via formatSignature', () => {
    for (const v of [0n, 1n, 0xdeadbeefn, 0xffffffffffffffffn]) {
      expect(parseSignature(formatSignature(v))).toEqual(v);
    }
  });

  test('parseSignature treats null / undefined / empty as 0n', () => {
    expect(parseSignature(null)).toEqual(0n);
    expect(parseSignature(undefined)).toEqual(0n);
    expect(parseSignature('')).toEqual(0n);
  });

  test('rowIDSignatureUnit is stable and distinct across rows', () => {
    const a = rowIDSignatureUnit(rowID('issues', '1'));
    const a2 = rowIDSignatureUnit(rowID('issues', '1'));
    const b = rowIDSignatureUnit(rowID('issues', '2'));
    const c = rowIDSignatureUnit(rowID('users', '1'));
    expect(a).toEqual(a2);
    expect(a).not.toEqual(b);
    // Same rowKey in a different table must hash differently.
    expect(a).not.toEqual(c);
  });

  test('xorSignature is commutative (order-independent)', () => {
    const a = rowIDSignatureUnit(rowID('issues', '1'));
    const b = rowIDSignatureUnit(rowID('issues', '2'));
    const c = rowIDSignatureUnit(rowID('issues', '3'));
    expect(xorSignature(undefined, a, b, c)).toEqual(
      xorSignature(undefined, c, a, b),
    );
    expect(xorSignature(undefined, a, b, c)).toEqual(
      xorSignature(undefined, b, c, a),
    );
  });

  test('xorSignature is self-inverse (XOR-in then XOR-out = identity)', () => {
    const a = rowIDSignatureUnit(rowID('issues', '1'));
    const b = rowIDSignatureUnit(rowID('issues', '2'));
    const afterAdds = xorSignature(undefined, a, b);
    const afterRemoves = xorSignature(afterAdds, a, b);
    expect(afterRemoves).toEqual('0');
  });

  test('empty set signature is "0"', () => {
    expect(xorSignature(undefined)).toEqual('0');
  });

  test('different row sets produce different signatures', () => {
    const set1 = xorSignature(
      undefined,
      rowIDSignatureUnit(rowID('issues', '1')),
      rowIDSignatureUnit(rowID('issues', '2')),
    );
    const set2 = xorSignature(
      undefined,
      rowIDSignatureUnit(rowID('issues', '1')),
      rowIDSignatureUnit(rowID('issues', '3')),
    );
    expect(set1).not.toEqual(set2);
  });
});
