import {describe, expect, test} from '@jest/globals';
import {Bytes} from 'firebase/firestore';
import * as v from 'shared/src/valita.js';
import {bytesSchema} from './bytes.js';

describe('bytes schema', () => {
  const schema = v.object({
    foo: bytesSchema,
  });

  test('Uint8array', () => {
    const buffer = Buffer.from('foobar');
    const data = {
      foo: buffer,
    };

    expect(v.is(data, schema)).toBe(true);
    const parsed = v.parse(data, schema);
    expect(parsed.foo).toBeInstanceOf(Uint8Array);
    expect(Buffer.from(parsed.foo).toString('hex')).toEqual(
      buffer.toString('hex'),
    );
  });

  test('Bytes', () => {
    const buffer = Buffer.from('barfoo');
    const data = {
      foo: Bytes.fromUint8Array(buffer),
    };

    expect(v.is(data, schema)).toBe(true);
    const parsed = v.parse(data, schema);
    expect(parsed.foo).not.toBeInstanceOf(Bytes);
    expect(parsed.foo).toBeInstanceOf(Uint8Array);
    expect(Buffer.from(parsed.foo).toString('hex')).toEqual(
      buffer.toString('hex'),
    );
  });

  test('invalid bytes', () => {
    const data = {
      foo: "these are not the bytes you're looking for",
    };
    expect(v.is(data, schema)).toBe(false);
    expect(() => v.parse(data, schema)).toThrowError;
  });
});
