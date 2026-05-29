import {expect, expectTypeOf, test} from 'vitest';
import type {SchemaValueToTSType} from '../../../zero-types/src/schema-value.ts';
import {column, number, timestamp} from './table-builder.ts';

test('.codec() attaches the codec and nulls customType at runtime', () => {
  const codec = {
    decode: (n: number) => new Date(n),
    encode: (d: Date) => d.getTime(),
  };
  const builder = number().codec<Date>(codec);
  expect(builder.schema).toMatchObject({
    type: 'number',
    optional: false,
    customType: null,
    codec,
  });
});

test('codec round-trips through the attached functions', () => {
  const {codec} = number().codec<Date>({
    decode: (n: number) => new Date(n),
    encode: (d: Date) => d.getTime(),
  }).schema;
  const d = new Date(1234);
  expect(codec.decode(codec.encode(d))).toEqual(d);
});

test('timestamp() preset decodes to Date', () => {
  const {codec, type} = timestamp().schema;
  expect(type).toBe('number');
  expect(codec.decode(1000)).toEqual(new Date(1000));
  expect(codec.encode(new Date(1000))).toBe(1000);
  expect(column.timestamp).toBe(timestamp);
});

test('.codec() composes with .optional() and .from()', () => {
  const builder = number()
    .from('created_at')
    .codec<Date>({
      decode: (n: number) => new Date(n),
      encode: (d: Date) => d.getTime(),
    })
    .optional();
  expect(builder.schema).toMatchObject({
    type: 'number',
    serverName: 'created_at',
    optional: true,
    customType: null,
  });
});

test('the user-facing TS type is the Decoded type', () => {
  const builder = number().codec<Date>({
    decode: (n: number) => new Date(n),
    encode: (d: Date) => d.getTime(),
  });
  expectTypeOf<
    SchemaValueToTSType<typeof builder.schema>
  >().toEqualTypeOf<Date>();

  const optional = builder.optional();
  expectTypeOf<
    SchemaValueToTSType<typeof optional.schema>
  >().toEqualTypeOf<Date | null>();
});

test('the encoded type of the codec is the column base type', () => {
  // The decode/encode functions are typed against `number` (the base type),
  // not the decoded `Date`.
  number().codec<Date>({
    // @ts-expect-error decode receives the encoded (number) type
    decode: (n: string) => new Date(n),
    encode: (d: Date) => d.getTime(),
  });
});
