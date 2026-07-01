import {describe, expect, test} from 'vitest';
import type {Codec, SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {
  columnsHaveCodecs,
  decodeRowFields,
  encodeRow,
  encodeValue,
  schemaHasCodecs,
} from './codec.ts';
import type {SourceSchema} from './schema.ts';

const dateCodec: Codec<number, Date> = {
  decode: (n: number) => new Date(n),
  encode: (d: Date) => d.getTime(),
};

const plainColumns: Record<string, SchemaValue> = {
  id: {type: 'string'},
  title: {type: 'string'},
};

const codecColumns: Record<string, SchemaValue> = {
  id: {type: 'string'},
  createdAt: {
    type: 'number',
    customType: null,
    codec: dateCodec,
  } as SchemaValue,
};

function source(
  columns: Record<string, SchemaValue>,
  relationships: Record<string, SourceSchema> = {},
): SourceSchema {
  return {
    tableName: 't',
    columns,
    primaryKey: ['id'],
    relationships,
    isHidden: false,
    system: 'client',
    compareRows: () => 0,
  };
}

describe('columnsHaveCodecs / schemaHasCodecs', () => {
  test('false when no codecs', () => {
    expect(columnsHaveCodecs(plainColumns)).toBe(false);
    expect(schemaHasCodecs(source(plainColumns))).toBe(false);
  });

  test('true when a column has a codec', () => {
    expect(columnsHaveCodecs(codecColumns)).toBe(true);
    expect(schemaHasCodecs(source(codecColumns))).toBe(true);
  });

  test('true when a relationship has a codec', () => {
    const schema = source(plainColumns, {comments: source(codecColumns)});
    expect(schemaHasCodecs(schema)).toBe(true);
  });

  test('handles cyclic relationships', () => {
    const self = source(plainColumns) as {
      relationships: Record<string, SourceSchema>;
    } & SourceSchema;
    self.relationships = {self};
    expect(schemaHasCodecs(self)).toBe(false);
  });
});

describe('decodeRowFields', () => {
  test('returns input unchanged when no codecs (no copy)', () => {
    const row = {id: 'a', title: 'x'};
    expect(decodeRowFields(row, source(plainColumns))).toBe(row);
  });

  test('decodes codec columns', () => {
    const row = {id: 'a', createdAt: 1000};
    const result = decodeRowFields(row, source(codecColumns));
    expect(result).not.toBe(row);
    expect((result as unknown as {createdAt: Date}).createdAt).toBeInstanceOf(
      Date,
    );
    expect((result as unknown as {createdAt: Date}).createdAt.getTime()).toBe(
      1000,
    );
    expect(result.id).toBe('a');
    // original untouched
    expect(row.createdAt).toBe(1000);
  });

  test('passes null through without decoding', () => {
    const row = {id: 'a', createdAt: null};
    const result = decodeRowFields(row, source(codecColumns));
    expect(result.createdAt).toBe(null);
  });

  test('decodes only columns present in the row', () => {
    const row = {id: 'a'}; // createdAt omitted
    const result = decodeRowFields(row, source(codecColumns));
    expect(result).toBe(row); // no codec columns present → unchanged
  });
});

describe('encodeRow / encodeValue', () => {
  test('returns input unchanged when no codecs (no copy)', () => {
    const row = {id: 'a', title: 'x'};
    expect(encodeRow(row, plainColumns)).toBe(row);
  });

  test('encodes codec columns', () => {
    const row = {id: 'a', createdAt: new Date(1234)};
    const result = encodeRow(row, codecColumns);
    expect(result).not.toBe(row);
    expect(result.createdAt).toBe(1234);
    expect(result.id).toBe('a');
  });

  test('only copies when a codec column is present in the row', () => {
    const row = {id: 'a'}; // createdAt omitted (partial update)
    expect(encodeRow(row, codecColumns)).toBe(row);
  });

  test('passes null through', () => {
    const row = {id: 'a', createdAt: null};
    const result = encodeRow(row, codecColumns);
    expect(result.createdAt).toBe(null);
  });

  test('encodeValue encodes single values', () => {
    expect(encodeValue(new Date(7), codecColumns.createdAt)).toBe(7);
    expect(encodeValue('x', plainColumns.title)).toBe('x');
    expect(encodeValue(null, codecColumns.createdAt)).toBe(null);
  });
});
