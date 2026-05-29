import {describe, expect, test} from 'vitest';
import type {Codec, SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {
  columnsHaveCodecs,
  decodeView,
  encodeRow,
  encodeValue,
  schemaHasCodecs,
} from './codec.ts';
import type {SourceSchema} from './schema.ts';
import type {Format} from './view.ts';

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

describe('decodeView', () => {
  const pluralFormat: Format = {singular: false, relationships: {}};

  test('returns input unchanged when no codecs (no copy)', () => {
    const data = [{id: 'a', title: 'x'}];
    const result = decodeView(data, source(plainColumns), pluralFormat);
    expect(result).toBe(data);
  });

  test('decodes codec columns in a plural result', () => {
    const data = [{id: 'a', createdAt: 1000}];
    const result = decodeView(
      data,
      source(codecColumns),
      pluralFormat,
    ) as Array<Record<string, unknown>>;
    expect(result).not.toBe(data);
    expect(result[0].createdAt).toBeInstanceOf(Date);
    expect((result[0].createdAt as Date).getTime()).toBe(1000);
    expect(result[0].id).toBe('a');
    // original untouched
    expect(data[0].createdAt).toBe(1000);
  });

  test('passes null/undefined through without decoding', () => {
    const data = [{id: 'a', createdAt: null}];
    const result = decodeView(
      data,
      source(codecColumns),
      pluralFormat,
    ) as Array<Record<string, unknown>>;
    expect(result[0].createdAt).toBe(null);
  });

  test('decodes a singular result', () => {
    const singular: Format = {singular: true, relationships: {}};
    const data = {id: 'a', createdAt: 2000};
    const result = decodeView(data, source(codecColumns), singular) as Record<
      string,
      unknown
    >;
    expect((result.createdAt as Date).getTime()).toBe(2000);
  });

  test('decodes nested relationships', () => {
    const schema = source(plainColumns, {comments: source(codecColumns)});
    const format: Format = {
      singular: false,
      relationships: {comments: {singular: false, relationships: {}}},
    };
    const data = [{id: 'a', title: 'x', comments: [{id: 'c1', createdAt: 5}]}];
    const result = decodeView(data, schema, format) as unknown as Array<{
      comments: Array<{createdAt: Date}>;
    }>;
    expect(result[0].comments[0].createdAt).toBeInstanceOf(Date);
    expect(result[0].comments[0].createdAt.getTime()).toBe(5);
  });

  test('undefined passes through', () => {
    expect(
      decodeView(undefined, source(codecColumns), pluralFormat),
    ).toBeUndefined();
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
    expect(encodeValue(new Date(7), undefined)).toBeInstanceOf(Date);
  });
});
