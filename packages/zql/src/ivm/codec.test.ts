import {describe, expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Codec, SchemaValue} from '../../../zero-types/src/schema-value.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {
  columnsHaveCodecs,
  decodeQueryResult,
  decodeRowFields,
  encodeRow,
  encodeValue,
} from './codec.ts';

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

describe('columnsHaveCodecs', () => {
  test('false when no codecs', () => {
    expect(columnsHaveCodecs(plainColumns)).toBe(false);
  });

  test('true when a column has a codec', () => {
    expect(columnsHaveCodecs(codecColumns)).toBe(true);
  });

  test('is memoized per columns object', () => {
    const columns: Record<string, SchemaValue> = {id: {type: 'string'}};
    expect(columnsHaveCodecs(columns)).toBe(false);
    // Mutating after the first query is not observed: the result is cached on
    // the columns object, which is immutable in practice (it belongs to the
    // schema).
    columns.createdAt = codecColumns.createdAt;
    expect(columnsHaveCodecs(columns)).toBe(false);
    expect(columnsHaveCodecs({...columns})).toBe(true);
  });
});

describe('decodeRowFields', () => {
  test('returns input unchanged when no codecs (no copy)', () => {
    const row = {id: 'a', title: 'x'};
    expect(decodeRowFields(row, plainColumns)).toBe(row);
  });

  test('decodes codec columns', () => {
    const row = {id: 'a', createdAt: 1000};
    const result = decodeRowFields(row, codecColumns);
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
    const result = decodeRowFields(row, codecColumns);
    expect(result.createdAt).toBe(null);
  });

  test('decodes only columns present in the row', () => {
    const row = {id: 'a'}; // createdAt omitted
    const result = decodeRowFields(row, codecColumns);
    expect(result).toBe(row); // no codec columns present → unchanged
  });

  test('ignores row keys that are not columns', () => {
    const row = {id: 'a', createdAt: 1000, extra: 1};
    const result = decodeRowFields(row, codecColumns);
    expect((result as unknown as {createdAt: Date}).createdAt.getTime()).toBe(
      1000,
    );
    expect(result.extra).toBe(1);
  });
});

describe('decodeQueryResult', () => {
  // issue (codec: createdAt) -> comments (codec: createdAt) -> author (plain)
  // issue -> labels via the issueLabel junction (hidden), label has no codec.
  const schema = {
    tables: {
      issue: {name: 'issue', columns: codecColumns, primaryKey: ['id']},
      comment: {
        name: 'comment',
        columns: {...codecColumns, issueID: {type: 'string'}},
        primaryKey: ['id'],
      },
      user: {name: 'user', columns: plainColumns, primaryKey: ['id']},
      issueLabel: {
        name: 'issueLabel',
        columns: {issueID: {type: 'string'}, labelID: {type: 'string'}},
        primaryKey: ['issueID', 'labelID'],
      },
      label: {name: 'label', columns: plainColumns, primaryKey: ['id']},
    },
    relationships: {},
  } as unknown as Schema;

  const correlation = {parentField: ['id'], childField: ['issueID']} as const;
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation,
        subquery: {
          table: 'comment',
          alias: 'comments',
          related: [
            {
              correlation: {parentField: ['authorID'], childField: ['id']},
              subquery: {table: 'user', alias: 'author'},
            },
          ],
        },
      },
      {
        correlation,
        hidden: true,
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {parentField: ['labelID'], childField: ['id']},
              subquery: {table: 'label', alias: 'labels'},
            },
          ],
        },
      },
    ],
  };
  const format: Format = {
    singular: false,
    relationships: {
      comments: {
        singular: false,
        relationships: {author: {singular: true, relationships: {}}},
      },
      labels: {singular: false, relationships: {}},
    },
  };

  test('decodes codec columns at every level, junctions included', () => {
    const result = [
      {
        id: 'i1',
        createdAt: 1000,
        comments: [
          {id: 'c1', issueID: 'i1', createdAt: 2000, author: {id: 'u1'}},
          {id: 'c2', issueID: 'i1', createdAt: null, author: null},
        ],
        labels: [{id: 'l1', title: 'bug'}],
      },
    ];
    const decoded = decodeQueryResult(result, ast, format, schema) as Array<{
      createdAt: Date;
      comments: Array<{createdAt: Date | null; author: unknown}>;
      labels: unknown[];
    }>;
    expect(decoded).not.toBe(result);
    expect(decoded[0].createdAt).toBeInstanceOf(Date);
    expect(decoded[0].createdAt.getTime()).toBe(1000);
    expect(decoded[0].comments[0].createdAt).toBeInstanceOf(Date);
    expect(decoded[0].comments[1].createdAt).toBe(null);
    // Codec-free subtrees keep identity.
    expect(decoded[0].comments[0].author).toBe(result[0].comments[0].author);
    expect(decoded[0].labels).toBe(result[0].labels);
    // The input is not mutated.
    expect(result[0].createdAt).toBe(1000);
    expect(result[0].comments[0].createdAt).toBe(2000);
  });

  test('singular results and null/undefined pass through', () => {
    const row = {id: 'i1', createdAt: 5, comments: [], labels: []};
    const decoded = decodeQueryResult(
      row,
      ast,
      {...format, singular: true},
      schema,
    ) as {createdAt: Date};
    expect(decoded.createdAt.getTime()).toBe(5);
    expect(decodeQueryResult(undefined, ast, format, schema)).toBe(undefined);
    expect(decodeQueryResult(null, ast, format, schema)).toBe(null);
  });

  test('returns the input unchanged when no table has a codec', () => {
    const plainAST: AST = {table: 'user'};
    const plainFormat: Format = {singular: false, relationships: {}};
    const result = [{id: 'u1', title: 'x'}];
    expect(decodeQueryResult(result, plainAST, plainFormat, schema)).toBe(
      result,
    );
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

  test('ignores row keys that are not columns', () => {
    const row = {id: 'a', createdAt: new Date(5), extra: 1};
    expect(encodeRow(row, codecColumns)).toEqual({
      id: 'a',
      createdAt: 5,
      extra: 1,
    });
  });

  test('encodeValue encodes single values', () => {
    expect(encodeValue(new Date(7), codecColumns.createdAt)).toBe(7);
    expect(encodeValue('x', plainColumns.title)).toBe('x');
    expect(encodeValue(null, codecColumns.createdAt)).toBe(null);
  });
});
