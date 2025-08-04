import {describe, expect, test} from 'vitest';
import type {LiteTableSpec} from '../db/specs.ts';
import {
  dataTypeToZqlValueType,
  JSON_PARSED,
  JSON_STRINGIFIED,
  liteRow,
  liteValue,
  type JSONFormat,
  type LiteTypeString,
} from './lite.ts';
import type {RowValue} from './row-key.ts';
import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';

describe('types/lite', () => {
  test.each([
    [
      {foo: 'bar'},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {foo: {dataType: 'string', pos: 1, elemPgTypeClass: null}},
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 2, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 2, elemPgTypeClass: null},
          boo: {dataType: 'int', pos: 3, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 2, elemPgTypeClass: null},
          boo: {dataType: 'int', pos: 3, elemPgTypeClass: null},
          zoo: {dataType: 'int', pos: 4, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: true},
      {foo: 1},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {foo: {dataType: 'bool', pos: 1, elemPgTypeClass: null}},
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', b: false},
      {foo: 'bar', b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          b: {dataType: 'boolean', pos: 2, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', b: true, baz: 2n},
      {foo: 'bar', b: 1, baz: 2n},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          b: {dataType: 'boolean', pos: 2, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 3, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {b: true, foo: 'bar', baz: 2n, boo: 3},
      {b: 1, foo: 'bar', baz: 2n, boo: 3},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          b: {dataType: 'boolean', pos: 2, elemPgTypeClass: null},
          boo: {dataType: 'int', pos: 3, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 4, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: false},
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1, elemPgTypeClass: null},
          b: {dataType: 'boolean', pos: 2, elemPgTypeClass: null},
          boo: {dataType: 'int', pos: 3, elemPgTypeClass: null},
          baz: {dataType: 'int', pos: 4, elemPgTypeClass: null},
          zoo: {dataType: 'float', pos: 5, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {
        foo: 'bar',
        bar: 1,
        baz: true,
        boo: {key: 'val'},
      },
      {
        foo: '"bar"',
        bar: '1',
        baz: 'true',
        boo: '{"key":"val"}',
      },
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'json', pos: 1, elemPgTypeClass: null},
          bar: {dataType: 'jsonb', pos: 2, elemPgTypeClass: null},
          baz: {dataType: 'json', pos: 3, elemPgTypeClass: null},
          boo: {dataType: 'jsonb', pos: 4, elemPgTypeClass: null},
        },
      },
      JSON_PARSED,
    ],
    [
      {
        foo: '"bar"',
        bar: '1',
        baz: 'true',
        boo: '{"key":"val"}',
      },
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'json', pos: 1, elemPgTypeClass: null},
          bar: {dataType: 'jsonb', pos: 2, elemPgTypeClass: null},
          baz: {dataType: 'json', pos: 3, elemPgTypeClass: null},
          boo: {dataType: 'jsonb', pos: 4, elemPgTypeClass: null},
        },
      },
      JSON_STRINGIFIED,
    ],
  ] satisfies [RowValue, RowValue | undefined, LiteTableSpec, JSONFormat][])(
    'liteRow: %s',
    (input, output, table, jsonFormat) => {
      const {row: lite, numCols} = liteRow(input, table, jsonFormat);
      if (output) {
        expect(lite).toEqual(output);
      } else {
        expect(lite).toBe(input); // toBe => identity (i.e. no copy)
      }
      expect(numCols).toBe(Object.keys(input).length);
    },
  );

  test.each([
    ['int', 1, 1],
    ['string', 'two', 'two'],
    ['string', null, null],
    ['int', 12313214123432n, 12313214123432n],
    ['float', 123.456, 123.456],
    ['bool', true, 1],
    ['boolean', false, 0],

    ['bytea', Buffer.from('hello world'), Buffer.from('hello world')],
    ['json', {custom: {json: 'object'}}, '{"custom":{"json":"object"}}'],
    ['jsonb', [1, 2], '[1,2]'],
    ['json', ['two', 'three'], '["two","three"]'],
    ['json', [null, null], '[null,null]'],
    [
      'int[]',
      [12313214123432n, 12313214123432n],
      '[12313214123432,12313214123432]',
    ],
    ['float[]', [123.456, 987.654], '[123.456,987.654]'],
    ['bool[]', [true, false], '[true,false]'],
    [
      'json',
      [{custom: {json: 'object'}}, {another: {json: 'object'}}],
      '[{"custom":{"json":"object"}},{"another":{"json":"object"}}]',
    ],

    // Multi-dimensional array
    [
      'json[][]',
      [
        [{custom: {json: 'object'}}, {another: {json: 'object'}}],
        [{custom: {foo: 'bar'}}, {another: {boo: 'far'}}],
      ],
      '[[{"custom":{"json":"object"}},{"another":{"json":"object"}}],[{"custom":{"foo":"bar"}},{"another":{"boo":"far"}}]]',
    ],
  ])('liteValue: %s', (dataType, input, output) => {
    expect(liteValue(input, dataType, JSON_PARSED)).toEqual(output);
  });
});

describe('dataTypeToZqlValueType', () => {
  test.each([
    ['int', 'number'],
    ['text', 'string'],
    ['float', 'number'],
    ['bool', 'boolean'],
    ['boolean', 'boolean'],
    ['json', 'json'],
    ['int[]|NOT_NULL', 'json'],
    ['float[]', 'json'],
    ['bool[]', 'json'],
    ['json[]', 'json'],
    ['f[]|TEXT_ENUM', 'json'],
    ['b[]', 'json'],
    ['int|TEXT_ARRAY', 'json'],
    ['float|TEXT_ARRAY', 'json'],
    ['bool|TEXT_ARRAY', 'json'],
    ['json|TEXT_ARRAY', 'json'],
  ] satisfies [LiteTypeString, ValueType][])(
    'dataTypeToZqlValueType: %s => %s',
    (pgType, zqlType) => {
      expect(dataTypeToZqlValueType(pgType)).toBe(zqlType);
    },
  );
});
