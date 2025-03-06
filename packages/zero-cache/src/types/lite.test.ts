import {describe, expect, test} from 'vitest';
import {liteRow, liteValue, liteValues} from './lite.ts';

describe('types/lite', () => {
  test.each([
    [
      {foo: 'bar'},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {foo: {dataType: 'string', pos: 1}},
      },
    ],
    [
      {foo: 'bar', baz: 2n},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          baz: {dataType: 'int', pos: 2},
        },
      },
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          baz: {dataType: 'int', pos: 2},
          boo: {dataType: 'int', pos: 3},
        },
      },
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null},
      undefined,
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          baz: {dataType: 'int', pos: 2},
          boo: {dataType: 'int', pos: 3},
          zoo: {dataType: 'int', pos: 4},
        },
      },
    ],
    [
      {foo: true},
      {foo: 1},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {foo: {dataType: 'bool', pos: 1}},
      },
    ],
    [
      {foo: 'bar', b: false},
      {foo: 'bar', b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          b: {dataType: 'boolean', pos: 2},
        },
      },
    ],
    [
      {foo: 'bar', b: true, baz: 2n},
      {foo: 'bar', b: 1, baz: 2n},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          b: {dataType: 'boolean', pos: 2},
          baz: {dataType: 'int', pos: 3},
        },
      },
    ],
    [
      {b: true, foo: 'bar', baz: 2n, boo: 3},
      {b: 1, foo: 'bar', baz: 2n, boo: 3},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          b: {dataType: 'boolean', pos: 2},
          boo: {dataType: 'int', pos: 3},
          baz: {dataType: 'int', pos: 4},
        },
      },
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: false},
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: 0},
      {
        name: 'tableName',
        primaryKey: ['foo'],
        columns: {
          foo: {dataType: 'string', pos: 1},
          b: {dataType: 'boolean', pos: 2},
          boo: {dataType: 'int', pos: 3},
          baz: {dataType: 'int', pos: 4},
          zoo: {dataType: 'float', pos: 5},
        },
      },
    ],
  ])('liteRow: %s', (input, output, table) => {
    const {row: lite, numCols} = liteRow(input, table);
    if (output) {
      expect(lite).toEqual(output);
    } else {
      expect(lite).toBe(input); // toBe => identity (i.e. no copy)
    }
    expect(numCols).toBe(Object.keys(input).length);
  });

  test('values', () => {
    expect(
      liteValues(
        {
          a: 1,
          b: 'two',
          c: true,
          d: false,
          e: null,
          f: 12313214123432n,
        },
        {
          name: 'tableName',
          primaryKey: ['a'],
          columns: {
            a: {dataType: 'int', pos: 1},
            b: {dataType: 'string', pos: 2},
            c: {dataType: 'bool', pos: 3},
            d: {dataType: 'bool', pos: 4},
            e: {dataType: 'float', pos: 5},
            f: {dataType: 'int8', pos: 6},
          },
        },
      ),
    ).toEqual([1, 'two', 1, 0, null, 12313214123432n]);
  });

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
    ['bool[]', [true, false], '[1,0]'],
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
    expect(liteValue(input, dataType)).toEqual(output);
  });
});
