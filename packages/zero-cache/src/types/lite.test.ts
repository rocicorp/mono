import {describe, expect, test} from 'vitest';
import {liteValue, liteValues} from './lite.js';

describe('types/lite', () => {
  test('values', () => {
    expect(
      liteValues({
        a: 1,
        b: 'two',
        c: true,
        d: false,
        e: null,
        f: 12313214123432n,
      }),
    ).toEqual([1, 'two', 1, 0, null, 12313214123432n]);
  });

  test.each([
    [1, 1],
    ['two', 'two'],
    [null, null],
    [12313214123432n, 12313214123432n],
    [123.456, 123.456],
    [true, 1],
    [false, 0],

    // Yet to be supported data types.
    [Buffer.from('hello world'), Buffer.from('hello world')],
    [new Date(Date.UTC(2024, 9, 8)), 1728345600000],
    [{custom: {json: 'object'}}, '{"custom":{"json":"object"}}'],
    [[1, 2], '[1,2]'],
    [['two', 'three'], '["two","three"]'],
    [[null, null], '[null,null]'],
    [[12313214123432n, 12313214123432n], '[12313214123432,12313214123432]'],
    [[123.456, 987.654], '[123.456,987.654]'],
    [[true, false], '[1,0]'],
    [
      [new Date(Date.UTC(2024, 9, 8)), new Date(Date.UTC(2024, 7, 6))],
      '[1728345600000,1722902400000]',
    ],
    [
      [{custom: {json: 'object'}}, {another: {json: 'object'}}],
      '[{"custom":{"json":"object"}},{"another":{"json":"object"}}]',
    ],

    // Multi-dimensional array
    [
      [
        [new Date(Date.UTC(2024, 10, 31))],
        [new Date(Date.UTC(2024, 9, 21)), new Date(Date.UTC(2024, 8, 11))],
      ],
      '[[1733011200000],[1729468800000,1726012800000]]',
    ],
  ])('liteValue: %s', (input, output) => {
    expect(liteValue(input)).toEqual(output);
  });
});
