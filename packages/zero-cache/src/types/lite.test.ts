import {PreciseDate} from '@google-cloud/precise-date';
import {describe, expect, test} from 'vitest';
import {liteRow, liteValue, liteValues} from './lite.js';

describe('types/lite', () => {
  test.each([
    [{foo: 'bar'}, undefined],
    [{foo: 'bar', baz: 2n}, undefined],
    [{foo: 'bar', baz: 2n, boo: 3}, undefined],
    [{foo: 'bar', baz: 2n, boo: 3, zoo: null}, undefined],
    [{foo: true}, {foo: 1}],
    [
      {foo: 'bar', b: false},
      {foo: 'bar', b: 0},
    ],
    [
      {foo: 'bar', b: true, baz: 2n},
      {foo: 'bar', b: 1, baz: 2n},
    ],
    [
      {b: true, foo: 'bar', baz: 2n, boo: 3},
      {b: 1, foo: 'bar', baz: 2n, boo: 3},
    ],
    [
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: false},
      {foo: 'bar', baz: 2n, boo: 3, zoo: null, b: 0},
    ],
  ])('liteRow: %s', (input, output) => {
    const lite = liteRow(input);
    if (output) {
      expect(lite).toEqual(output);
    } else {
      expect(lite).toBe(input); // toBe => identity (i.e. no copy)
    }
  });

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
    [new PreciseDate('2024-10-08T00:00:00.123456Z'), 1728345600123456n],
    [{custom: {json: 'object'}}, '{"custom":{"json":"object"}}'],
    [[1, 2], '[1,2]'],
    [['two', 'three'], '["two","three"]'],
    [[null, null], '[null,null]'],
    [[12313214123432n, 12313214123432n], '[12313214123432,12313214123432]'],
    [[123.456, 987.654], '[123.456,987.654]'],
    [[true, false], '[1,0]'],
    [
      [
        new PreciseDate(Date.UTC(2024, 9, 8)),
        new PreciseDate(Date.UTC(2024, 7, 6)),
      ],
      '[1728345600000000,1722902400000000]',
    ],
    [
      [{custom: {json: 'object'}}, {another: {json: 'object'}}],
      '[{"custom":{"json":"object"}},{"another":{"json":"object"}}]',
    ],

    // Multi-dimensional array
    [
      [
        [new PreciseDate(Date.UTC(2024, 10, 31))],
        [
          new PreciseDate(Date.UTC(2024, 9, 21)),
          new PreciseDate(Date.UTC(2024, 8, 11)),
        ],
      ],
      '[[1733011200000000],[1729468800000000,1726012800000000]]',
    ],
  ])('liteValue: %s', (input, output) => {
    expect(liteValue(input)).toEqual(output);
  });
});
