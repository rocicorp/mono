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
    ).toEqual([1, 'two', 'true', 'false', null, 12313214123432n]);
  });

  test.each([
    [1, 1],
    ['two', 'two'],
    [null, null],
    [12313214123432n, 12313214123432n],
    [123.456, 123.456],
    [true, 'true'],
    [false, 'false'],

    // Yet to be supported data types.
    [Buffer.from('hello world'), Buffer.from('hello world')],
    [new Date(Date.UTC(2024, 9, 8)), '"2024-10-08T00:00:00.000Z"'],
    [{custom: {json: 'object'}}, '{"custom":{"json":"object"}}'],
    [[1, 2], '[1,2]'],
    [['two', 'three'], '["two","three"]'],
    [[null, null], '[null,null]'],
    [[12313214123432n, 12313214123432n], '[12313214123432,12313214123432]'],
    [[123.456, 987.654], '[123.456,987.654]'],
    [[true, false], '[true,false]'],
    [
      [new Date(Date.UTC(2024, 9, 8)), new Date(Date.UTC(2024, 7, 6))],
      '["2024-10-08T00:00:00.000Z","2024-08-06T00:00:00.000Z"]',
    ],
    [
      [{custom: {json: 'object'}}, {another: {json: 'object'}}],
      '[{"custom":{"json":"object"}},{"another":{"json":"object"}}]',
    ],
  ])('liteValue: %s', (input, output) => {
    expect(liteValue(input)).toEqual(output);
  });
});
