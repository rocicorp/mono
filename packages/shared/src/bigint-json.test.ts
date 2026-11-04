import {describe, expect, test} from 'vitest';
import {parse, stringify} from './bigint-json.ts';

describe('types/json', () => {
  type Case = {
    serialized: string;
    deserialized: unknown;
  };

  const cases: Case[] = [
    {
      serialized: '9007199254740991',
      deserialized: 9007199254740991,
    },
    {
      serialized: '9007199254740993',
      deserialized: 9007199254740993n,
    },
    {
      serialized: '{"big":90071992547409930000000000}',
      deserialized: {big: 90071992547409930000000000n},
    },
  ];

  for (const c of cases) {
    test(c.serialized, () => {
      expect(parse(c.serialized)).toEqual(c.deserialized);
      expect(stringify(c.deserialized)).toBe(c.serialized);
    });
  }
});

describe('numbers beyond the safe integer range', () => {
  test.each([
    ['2^53', 2 ** 53, '9.007199254740992e+15'],
    ['2^62', 2 ** 62, '4.611686018427388e+18'],
    ['-(2^63)', -(2 ** 63), '-9.223372036854776e+18'],
    ['1e19', 1e19, '1e+19'],
    ['1e21', 1e21, '1e+21'],
    ['1e300', 1e300, '1e+300'],
  ])('%s stays a number', (_, value, json) => {
    expect(stringify({n: value})).toBe(`{"n":${json}}`);
    expect(parse(stringify({n: value}))).toEqual({n: value});
    expect(JSON.parse(stringify({n: value}))).toEqual({n: value});
  });

  test.each([
    ['a bigint', 9007199254740993n, '9007199254740993'],
    ['a large bigint', 10n ** 20n, '100000000000000000000'],
    ['the largest safe integer', 9007199254740991, '9007199254740991'],
    ['a fraction', 1.5, '1.5'],
    ['a large fraction', 1e15 + 0.5, '1000000000000000.5'],
  ])('%s is unchanged', (_, value, json) => {
    expect(stringify(value)).toBe(json);
    expect(parse(stringify(value))).toBe(value);
  });

  test('a bigint and a number stay distinct in one value', () => {
    const value = {big: 2n ** 62n, double: 2 ** 62};

    expect(parse(stringify(value))).toEqual(value);
  });
});
