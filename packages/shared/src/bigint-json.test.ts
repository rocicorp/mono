import {describe, expect, test} from 'vitest';
import {hasUnsafeIntegerDigits, parse, stringify} from './bigint-json.ts';

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
    {
      serialized: '{"a":[1,-123456789012345,1.5,null,true],"b":"x"}',
      deserialized: {a: [1, -123456789012345, 1.5, null, true], b: 'x'},
    },
    {
      serialized: '{"id":"1234567890123456","n":1}',
      deserialized: {id: '1234567890123456', n: 1},
    },
    {
      serialized: '{"id":"1234567890123456","n":9007199254740993}',
      deserialized: {id: '1234567890123456', n: 9007199254740993n},
    },
    {
      serialized: '-9007199254740993',
      deserialized: -9007199254740993n,
    },
  ];

  for (const c of cases) {
    test(c.serialized, () => {
      expect(parse(c.serialized)).toEqual(c.deserialized);
      expect(stringify(c.deserialized)).toBe(c.serialized);
    });
  }
});

describe('hasUnsafeIntegerDigits', () => {
  test.each([
    ['', false],
    ['123456789012345', false],
    ['1234567890123456', true],
    ['x1234567890123456', true],
    ['1234567890123456x', true],
    ['{"a":123456789012345,"b":123456789012345}', false],
    ['{"a":1234567890123,"b":12345678901234567}', true],
    ['{"a":"12345678.12345678"}', false],
  ])('%s', (str, expected) => {
    expect(hasUnsafeIntegerDigits(str)).toBe(expected);
  });

  test('matches /\\d{16}/ at every offset and run length', () => {
    for (let pad = 0; pad < 40; pad++) {
      for (let len = 0; len < 40; len++) {
        for (const before of ['', '1234567890123']) {
          const str =
            before + 'x'.repeat(pad) + '7'.repeat(len) + 'y'.repeat(pad % 7);
          expect(hasUnsafeIntegerDigits(str), str).toBe(/\d{16}/.test(str));
        }
      }
    }
  });
});
