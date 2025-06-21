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
