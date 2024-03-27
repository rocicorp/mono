import {describe, expect, test} from '@jest/globals';
import {rowKeyHash} from './row-key.js';

describe('types/row-key', () => {
  type Case = {
    keys: Record<string, unknown>[];
    rowKey: string;
  };

  const cases: Case[] = [
    {
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKey: 'obTrSkxiG_NZGYmdhk5cCQ',
    },
    {
      keys: [{foo: ['bar']}, {foo: ['bar']}],
      rowKey: 'EXSpge3fqVTerIyk9DfobA',
    },
    {
      keys: [{foo: 1}, {foo: 1}],
      rowKey: 'xI2eLW2t4cDhxaoLThCOBg',
    },
    {
      keys: [{foo: '1'}, {foo: '1'}],
      rowKey: 'uj8XlgCYqo0xZ-ptdN-X0A',
    },
    {
      // Two-column keys
      keys: [
        {foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar'},
      ],
      rowKey: 'tjWbs643-85yVQmnKG7yyA',
    },
    {
      // Three-column keys
      keys: [
        {foo: 'bar', bar: ['foo'], baz: 2},
        {baz: 2, foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar', baz: 2},
      ],
      rowKey: 'RgvjgWwor4UuJ-huLJUHIA',
    },
  ];

  for (const c of cases) {
    test(`RowKey: ${c.keys.join(',')}`, () => {
      for (const keys of c.keys) {
        expect(rowKeyHash(keys)).toBe(c.rowKey);
      }
    });
  }
});
