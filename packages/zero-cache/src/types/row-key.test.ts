import {describe, expect, test} from 'vitest';
import {RowKeyValue, rowKeyHash, rowKeyString} from './row-key.js';

describe('types/row-key', () => {
  type Case = {
    keys: RowKeyValue[];
    rowKeyString: string;
    rowKeyHash: string;
  };

  const cases: Case[] = [
    {
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowKeyHash: 'obTrSkxiG_NZGYmdhk5cCQ',
    },
    {
      keys: [{foo: ['bar']}, {foo: ['bar']}],
      rowKeyString: '["foo",["bar"]]',
      rowKeyHash: 'EXSpge3fqVTerIyk9DfobA',
    },
    {
      keys: [{foo: 1}, {foo: 1}],
      rowKeyString: '["foo",1]',
      rowKeyHash: 'xI2eLW2t4cDhxaoLThCOBg',
    },
    {
      keys: [{foo: '1'}, {foo: '1'}],
      rowKeyString: '["foo","1"]',
      rowKeyHash: 'uj8XlgCYqo0xZ-ptdN-X0A',
    },
    {
      // Two-column keys
      keys: [
        {foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar'},
      ],
      rowKeyString: '["bar",["foo"],"foo","bar"]',
      rowKeyHash: 'tjWbs643-85yVQmnKG7yyA',
    },
    {
      // Three-column keys
      keys: [
        {foo: 'bar', bar: ['foo'], baz: 2},
        {baz: 2, foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar', baz: 2},
      ],
      rowKeyString: '["bar",["foo"],"baz",2,"foo","bar"]',
      rowKeyHash: 'RgvjgWwor4UuJ-huLJUHIA',
    },
  ];

  for (const c of cases) {
    test(`RowKey: ${c.keys.join(',')}`, () => {
      for (const keys of c.keys) {
        expect(rowKeyString(keys)).toBe(c.rowKeyString);
        expect(rowKeyHash(keys)).toBe(c.rowKeyHash);
      }
    });
  }
});
