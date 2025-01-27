import {describe, expect, test} from 'vitest';
import {
  type RowKey,
  normalizedKeyOrder,
  rowIDHash,
  rowIDString,
  rowKeyString,
} from './row-key.ts';

describe('types/row-key', () => {
  type Case = {
    schema?: string;
    table?: string;
    keys: RowKey[];
    rowKeyString: string;
    rowIDString: string;
    rowIDHash: string;
  };

  const cases: Case[] = [
    {
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDString: '["public","issue","foo","bar"]',
      rowIDHash: 'ciol231ukcwkot147odcn45m0',
    },
    {
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDString: '["public","clients","foo","bar"]',
      rowIDHash: '64611vx2jblwgdkqghzcfnbhm',
    },
    {
      schema: 'zero',
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDString: '["zero","clients","foo","bar"]',
      rowIDHash: 'd5ylu9yny0atlxwv84ckob3iq',
    },
    {
      schema: 'clients',
      table: 'zero',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDString: '["clients","zero","foo","bar"]',
      rowIDHash: '46fn166ycpx29z47xjh8mcqxp',
    },
    {
      table: 'issues',
      keys: [{foo: ['bar']}, {foo: ['bar']}],
      rowKeyString: '["foo",["bar"]]',
      rowIDString: '["public","issues","foo",["bar"]]',
      rowIDHash: '9q3o77bjorgu22uheyyr3yyh2',
    },
    {
      keys: [{foo: 1}, {foo: 1}],
      rowKeyString: '["foo",1]',
      rowIDString: '["public","issue","foo",1]',
      rowIDHash: 'cy4p72xet3a20cgyrdj1c81ak',
    },
    {
      keys: [{foo: '1'}, {foo: '1'}],
      rowKeyString: '["foo","1"]',
      rowIDString: '["public","issue","foo","1"]',
      rowIDHash: '5ejr02sz9n3l7zpt82rr8mh7c',
    },
    {
      // Two-column keys
      keys: [
        {foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar'},
      ],
      rowKeyString: '["bar",["foo"],"foo","bar"]',
      rowIDString: '["public","issue","bar",["foo"],"foo","bar"]',
      rowIDHash: '5h887x9fpyacg9dsk8ld9w6qf',
    },
    {
      // Three-column keys
      keys: [
        {foo: 'bar', bar: ['foo'], baz: 2},
        {baz: 2, foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar', baz: 2},
      ],
      rowKeyString: '["bar",["foo"],"baz",2,"foo","bar"]',
      rowIDString: '["public","issue","bar",["foo"],"baz",2,"foo","bar"]',
      rowIDHash: '3qflvcrevxjynhsqs07r27cik',
    },
    {
      keys: [{id: 'HhCx1Vi3js'}],
      rowKeyString: '["id","HhCx1Vi3js"]',
      rowIDString: '["public","issue","id","HhCx1Vi3js"]',
      rowIDHash: '6si0q0rmq27la39k5mhtl9420',
    },
  ];

  for (const c of cases) {
    const {schema = 'public', table = 'issue'} = c;
    test(`RowKey: ${schema}.${table}: ${JSON.stringify(c.keys)}`, () => {
      for (const keys of c.keys) {
        expect(rowKeyString(keys)).toBe(c.rowKeyString);
        expect(rowIDString({schema, table, rowKey: keys})).toBe(c.rowIDString);
        expect(rowIDHash({schema, table, rowKey: keys})).toBe(c.rowIDHash);
      }
    });
  }

  test('normalizedKeyOrder', () => {
    const sorted = {a: 3, b: 2, c: 1};
    const notSorted = [
      {a: 3, c: 1, b: 2},
      {b: 2, a: 3, c: 1},
      {b: 2, c: 1, a: 3},
      {c: 1, b: 2, a: 3},
      {c: 1, a: 3, b: 2},
    ];

    expect(normalizedKeyOrder(sorted)).toBe(sorted);

    for (const str of notSorted) {
      expect(Object.keys(normalizedKeyOrder(str))).toEqual(['a', 'b', 'c']);
    }

    expect(() => normalizedKeyOrder({})).toThrowError('empty row key');
  });
});
