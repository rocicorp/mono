import {describe, expect, test} from 'vitest';
import {RowKey, rowIDHash, rowKeyString} from './row-key.js';

describe('types/row-key', () => {
  type Case = {
    schema?: string;
    table?: string;
    keys: RowKey[];
    rowKeyString: string;
    rowIDHash: string;
  };

  const cases: Case[] = [
    {
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: 'LzYCyoprbpgVX1OvowtR0w',
    },
    {
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: '5dx4yFSW4NH-U00OUN6nBA',
    },
    {
      schema: 'zero',
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: 'TP0l9Jbd9d4Jppi30gCF7A',
    },
    {
      schema: 'clients',
      table: 'zero',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: 'iakA7CJFm2Yxz2vcRy-tgw',
    },
    {
      table: 'issues',
      keys: [{foo: ['bar']}, {foo: ['bar']}],
      rowKeyString: '["foo",["bar"]]',
      rowIDHash: '-sQXIlhIMvuh7cZ_2j_VUQ',
    },
    {
      keys: [{foo: 1}, {foo: 1}],
      rowKeyString: '["foo",1]',
      rowIDHash: '7P8yzu686tZglB9sTe-EUQ',
    },
    {
      keys: [{foo: '1'}, {foo: '1'}],
      rowKeyString: '["foo","1"]',
      rowIDHash: 'PzXFZK-F8o5nIMOJa0TQ6g',
    },
    {
      // Two-column keys
      keys: [
        {foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar'},
      ],
      rowKeyString: '["bar",["foo"],"foo","bar"]',
      rowIDHash: 'BPPyZXf9D3XzztvswmthYA',
    },
    {
      // Three-column keys
      keys: [
        {foo: 'bar', bar: ['foo'], baz: 2},
        {baz: 2, foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar', baz: 2},
      ],
      rowKeyString: '["bar",["foo"],"baz",2,"foo","bar"]',
      rowIDHash: '6aqwQTpouYQb9ST-v53efw',
    },
    {
      keys: [{id: 'HhCx1Vi3js'}],
      rowKeyString: '["id","HhCx1Vi3js"]',
      rowIDHash: 'DRduv2GfSDYjdImfnsw1Aw',
    },
  ];

  for (const c of cases) {
    const {schema = 'public', table = 'issue'} = c;
    test(`RowKey: ${schema}.${table}: ${JSON.stringify(c.keys)}`, () => {
      for (const keys of c.keys) {
        expect(rowKeyString(keys)).toBe(c.rowKeyString);
        expect(rowIDHash({schema, table, rowKey: keys})).toBe(c.rowIDHash);
      }
    });
  }
});
