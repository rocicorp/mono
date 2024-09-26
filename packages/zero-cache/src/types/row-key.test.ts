import {describe, expect, test} from 'vitest';
import {
  type RowKey,
  normalizedKeyOrder,
  rowIDHash,
  rowKeyString,
} from './row-key.js';

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
      rowIDHash: '3z7dbf9d35nybsu0u6j0qdduu',
    },
    {
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: '369s9ujkm8cshq8maksagrk4z',
    },
    {
      schema: 'zero',
      table: 'clients',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: 'bdi7ujkjhk018p49qckqpjs59',
    },
    {
      schema: 'clients',
      table: 'zero',
      keys: [{foo: 'bar'}, {foo: 'bar'}],
      rowKeyString: '["foo","bar"]',
      rowIDHash: '83rna1e2y74s7ik5skigv9223',
    },
    {
      table: 'issues',
      keys: [{foo: ['bar']}, {foo: ['bar']}],
      rowKeyString: '["foo",["bar"]]',
      rowIDHash: 'c8pu7tydek3r9wopx9c4o64nl',
    },
    {
      keys: [{foo: 1}, {foo: 1}],
      rowKeyString: '["foo",1]',
      rowIDHash: 'gjssbmsl6avdktnaq8an52jg',
    },
    {
      keys: [{foo: '1'}, {foo: '1'}],
      rowKeyString: '["foo","1"]',
      rowIDHash: 'cr6zlx3dei78jpjv8qecv5b63',
    },
    {
      // Two-column keys
      keys: [
        {foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar'},
      ],
      rowKeyString: '["bar",["foo"],"foo","bar"]',
      rowIDHash: '73vrcw1djlz99hvz4lqjyt2bw',
    },
    {
      // Three-column keys
      keys: [
        {foo: 'bar', bar: ['foo'], baz: 2},
        {baz: 2, foo: 'bar', bar: ['foo']},
        {bar: ['foo'], foo: 'bar', baz: 2},
      ],
      rowKeyString: '["bar",["foo"],"baz",2,"foo","bar"]',
      rowIDHash: '802jgj2gmrqy0khigiknxueof',
    },
    {
      keys: [{id: 'HhCx1Vi3js'}],
      rowKeyString: '["id","HhCx1Vi3js"]',
      rowIDHash: 'd9wwy0a6s1olyhxq8vkvw7kln',
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
  });
});
