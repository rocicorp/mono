import {expect, expectTypeOf, test} from 'vitest';
import {createSchema} from './mod.js';
import {number, string, table} from './builder/table-builder.js';
import {relationships} from './builder/relationship-builder.js';

test('Key name does not matter', () => {
  const schema = createSchema(
    1,
    {foo: table('bar').columns({id: string()}).primaryKey('id')},
    {},
  );

  expectTypeOf(schema.tables.bar).toEqualTypeOf<{
    name: 'bar';
    columns: {id: {type: 'string'; optional: false; customType: string}};
    primaryKey: ['id'];
  }>({} as never);
  // @ts-expect-error - no foo table
  schema.tables.foo;
});

test('Missing primary key is an error', () => {
  expect(() =>
    createSchema(1, {foo: table('foo').columns({id: string()})}, {}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Table "foo" is missing a primary key]`,
  );
});

test('Missing table in direct relationship should throw', () => {
  const bar = table('bar')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const foo = table('foo')
    .columns({
      id: number(),
      barID: number(),
    })
    .primaryKey('id');

  const fooRelationships = relationships(foo, connect => ({
    barRelation: connect({
      sourceField: ['barID'],
      destField: ['id'],
      destSchema: bar,
    }),
  }));

  expect(() =>
    createSchema(1, {foo}, {fooRelationships}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "foo"."barRelation", destination table "bar" is missing in the schema]`,
  );
});

test('Missing table in junction relationship should throw', () => {
  const tableA = table('tableA')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const tableB = table('tableB')
    .columns({
      id: number(),
      aID: number(),
    })
    .primaryKey('id');

  const tableC = table('tableC')
    .columns({
      id: number(),
      bID: number(),
      aID: number(),
    })
    .primaryKey('id');

  const tableBRelationships = relationships(tableB, connect => ({
    relationBToA: connect({
      sourceField: ['aID'],
      destField: ['id'],
      destSchema: tableA,
    }),
  }));

  const tableCRelationships = relationships(tableC, connect => ({
    relationCToB: connect(
      {
        sourceField: ['bID'],
        destField: ['id'],
        destSchema: tableB,
      },
      {
        sourceField: ['aID'],
        destField: ['id'],
        destSchema: tableA,
      },
    ),
  }));

  expect(() =>
    createSchema(
      1,
      {tableB, tableC},
      {tableBRelationships, tableCRelationships},
    ),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "tableB"."relationBToA", destination table "tableA" is missing in the schema]`,
  );
});

test('Missing column in direct relationship destination should throw', () => {
  const bar = table('bar')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const foo = table('foo')
    .columns({
      id: number(),
      barID: number(),
    })
    .primaryKey('id');

  relationships(foo, connect => ({
    barRelation: connect({
      sourceField: ['barID'],
      // @ts-expect-error - missing column
      destField: ['missing'],
      destSchema: bar,
    }),
  }));
});

test('Missing column in direct relationship source should throw', () => {
  const bar = table('bar')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const foo = table('foo')
    .columns({
      id: number(),
      barID: number(),
    })
    .primaryKey('id');

  const fooRelationships = relationships(foo, connect => ({
    barRelation: connect({
      sourceField: ['missing'],
      destField: ['id'],
      destSchema: bar,
    }),
  }));

  expect(() =>
    createSchema(1, {bar, foo}, {fooRelationships}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "foo"."barRelation", the source field "missing" is missing in the table schema "foo"]`,
  );
});

test('Missing column in junction relationship destination should throw', () => {
  const tableB = table('tableB')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const junctionTable = table('junctionTable')
    .columns({
      id: number(),
      aID: number(),
      bID: number(),
    })
    .primaryKey('id');

  const tableA = table('tableA')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  relationships(tableA, connect => ({
    relationAToB: connect(
      {
        sourceField: ['id'],
        destField: ['aID'],
        destSchema: junctionTable,
      },
      {
        sourceField: ['aID'],
        // @ts-expect-error - missing column
        destField: ['missing'],
        destSchema: tableB,
      },
    ),
  }));
});

test('Missing column in junction relationship source should throw', () => {
  const tableB = table('tableB')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const junctionTable = table('junctionTable')
    .columns({
      id: number(),
      aID: number(),
      bID: number(),
    })
    .primaryKey('id');

  const tableA = table('tableA')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const tableARelationships = relationships(tableA, connect => ({
    relationAToB: connect(
      {
        sourceField: ['id'],
        destField: ['aID'],
        destSchema: junctionTable,
      },
      {
        sourceField: ['missing'],
        destField: ['id'],
        destSchema: tableB,
      },
    ),
  }));

  expect(() =>
    createSchema(1, {tableA, tableB, junctionTable}, {tableARelationships}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "tableA"."relationAToB", the source field "missing" is missing in the table schema "junctionTable"]`,
  );
});
