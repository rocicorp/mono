import {expect, expectTypeOf, test} from 'vitest';
import {relationships} from './builder/relationship-builder.ts';
import {createSchema} from './builder/schema-builder.ts';
import {number, string, table} from './builder/table-builder.ts';

test('Key name does not matter', () => {
  const schema = createSchema({
    tables: [table('bar').columns({id: string()}).primaryKey('id')],
  });

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
    createSchema({tables: [table('foo').columns({id: string()})]}),
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
    barRelation: connect.many({
      sourceField: ['barID'],
      destField: ['id'],
      destSchema: bar,
    }),
  }));

  expect(() =>
    createSchema({tables: [foo], relationships: [fooRelationships]}),
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
    relationBToA: connect.many({
      sourceField: ['aID'],
      destField: ['id'],
      destSchema: tableA,
    }),
  }));

  const tableCRelationships = relationships(tableC, connect => ({
    relationCToB: connect.many(
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
    createSchema({
      tables: [tableB, tableC],
      relationships: [tableBRelationships, tableCRelationships],
    }),
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
    barRelation: connect.many({
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
    barRelation: connect.many({
      // @ts-expect-error - missing column
      sourceField: ['missing'],
      destField: ['id'],
      destSchema: bar,
    }),
  }));

  expect(() =>
    createSchema({tables: [bar, foo], relationships: [fooRelationships]}),
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
    relationAToB: connect.many(
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
    relationAToB: connect.many(
      {
        sourceField: ['id'],
        destField: ['aID'],
        destSchema: junctionTable,
      },
      {
        // @ts-expect-error - missing column
        sourceField: ['missing'],
        destField: ['id'],
        destSchema: tableB,
      },
    ),
  }));

  expect(() =>
    createSchema({
      tables: [tableA, tableB, junctionTable],
      relationships: [tableARelationships],
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "tableA"."relationAToB", the source field "missing" is missing in the table schema "junctionTable"]`,
  );
});

test('Two-hop one-to-one relationship works (invoice_line -> invoice -> customer)', () => {
  const customer = table('customer')
    .columns({
      id: number(),
      name: string(),
    })
    .primaryKey('id');

  const invoice = table('invoice')
    .columns({
      id: number(),
      customerID: number(),
    })
    .primaryKey('id');

  const invoiceLine = table('invoice_line')
    .columns({
      id: number(),
      invoiceID: number(),
    })
    .primaryKey('id');

  const invoiceLineRelationships = relationships(invoiceLine, connect => ({
    customer: connect.one(
      {
        sourceField: ['invoiceID'],
        destField: ['id'],
        destSchema: invoice,
      },
      {
        sourceField: ['customerID'],
        destField: ['id'],
        destSchema: customer,
      },
    ),
  }));

  const schema = createSchema({
    tables: [customer, invoice, invoiceLine],
    relationships: [invoiceLineRelationships],
  });

  expect(schema.relationships.invoice_line.customer).toMatchObject([
    {
      sourceField: ['invoiceID'],
      destField: ['id'],
      destSchema: 'invoice',
      cardinality: 'one',
    },
    {
      sourceField: ['customerID'],
      destField: ['id'],
      destSchema: 'customer',
      cardinality: 'one',
    },
  ]);
});

test('Missing column in two-hop one-to-one relationship source should throw', () => {
  const customer = table('customer')
    .columns({
      id: number(),
    })
    .primaryKey('id');

  const invoice = table('invoice')
    .columns({
      id: number(),
      customerID: number(),
    })
    .primaryKey('id');

  const invoiceLine = table('invoice_line')
    .columns({
      id: number(),
      invoiceID: number(),
    })
    .primaryKey('id');

  const invoiceLineRelationships = relationships(invoiceLine, connect => ({
    customer: connect.one(
      {
        sourceField: ['invoiceID'],
        destField: ['id'],
        destSchema: invoice,
      },
      {
        // @ts-expect-error - missing column
        sourceField: ['missing'],
        destField: ['id'],
        destSchema: customer,
      },
    ),
  }));

  expect(() =>
    createSchema({
      tables: [customer, invoice, invoiceLine],
      relationships: [invoiceLineRelationships],
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: For relationship "invoice_line"."customer", the source field "missing" is missing in the table schema "invoice"]`,
  );
});
