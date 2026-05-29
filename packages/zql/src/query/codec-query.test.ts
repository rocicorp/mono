import {expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {InsertValue, UpdateValue} from '../mutate/crud.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import type {Row} from './query.ts';

const event = table('event')
  .columns({
    id: string(),
    at: number().codec<Date>({
      decode: (n: number) => new Date(n),
      encode: (d: Date) => d.getTime(),
    }),
  })
  .primaryKey('id');

const schema = createSchema({tables: [event]});

function ast(q: unknown) {
  // oxlint-disable-next-line no-explicit-any
  return asQueryInternals(q as any).ast;
}

test('row type decodes codec column to the Decoded type', () => {
  expectTypeOf<Row<typeof schema.tables.event>>().toEqualTypeOf<{
    readonly id: string;
    readonly at: Date;
  }>();
});

test('insert/update value types use the Decoded type', () => {
  expectTypeOf<
    InsertValue<typeof schema.tables.event>['at']
  >().toEqualTypeOf<Date>();
  expectTypeOf<UpdateValue<typeof schema.tables.event>['at']>().toEqualTypeOf<
    Date | undefined
  >();
});

test('where encodes a codec literal to its stored value', () => {
  const q = newQuery(schema, 'event').where('at', '>', new Date(1000));
  const where = ast(q).where;
  expect(where).toMatchObject({
    type: 'simple',
    op: '>',
    left: {type: 'column', name: 'at'},
    right: {type: 'literal', value: 1000},
  });
});

test('where encodes codec literals in IN arrays', () => {
  const q = newQuery(schema, 'event').where('at', 'IN', [
    new Date(1),
    new Date(2),
  ]);
  const where = ast(q).where;
  expect(where).toMatchObject({
    right: {type: 'literal', value: [1, 2]},
  });
});

test('where does not encode non-codec columns', () => {
  const q = newQuery(schema, 'event').where('id', '=', 'abc');
  const where = ast(q).where;
  expect(where).toMatchObject({
    right: {type: 'literal', value: 'abc'},
  });
});
