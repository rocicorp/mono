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

test('expression-builder cmp encodes codec literals', () => {
  const q = newQuery(schema, 'event').where(({cmp, and}) =>
    and(cmp('at', '<=', new Date(500)), cmp('id', '=', 'x')),
  );
  const where = ast(q).where;
  expect(where).toMatchObject({
    type: 'and',
    conditions: [
      {op: '<=', left: {name: 'at'}, right: {type: 'literal', value: 500}},
      {op: '=', left: {name: 'id'}, right: {type: 'literal', value: 'x'}},
    ],
  });
});

test('start encodes codec columns of the start row', () => {
  // Start rows come from (decoded) query results, so `at` arrives as a Date.
  const q = newQuery(schema, 'event')
    .orderBy('at', 'asc')
    .start({id: 'a', at: new Date(1000)});
  expect(ast(q).start).toEqual({
    row: {id: 'a', at: 1000},
    exclusive: true,
  });
});

test('expression-builder 2-arg cmp encodes codec literals', () => {
  const q = newQuery(schema, 'event').where(({cmp}) => cmp('at', new Date(7)));
  const where = ast(q).where;
  expect(where).toMatchObject({
    op: '=',
    right: {type: 'literal', value: 7},
  });
});

test('LIKE-family patterns on a codec column bypass the codec', () => {
  const tagged = table('tagged')
    .columns({
      id: string(),
      // A codec whose encode would break on a plain pattern string.
      at: string().codec<Date>({
        decode: (s: string) => new Date(s),
        encode: (d: Date) => d.toISOString(),
      }),
    })
    .primaryKey('id');
  const s = createSchema({tables: [tagged]});
  for (const op of ['LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE'] as const) {
    const q = newQuery(s, 'tagged').where('at', op, '2024-%');
    expect(ast(q).where).toMatchObject({
      op,
      right: {type: 'literal', value: '2024-%'},
    });
    const qe = newQuery(s, 'tagged').where(({cmp}) => cmp('at', op, '2024-%'));
    expect(ast(qe).where).toMatchObject({
      op,
      right: {type: 'literal', value: '2024-%'},
    });
  }
  // Comparison operators still encode.
  expect(
    ast(newQuery(s, 'tagged').where('at', '=', new Date(0))).where,
  ).toMatchObject({right: {value: '1970-01-01T00:00:00.000Z'}});

  // @ts-expect-error - a LIKE pattern is a string, not the decoded Date
  newQuery(s, 'tagged').where('at', 'LIKE', new Date(0));
});
