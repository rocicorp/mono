import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {makeCRUDMutate, type CRUDKind} from './crud.ts';

const schema = createSchema({
  tables: [
    table('event')
      .columns({
        id: string(),
        at: number().codec<Date>({
          decode: (n: number) => new Date(n),
          encode: (d: Date) => d.getTime(),
        }),
      })
      .primaryKey('id'),
  ],
});

test('the callable and table-property CRUD forms both encode codec columns', async () => {
  const calls: [string, CRUDKind, unknown][] = [];
  const mutate = makeCRUDMutate(schema, true, (table, kind, args) => {
    calls.push([table, kind, args]);
    return Promise.resolve();
  });

  await mutate({
    schema,
    table: 'event',
    kind: 'insert',
    args: {id: 'a', at: new Date(1000)},
  });
  await mutate.event.insert({id: 'b', at: new Date(2000)});

  expect(calls).toEqual([
    ['event', 'insert', {id: 'a', at: 1000}],
    ['event', 'insert', {id: 'b', at: 2000}],
  ]);
});
