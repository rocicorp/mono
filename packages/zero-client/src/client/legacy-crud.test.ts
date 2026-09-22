import {expect, test} from 'vitest';
import type {CRUDOp} from '../../../zero-protocol/src/mutation.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {addTableCRUDProperties, makeBatchCRUDMutate} from './crud.ts';

// A column stored as epoch millis but exposed to the app as a Date.
const schema = createSchema({
  enableLegacyMutators: true,
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

test('legacy batch CRUD encodes codec columns into the op', async () => {
  const ops: CRUDOp[] = [];
  const event = makeBatchCRUDMutate<typeof schema.tables.event>(
    'event',
    schema,
    ops,
  );
  await event.insert({id: 'a', at: new Date(1000)});
  await event.upsert({id: 'b', at: new Date(2000)});
  await event.update({id: 'a', at: new Date(3000)});
  await event.delete({id: 'a'});

  expect(ops.map(op => op.value)).toEqual([
    {id: 'a', at: 1000},
    {id: 'b', at: 2000},
    {id: 'a', at: 3000},
    {id: 'a'},
  ]);
});

test('legacy table CRUD properties encode codec columns into the op', async () => {
  const sent: CRUDOp[][] = [];
  const mutate: Record<string, unknown> = {};
  addTableCRUDProperties(schema, mutate, {
    _zero_crud: arg => {
      sent.push([...arg.ops]);
      return Promise.resolve();
    },
  });
  const event = mutate.event as {
    insert: (v: {id: string; at: Date}) => Promise<void>;
  };
  await event.insert({id: 'a', at: new Date(4000)});
  expect(sent).toEqual([
    [
      {
        op: 'insert',
        tableName: 'event',
        primaryKey: ['id'],
        value: {id: 'a', at: 4000},
      },
    ],
  ]);
});
