/* eslint-disable @typescript-eslint/naming-convention */
import {testDBs} from '../../zero-cache/src/test/db.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';

import {makeSchemaCRUD} from './custom.ts';
import {Transaction} from './test/util.ts';
import type {DBTransaction, SchemaCRUD} from '../../zql/src/mutate/custom.ts';
import {schema, schemaSql} from './test/schema.ts';

describe('makeSchemaCRUD', () => {
  let pg: PostgresDB;
  let crudProvider: (tx: DBTransaction<unknown>) => SchemaCRUD<typeof schema>;

  beforeEach(async () => {
    pg = await testDBs.create('makeSchemaCRUD-test');
    await pg.unsafe(schemaSql);

    crudProvider = makeSchemaCRUD(schema);
  });

  const timeRow = {
    ts: new Date('2025-05-05T00:00:00Z').getTime(),
    tstz: new Date('2025-06-06T00:00:00Z').getTime(),
    tswtz: new Date('2025-07-07T00:00:00Z').getTime(),
    tswotz: new Date('2025-08-08T00:00:01Z').getTime(),
    d: new Date('2025-09-09T00:00:00Z').getTime(),
  };

  test('insert', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));

      await Promise.all([
        crud.basic.insert({id: '1', a: 2, b: 'foo', c: true}),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [{id: '1', a: 2, b: 'foo', c: true}]),
        checkDb(tx, 'divergent_names', [
          {
            divergent_id: '2',
            divergent_a: 3,
            divergent_b: 'bar',
            divergent_c: false,
          },
        ]),
        checkDb(tx, 'compoundPk', [{a: 'a', b: 1, c: 'c'}]),
        checkDb(tx, 'dateTypes', [timeRow]),
      ]);
    });
  });

  test('insert with missing columns', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));
      await crud.basic.insert({id: '1', a: 2, b: 'foo'});

      await checkDb(tx, 'basic', [{id: '1', a: 2, b: 'foo', c: null}]);
    });
  });

  test('upsert', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));
      await Promise.all([
        crud.basic.upsert({id: '1', a: 2, b: 'foo', c: true}),
        crud.names.upsert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.upsert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.upsert(timeRow),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [{id: '1', a: 2, b: 'foo', c: true}]),
        checkDb(tx, 'divergent_names', [
          {
            divergent_id: '2',
            divergent_a: 3,
            divergent_b: 'bar',
            divergent_c: false,
          },
        ]),
        checkDb(tx, 'compoundPk', [{a: 'a', b: 1, c: 'c'}]),
        checkDb(tx, 'dateTypes', [timeRow]),
      ]);

      // upsert all the existing rows to change non-primary key values
      await Promise.all([
        crud.basic.upsert({id: '1', a: 3, b: 'baz', c: false}),
        crud.names.upsert({id: '2', a: 4, b: 'qux', c: true}),
        crud.compoundPk.upsert({a: 'a', b: 1, c: 'd'}),
        crud.dateTypes.upsert({
          ...timeRow,
          tstz: new Date('2026-05-05T00:00:01Z').getTime(),
        }),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [{id: '1', a: 3, b: 'baz', c: false}]),
        checkDb(tx, 'divergent_names', [
          {
            divergent_id: '2',
            divergent_a: 4,
            divergent_b: 'qux',
            divergent_c: true,
          },
        ]),
        checkDb(tx, 'compoundPk', [{a: 'a', b: 1, c: 'd'}]),
        checkDb(tx, 'dateTypes', [
          {
            ...timeRow,
            tstz: new Date('2026-05-05T00:00:01Z').getTime(),
          },
        ]),
      ]);
    });
  });

  test('update', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));
      await Promise.all([
        crud.basic.insert({id: '1', a: 2, b: 'foo', c: true}),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
      ]);

      await Promise.all([
        crud.basic.update({id: '1', a: 3, b: 'baz'}),
        crud.names.update({id: '2', a: 4, b: 'qux'}),
        crud.compoundPk.update({a: 'a', b: 1, c: 'd'}),
        crud.dateTypes.update({
          ...timeRow,
          tstz: new Date('2027-05-05T00:00:01Z').getTime(),
        }),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [{id: '1', a: 3, b: 'baz', c: true}]),
        checkDb(tx, 'divergent_names', [
          {
            divergent_id: '2',
            divergent_a: 4,
            divergent_b: 'qux',
            divergent_c: false,
          },
        ]),
        checkDb(tx, 'compoundPk', [{a: 'a', b: 1, c: 'd'}]),
        checkDb(tx, 'dateTypes', [
          {
            ...timeRow,
            tstz: new Date('2027-05-05T00:00:01Z').getTime(),
          },
        ]),
      ]);
    });
  });

  test('delete', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));
      await Promise.all([
        crud.basic.insert({id: '1', a: 2, b: 'foo', c: true}),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
      ]);

      await Promise.all([
        crud.basic.delete({id: '1'}),
        crud.names.delete({id: '2'}),
        crud.compoundPk.delete({a: 'a', b: 1}),
        crud.dateTypes.delete({ts: timeRow.ts}),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', []),
        checkDb(tx, 'divergent_names', []),
        checkDb(tx, 'compoundPk', []),
        checkDb(tx, 'dateTypes', []),
      ]);
    });
  });
});

async function checkDb(pg: PostgresDB, table: string, expected: unknown[]) {
  const rows = await pg.unsafe(`SELECT * FROM "${table}"`);
  expect(rows).toEqual(expected);
}
