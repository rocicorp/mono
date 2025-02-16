/* eslint-disable @typescript-eslint/naming-convention */
import {testDBs} from '../../zero-cache/src/test/db.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {DBTransaction} from './db.ts';
import {makeSchemaCRUD} from './custom.ts';
import {Transaction} from './test/util.ts';
import type {SchemaCRUD} from '../../zql/src/mutate/custom.ts';

const schema = createSchema(1, {
  tables: [
    table('basic')
      .columns({
        id: string(),
        a: number(),
        b: string(),
        c: boolean().optional(),
      })
      .primaryKey('id'),
    table('names')
      .from('divergent_names')
      .columns({
        id: string().from('divergent_id'),
        a: number().from('divergent_a'),
        b: string().from('divergent_b'),
        c: boolean().from('divergent_c').optional(),
      })
      .primaryKey('id'),
    table('compoundPk')
      .columns({
        a: string(),
        b: number(),
        c: string().optional(),
      })
      .primaryKey('a', 'b'),
  ],
  relationships: [],
});

describe('makeSchemaCRUD', () => {
  let pg: PostgresDB;
  let crudProvider: (tx: DBTransaction<unknown>) => SchemaCRUD<typeof schema>;

  beforeEach(async () => {
    pg = await testDBs.create('makeSchemaCRUD-test');
    await pg.unsafe(`
      CREATE TABLE basic (
        id TEXT PRIMARY KEY,
        a INTEGER,
        b TEXT,
        C BOOLEAN
      );

      CREATE TABLE divergent_names (
        divergent_id TEXT PRIMARY KEY,
        divergent_a INTEGER,
        divergent_b TEXT,
        divergent_c BOOLEAN
      );

      CREATE TABLE "compoundPk" (
        a TEXT,
        b INTEGER,
        c TEXT,
        PRIMARY KEY (a, b)
      );
    `);

    crudProvider = makeSchemaCRUD(schema);
  });

  test('insert', async () => {
    await pg.begin(async tx => {
      const crud = crudProvider(new Transaction(tx));
      await Promise.all([
        crud.basic.insert({id: '1', a: 2, b: 'foo', c: true}),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
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
      ]);

      // upsert all the existing rows to change non-primary key values
      await Promise.all([
        crud.basic.upsert({id: '1', a: 3, b: 'baz', c: false}),
        crud.names.upsert({id: '2', a: 4, b: 'qux', c: true}),
        crud.compoundPk.upsert({a: 'a', b: 1, c: 'd'}),
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
      ]);

      await Promise.all([
        crud.basic.update({id: '1', a: 3, b: 'baz'}),
        crud.names.update({id: '2', a: 4, b: 'qux'}),
        crud.compoundPk.update({a: 'a', b: 1, c: 'd'}),
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
      ]);

      await Promise.all([
        crud.basic.delete({id: '1'}),
        crud.names.delete({id: '2'}),
        crud.compoundPk.delete({a: 'a', b: 1}),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', []),
        checkDb(tx, 'divergent_names', []),
        checkDb(tx, 'compoundPk', []),
      ]);
    });
  });
});

async function checkDb(pg: PostgresDB, table: string, expected: unknown[]) {
  const rows = await pg.unsafe(`SELECT * FROM "${table}"`);
  expect(rows).toEqual(expected);
}
