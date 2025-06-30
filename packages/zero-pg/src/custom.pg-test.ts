/* eslint-disable @typescript-eslint/naming-convention */
import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';

import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {DBTransaction, SchemaCRUD} from '../../zql/src/mutate/custom.ts';
import {makeSchemaCRUD} from './custom.ts';
import {getServerSchema} from './schema.ts';
import {schema, schemaSql} from './test/schema.ts';
import {Transaction} from './test/util.ts';

describe('makeSchemaCRUD', () => {
  let pg: PostgresDB;
  let crudProvider: (
    tx: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<typeof schema>;

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

  const jsonRow = {
    str: 'foo',
    num: 1,
    bool: true,
    nil: null,
    obj: {foo: 'bar'},
    arr: ['a', 'b', 'c'],
  };

  const basicRow = {id: '1', a: 2, b: 'foo', c: true};

  const typesWithParamsRow = {
    id: '1',
    char: 'hello',
    varchar: 'goodbye',
    numeric: 10.1234,
    decimal: 5.01,
  };

  const typesWithParamsExpectedRow = {
    id: '1',
    // char gets padded to 10
    char: 'hello'.padEnd(10),
    varchar: 'goodbye',
    // NUMERIC(8, 3) gets truncated to 3 decimal places
    numeric: 10.123,
    decimal: 5.01,
  };

  const uuidAndEnumRow = {
    id: '123e4567-e89b-12d3-a456-426614174000',
    reference_id: '987fcdeb-a89b-12d3-a456-426614174000',
    status: 'active' as const,
    type: 'user' as const,
  };

  test('insert', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );

      await Promise.all([
        crud.basic.insert(basicRow),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
        crud.jsonCases.insert(jsonRow),
        crud.jsonbCases.insert(jsonRow),
        crud.typesWithParams.insert(typesWithParamsRow),
        crud.uuidAndEnum.insert(uuidAndEnumRow),
        crud.alternate_basic.insert(basicRow),
        crud.defaults.insert({id: '1'}),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [basicRow]),
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
        checkDb(tx, 'jsonCases', [jsonRow]),
        checkDb(tx, 'jsonbCases', [jsonRow]),
        checkDb(tx, 'types_with_params', [typesWithParamsExpectedRow]),
        checkDb(tx, 'uuidAndEnum', [uuidAndEnumRow]),
        checkDb(tx, 'alternate_schema.basic', [basicRow]),
        checkDb(tx, 'defaults', [
          {
            id: '1',
            insert: 'server-insert-default-1',
            update: null,
            insert_update: 'server-insert-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: null,
            insert_update_db_generated: 'db-insert-update-default-2',
          },
        ]),
      ]);
    });
  });

  test('insert/update/upsert with missing columns', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );

      await crud.basic.insert({id: '1', a: 2, b: 'foo'});

      await checkDb(tx, 'basic', [{id: '1', a: 2, b: 'foo', c: null}]);

      // undefined should be allowed too.
      await crud.basic.insert({id: '2', a: 2, b: 'foo', c: undefined});
      await checkDb(tx, 'basic', [
        {id: '1', a: 2, b: 'foo', c: null},
        {id: '2', a: 2, b: 'foo', c: null},
      ]);
      await crud.basic.delete({id: '2'});

      await crud.basic.update({id: '1', a: 3, b: 'bar', c: true});
      await crud.basic.update({id: '1', a: 3, b: 'bar'});
      await checkDb(tx, 'basic', [{id: '1', a: 3, b: 'bar', c: true}]);
      await crud.basic.update({id: '1', a: 3, b: 'bar', c: undefined});
      await checkDb(tx, 'basic', [{id: '1', a: 3, b: 'bar', c: true}]);

      await crud.basic.upsert({id: '1', a: 3, b: 'bar'});
      await checkDb(tx, 'basic', [{id: '1', a: 3, b: 'bar', c: true}]);
      await crud.basic.upsert({id: '1', a: 3, b: 'bar', c: undefined});
      await checkDb(tx, 'basic', [{id: '1', a: 3, b: 'bar', c: true}]);

      // zero out the column
      const row = {id: '1', a: 3, b: 'bar', c: null};
      await crud.basic.upsert(row);
      await checkDb(tx, 'basic', [row]);
      await crud.basic.update({
        ...row,
        c: true,
      });
      await checkDb(tx, 'basic', [{...row, c: true}]);
      await crud.basic.update({
        ...row,
        c: null,
      });
      await checkDb(tx, 'basic', [row]);
    });
  });

  test('upsert', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );

      await Promise.all([
        crud.basic.upsert(basicRow),
        crud.names.upsert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.upsert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.upsert(timeRow),
        crud.jsonCases.upsert(jsonRow),
        crud.jsonbCases.upsert(jsonRow),
        crud.typesWithParams.upsert(typesWithParamsRow),
        crud.uuidAndEnum.upsert(uuidAndEnumRow),
        crud.alternate_basic.upsert(basicRow),
        crud.defaults.upsert({
          id: '1',
          update: undefined,
          update_db_generated: undefined,
        }),
        crud.defaults.upsert({
          id: '2',
          update: undefined,
          update_db_generated: undefined,
        }),
        crud.defaults.upsert({
          id: '3',
          update: undefined,
          update_db_generated: undefined,
        }),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', [basicRow]),
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
        checkDb(tx, 'jsonCases', [jsonRow]),
        checkDb(tx, 'jsonbCases', [jsonRow]),
        checkDb(tx, 'types_with_params', [typesWithParamsExpectedRow]),
        checkDb(tx, 'uuidAndEnum', [uuidAndEnumRow]),
        checkDb(tx, 'alternate_schema.basic', [basicRow]),
        checkDb(tx, 'defaults', [
          {
            id: '1',
            insert: 'server-insert-default-1',
            update: null,
            insert_update: 'server-insert-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: null,
            insert_update_db_generated: 'db-insert-update-default-2',
          },
          {
            id: '2',
            insert: 'server-insert-default-1',
            update: null,
            insert_update: 'server-insert-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: null,
            insert_update_db_generated: 'db-insert-update-default-2',
          },
          {
            id: '3',
            insert: 'server-insert-default-1',
            update: null,
            insert_update: 'server-insert-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: null,
            insert_update_db_generated: 'db-insert-update-default-2',
          },
        ]),
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
        crud.jsonCases.upsert({
          ...jsonRow,
          num: 2,
          bool: false,
          obj: {foo: 'baz'},
          arr: ['d', 'e', 'f'],
        }),
        crud.jsonbCases.upsert({
          ...jsonRow,
          num: 2,
          bool: false,
          obj: {foo: 'baz'},
          arr: ['d', 'e', 'f'],
        }),
        crud.typesWithParams.upsert({
          id: '1',
          char: 'foo',
          varchar: 'bar',
          decimal: 100.5,
          numeric: 5.001,
        }),
        crud.uuidAndEnum.upsert({
          ...uuidAndEnumRow,
          status: 'inactive',
          type: 'system',
          reference_id: '987fcdeb-a89b-12d3-a456-426614174002',
        }),
        crud.alternate_basic.upsert({
          id: '1',
          a: 3,
          b: 'baz',
          c: false,
        }),
        crud.defaults.upsert({
          id: '1',
          insert: 'new-value-1',
          update: undefined,
          update_db_generated: undefined,
        }),
        crud.defaults.upsert({
          id: '3',
          insert_db_generated: 'new-value-4',
          update_db_generated: 'new-value-5',
          update: 'new-value-2',
          insert_update_db_generated: 'new-value-6',
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
        checkDb(tx, 'jsonCases', [
          {
            ...jsonRow,
            num: 2,
            bool: false,
            obj: {foo: 'baz'},
            arr: ['d', 'e', 'f'],
          },
        ]),
        checkDb(tx, 'jsonbCases', [
          {
            ...jsonRow,
            num: 2,
            bool: false,
            obj: {foo: 'baz'},
            arr: ['d', 'e', 'f'],
          },
        ]),
        checkDb(tx, 'types_with_params', [
          {
            id: '1',
            char: 'foo'.padEnd(10),
            varchar: 'bar',
            decimal: 100.5,
            numeric: 5.001,
          },
        ]),
        checkDb(tx, 'uuidAndEnum', [
          {
            ...uuidAndEnumRow,
            status: 'inactive',
            type: 'system',
            reference_id: '987fcdeb-a89b-12d3-a456-426614174002',
          },
        ]),
        checkDb(tx, 'alternate_schema.basic', [
          {id: '1', a: 3, b: 'baz', c: false},
        ]),
        checkDb(tx, 'defaults', [
          {
            id: '2',
            insert: 'server-insert-default-1',
            update: null,
            insert_update: 'server-insert-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: null,
            insert_update_db_generated: 'db-insert-update-default-2',
          },
          {
            id: '1',
            insert: 'new-value-1',
            // this value was updated because the row was upserted (updated)
            // and undefined was passed for it
            update: 'server-update-default-2',
            insert_update: 'server-update-default-3',
            insert_db_generated: 'db-insert-default-1',
            update_db_generated: 'db-update-default-3',
            insert_update_db_generated: 'db-insert-update-default-4',
          },
          {
            id: '3',
            insert: 'server-insert-default-1',
            update: 'new-value-2',
            insert_update: 'server-update-default-3',
            insert_db_generated: 'new-value-4',
            // these take the value from the trigger
            update_db_generated: 'db-update-default-3',
            insert_update_db_generated: 'db-insert-update-default-4',
          },
        ]),
      ]);
    });
  });

  test('update', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );
      await Promise.all([
        crud.basic.insert(basicRow),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
        crud.jsonCases.insert(jsonRow),
        crud.jsonbCases.insert(jsonRow),
        crud.typesWithParams.insert(typesWithParamsRow),
        crud.uuidAndEnum.insert(uuidAndEnumRow),
        crud.alternate_basic.insert(basicRow),
        crud.defaults.insert({id: '1'}),
      ]);

      await Promise.all([
        crud.basic.update({id: '1', a: 3, b: 'baz'}),
        crud.names.update({id: '2', a: 4, b: 'qux'}),
        crud.compoundPk.update({a: 'a', b: 1, c: 'd'}),
        crud.dateTypes.update({
          ...timeRow,
          tstz: new Date('2027-05-05T00:00:01Z').getTime(),
        }),
        crud.jsonCases.update({
          ...jsonRow,
          num: 2,
          bool: false,
          obj: {foo: 'baz'},
          arr: ['d', 'e', 'f'],
        }),
        crud.jsonbCases.update({
          ...jsonRow,
          num: 2,
          bool: false,
          obj: {foo: 'baz'},
          arr: ['d', 'e', 'f'],
        }),
        crud.typesWithParams.update({
          id: '1',
          char: 'foo',
          varchar: 'bar',
          decimal: 100.5,
          numeric: 5.001,
        }),
        crud.uuidAndEnum.update({
          id: uuidAndEnumRow.id,
          status: 'pending',
          type: 'admin',
          reference_id: '987fcdeb-a89b-12d3-a456-426614174002',
        }),
        crud.alternate_basic.update({
          id: '1',
          a: 3,
          b: 'baz',
        }),
        crud.defaults.update({
          id: '1',
          insert_db_generated: 'update-value-from-inline-test-44',
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
        checkDb(tx, 'jsonCases', [
          {
            ...jsonRow,
            num: 2,
            bool: false,
            obj: {foo: 'baz'},
            arr: ['d', 'e', 'f'],
          },
        ]),
        checkDb(tx, 'jsonbCases', [
          {
            ...jsonRow,
            num: 2,
            bool: false,
            obj: {foo: 'baz'},
            arr: ['d', 'e', 'f'],
          },
        ]),
        checkDb(tx, 'types_with_params', [
          {
            id: '1',
            char: 'foo'.padEnd(10),
            varchar: 'bar',
            decimal: 100.5,
            numeric: 5.001,
          },
        ]),
        checkDb(tx, 'uuidAndEnum', [
          {
            ...uuidAndEnumRow,
            status: 'pending',
            type: 'admin',
            reference_id: '987fcdeb-a89b-12d3-a456-426614174002',
          },
        ]),
        checkDb(tx, 'alternate_schema.basic', [
          {id: '1', a: 3, b: 'baz', c: true},
        ]),
        checkDb(tx, 'defaults', [
          {
            id: '1',
            insert: 'server-insert-default-1',
            update: 'server-update-default-2',
            insert_update: 'server-update-default-3',
            insert_db_generated: 'update-value-from-inline-test-44',
            update_db_generated: 'db-update-default-3',
            insert_update_db_generated: 'db-insert-update-default-4',
          },
        ]),
      ]);
    });
  });

  test('delete', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );

      await Promise.all([
        crud.basic.insert(basicRow),
        crud.names.insert({id: '2', a: 3, b: 'bar', c: false}),
        crud.compoundPk.insert({a: 'a', b: 1, c: 'c'}),
        crud.dateTypes.insert(timeRow),
        crud.jsonCases.insert(jsonRow),
        crud.jsonbCases.insert(jsonRow),
        crud.typesWithParams.insert(typesWithParamsRow),
        crud.uuidAndEnum.insert(uuidAndEnumRow),
        crud.alternate_basic.insert(basicRow),
        crud.defaults.insert({id: '1'}),
      ]);

      await Promise.all([
        crud.basic.delete({id: '1'}),
        crud.names.delete({id: '2'}),
        crud.compoundPk.delete({a: 'a', b: 1}),
        crud.dateTypes.delete({ts: timeRow.ts}),
        crud.jsonCases.delete({str: jsonRow.str}),
        crud.jsonbCases.delete({str: jsonRow.str}),
        crud.typesWithParams.delete({id: typesWithParamsRow.id}),
        crud.uuidAndEnum.delete({id: uuidAndEnumRow.id}),
        crud.alternate_basic.delete({id: '1'}),
        crud.defaults.delete({id: '1'}),
      ]);

      await Promise.all([
        checkDb(tx, 'basic', []),
        checkDb(tx, 'divergent_names', []),
        checkDb(tx, 'compoundPk', []),
        checkDb(tx, 'dateTypes', []),
        checkDb(tx, 'jsonCases', []),
        checkDb(tx, 'jsonbCases', []),
        checkDb(tx, 'types_with_params', []),
        checkDb(tx, 'uuidAndEnum', []),
        checkDb(tx, 'alternate_schema.basic', []),
        checkDb(tx, 'defaults', []),
      ]);
    });
  });

  test('insert/update/upsert with default columns', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const crud = crudProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );

      // Test insert with minimal columns - defaults should be applied
      await crud.defaults.insert({
        id: '1',
      });
      await checkDb(tx, 'defaults', [
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
      ]);

      // Test insert with some overrides
      await crud.defaults.insert({
        id: '2',
        insert: 'explicit_server_update',
        insert_db_generated: 'explicit_no_server_update',
      });
      await checkDb(tx, 'defaults', [
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
        {
          id: '2',
          insert: 'explicit_server_update',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'explicit_no_server_update',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
      ]);

      // Test update with missing columns - update defaults should be applied
      await crud.defaults.update({id: '1'});
      await checkDb(tx, 'defaults', [
        {
          id: '2',
          insert: 'explicit_server_update',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'explicit_no_server_update',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: 'server-update-default-2',
          insert_update: 'server-update-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
      ]);

      // Test update with explicit values overriding defaults
      await crud.defaults.update({
        id: '2',
        insert_update_db_generated: 'overridden_update',
        update_db_generated: 'overridden_db_default',
        update: 'overridden_update-2',
        insert_update: 'overridden_update-3',
      });
      await checkDb(tx, 'defaults', [
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: 'server-update-default-2',
          insert_update: 'server-update-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
        {
          id: '2',
          insert: 'explicit_server_update',
          update: 'overridden_update-2',
          insert_update: 'overridden_update-3',
          insert_db_generated: 'explicit_no_server_update',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
      ]);

      // Test upsert on non-existing row (insert behavior)
      await crud.defaults.upsert({
        id: '3',
        update: undefined,
        update_db_generated: undefined,
      });

      // Test upsert on existing row (update behavior)
      await crud.defaults.upsert({
        id: '1',
        update: undefined,
        update_db_generated: undefined,
      });

      await checkDb(tx, 'defaults', [
        {
          id: '2',
          insert: 'explicit_server_update',
          update: 'overridden_update-2',
          insert_update: 'overridden_update-3',
          insert_db_generated: 'explicit_no_server_update',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
        {
          id: '3',
          insert: 'server-insert-default-1',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: 'server-update-default-2',
          insert_update: 'server-update-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
      ]);

      // Test with only required fields - should use defaults for the rest
      await crud.defaults.insert({
        id: '4',
      });
      await checkDb(tx, 'defaults', [
        {
          id: '2',
          insert: 'explicit_server_update',
          update: 'overridden_update-2',
          insert_update: 'overridden_update-3',
          insert_db_generated: 'explicit_no_server_update',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
        {
          id: '3',
          insert: 'server-insert-default-1',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
        {
          id: '1',
          insert: 'server-insert-default-1',
          update: 'server-update-default-2',
          insert_update: 'server-update-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: 'db-update-default-3',
          insert_update_db_generated: 'db-insert-update-default-4',
        },
        {
          id: '4',
          insert: 'server-insert-default-1',
          update: null,
          insert_update: 'server-insert-default-3',
          insert_db_generated: 'db-insert-default-1',
          update_db_generated: null,
          insert_update_db_generated: 'db-insert-update-default-2',
        },
      ]);
    });
  });
});

async function checkDb(pg: PostgresDB, table: string, expected: unknown[]) {
  const rows = await pg`SELECT * FROM ${pg(table)}`;
  expect(rows).toEqual(expected);
}
