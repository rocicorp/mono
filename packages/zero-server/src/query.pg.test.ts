import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {DBTransaction, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {asRunnableQuery} from '../../zql/src/query/runnable-query.ts';
import {makeSchemaQuery} from './query.ts';
import {getServerSchema} from './schema.ts';
import {schema, schemaSql, seedDataSql} from './test/schema.ts';
import {Transaction} from './test/util.ts';

describe('makeSchemaQuery', () => {
  let pg: PostgresDB;
  let queryProvider: (
    tx: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<typeof schema, unknown>;

  beforeEach(async () => {
    pg = await testDBs.create('makeSchemaQuery-test');
    await pg.unsafe(schemaSql);
    await pg.unsafe(seedDataSql);

    queryProvider = makeSchemaQuery(schema);
  });

  test('select', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const serverSchema = await getServerSchema(transaction, schema);
      const query = queryProvider(transaction, serverSchema);
      const result = await asRunnableQuery(query.basic).run();
      expect(result).toEqual([{id: '1', a: 2, b: 'foo', c: true}]);

      const result2 = await asRunnableQuery(query.names).run();
      expect(result2).toEqual([{id: '2', a: 3, b: 'bar', c: false}]);

      const result3 = await asRunnableQuery(query.compoundPk).run();
      expect(result3).toEqual([{a: 'a', b: 1, c: 'c'}]);
    });
  });

  test('select singular', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const query = queryProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );
      const result = await asRunnableQuery(query.basic.one()).run();
      expect(result).toEqual({id: '1', a: 2, b: 'foo', c: true});
    });
  });

  test('select singular with no results', async () => {
    await pg.begin(async tx => {
      const transaction = new Transaction(tx);
      const query = queryProvider(
        transaction,
        await getServerSchema(transaction, schema),
      );
      const result = await asRunnableQuery(
        query.basic.where('id', 'non-existent').one(),
      ).run();
      expect(result).toEqual(undefined);
    });
  });
});
