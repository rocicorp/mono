import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {
  PostgresDB,
  PostgresTransaction,
} from '../../zero-cache/src/types/pg.ts';
import {makeQueryRun} from './query-runner.ts';
import {makeSchemaQuery} from './query.ts';
import {getServerSchema} from './schema.ts';
import {schema, schemaSql, seedDataSql} from './test/schema.ts';
import {Transaction} from './test/util.ts';

describe('makeSchemaQuery', () => {
  type Context = {userID: string};
  let pg: PostgresDB;
  let queryProvider: ReturnType<typeof makeSchemaQuery<typeof schema, Context>>;
  let runProvider: ReturnType<typeof makeQueryRun<typeof schema, Context>>;

  async function getQueryAndRun(tx: PostgresTransaction) {
    const transaction = new Transaction(tx);
    const serverSchema = await getServerSchema(transaction, schema);
    return {
      query: queryProvider(transaction, serverSchema),
      run: runProvider(transaction, {userID: 'user1'}),
    };
  }

  beforeEach(async () => {
    pg = await testDBs.create('makeSchemaQuery-test');
    await pg.unsafe(schemaSql);
    await pg.unsafe(seedDataSql);

    queryProvider = makeSchemaQuery(schema);
    runProvider = makeQueryRun(schema);
  });

  test('select', async () => {
    await pg.begin(async tx => {
      const {query, run} = await getQueryAndRun(tx);

      const result = await run(query.basic);

      expect(result).toEqual([{id: '1', a: 2, b: 'foo', c: true}]);

      const result2 = await run(query.names);
      expect(result2).toEqual([{id: '2', a: 3, b: 'bar', c: false}]);

      const result3 = await run(query.compoundPk);
      expect(result3).toEqual([{a: 'a', b: 1, c: 'c'}]);
    });
  });

  test('select singular', async () => {
    await pg.begin(async tx => {
      const {query, run} = await getQueryAndRun(tx);
      const result = await run(query.basic.one());
      expect(result).toEqual({id: '1', a: 2, b: 'foo', c: true});
    });
  });

  test('select singular with no results', async () => {
    await pg.begin(async tx => {
      const {query, run} = await getQueryAndRun(tx);
      const result = await run(query.basic.where('id', 'non-existent').one());
      expect(result).toEqual(undefined);
    });
  });
});
