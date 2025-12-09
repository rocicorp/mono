import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {makeServerTransaction} from './custom.ts';
import {schema, schemaSql, seedDataSql} from './test/schema.ts';
import {Transaction} from './test/util.ts';

describe('makeSchemaQuery', () => {
  let pg: PostgresDB;

  beforeEach(async () => {
    pg = await testDBs.create('makeSchemaQuery-test');
    await pg.unsafe(schemaSql);
    await pg.unsafe(seedDataSql);
  });

  test('select', async () => {
    await pg.begin(async tx => {
      const dbTransaction = new Transaction(tx);
      const transaction = await makeServerTransaction(
        dbTransaction,
        'test-client',
        1,
        schema,
      );

      const result = await transaction.run(transaction.query.basic);
      expect(result).toEqual([{id: '1', a: 2, b: 'foo', c: true}]);

      const result2 = await transaction.run(transaction.query.names);
      expect(result2).toEqual([{id: '2', a: 3, b: 'bar', c: false}]);

      const result3 = await transaction.run(transaction.query.compoundPk);
      expect(result3).toEqual([{a: 'a', b: 1, c: 'c'}]);
    });
  });

  test('select singular', async () => {
    await pg.begin(async tx => {
      const dbTransaction = new Transaction(tx);
      const transaction = await makeServerTransaction(
        dbTransaction,
        'test-client',
        1,
        schema,
      );

      const result = await transaction.run(transaction.query.basic.one());
      expect(result).toEqual({id: '1', a: 2, b: 'foo', c: true});
    });
  });

  test('select singular with no results', async () => {
    await pg.begin(async tx => {
      const dbTransaction = new Transaction(tx);
      const transaction = await makeServerTransaction(
        dbTransaction,
        'test-client',
        1,
        schema,
      );

      const result = await transaction.run(
        transaction.query.basic.where('id', 'non-existent').one(),
      );
      expect(result).toEqual(undefined);
    });
  });

  test('tx.query.table.run() works', async () => {
    await pg.begin(async tx => {
      const dbTransaction = new Transaction(tx);
      const transaction = await makeServerTransaction(
        dbTransaction,
        'test-client',
        1,
        schema,
      );

      // Test that tx.query.table.run() works (the fix for the bug)
      const result = await transaction.query.basic.run();
      expect(result).toEqual([{id: '1', a: 2, b: 'foo', c: true}]);
    });
  });
});
