import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {CRUDOp} from '../../zero-protocol/src/push.ts';
import {executeCrudOps} from './crud-ops.ts';
import {CRUDMutatorFactory} from './custom.ts';
import {schema, schemaSql} from './test/schema.ts';
import {Transaction} from './test/util.ts';

describe('executeCrudOps', () => {
  let pg: PostgresDB;
  let factory: CRUDMutatorFactory<typeof schema>;

  beforeEach(async () => {
    pg = await testDBs.create('executeCrudOps-test');
    await pg.unsafe(schemaSql);
    factory = new CRUDMutatorFactory(schema);
  });

  const basicRow = {id: '1', a: 2, b: 'foo', c: true};

  test('insert operation', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      const ops: CRUDOp[] = [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
      ];

      await executeCrudOps(tx, ops);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([basicRow]);
    });
  });

  test('upsert operation - insert new row', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      const ops: CRUDOp[] = [
        {
          op: 'upsert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
      ];

      await executeCrudOps(tx, ops);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([basicRow]);
    });
  });

  test('upsert operation - update existing row', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      // First insert
      await executeCrudOps(tx, [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
      ]);

      // Then upsert with updated values
      const updatedRow = {id: '1', a: 3, b: 'bar', c: false};
      await executeCrudOps(tx, [
        {
          op: 'upsert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: updatedRow,
        },
      ]);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([updatedRow]);
    });
  });

  test('update operation', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      // First insert
      await executeCrudOps(tx, [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
      ]);

      // Then update
      const ops: CRUDOp[] = [
        {
          op: 'update',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '1', a: 3, b: 'bar'},
        },
      ];

      await executeCrudOps(tx, ops);

      const rows = await pgTx`SELECT * FROM basic`;
      // c should remain unchanged from original insert
      expect(rows).toEqual([{id: '1', a: 3, b: 'bar', c: true}]);
    });
  });

  test('delete operation', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      // First insert
      await executeCrudOps(tx, [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
      ]);

      // Then delete
      const ops: CRUDOp[] = [
        {
          op: 'delete',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '1'},
        },
      ];

      await executeCrudOps(tx, ops);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([]);
    });
  });

  test('multiple operations in sequence', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      const ops: CRUDOp[] = [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '1', a: 1, b: 'one', c: true},
        },
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '2', a: 2, b: 'two', c: false},
        },
        {
          op: 'update',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '1', b: 'updated'},
        },
        {
          op: 'delete',
          tableName: 'basic',
          primaryKey: ['id'],
          value: {id: '2'},
        },
      ];

      await executeCrudOps(tx, ops);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([{id: '1', a: 1, b: 'updated', c: true}]);
    });
  });

  test('operations across multiple tables', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      const ops: CRUDOp[] = [
        {
          op: 'insert',
          tableName: 'basic',
          primaryKey: ['id'],
          value: basicRow,
        },
        {
          op: 'insert',
          tableName: 'compoundPk',
          primaryKey: ['a', 'b'],
          value: {a: 'x', b: 1, c: 'y'},
        },
      ];

      await executeCrudOps(tx, ops);

      const basicRows = await pgTx`SELECT * FROM basic`;
      expect(basicRows).toEqual([basicRow]);

      const compoundRows = await pgTx`SELECT * FROM "compoundPk"`;
      expect(compoundRows).toEqual([{a: 'x', b: 1, c: 'y'}]);
    });
  });

  test('empty ops array does nothing', async () => {
    await pg.begin(async pgTx => {
      const dbTransaction = new Transaction(pgTx);
      const tx = await factory.createTransaction(dbTransaction, 'client1', 1);

      await executeCrudOps(tx, []);

      const rows = await pgTx`SELECT * FROM basic`;
      expect(rows).toEqual([]);
    });
  });
});
