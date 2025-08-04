import {beforeEach, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import {
  getClientsTableDefinition,
  getMutationsTableDefinition,
} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {zeroPostgresJS} from './adapters/postgresjs.ts';

let sql: PostgresDB;

beforeEach(async () => {
  sql = await testDBs.create('zero-pg-web');
  await sql.unsafe(`
    CREATE SCHEMA IF NOT EXISTS zero_0;
    ${getClientsTableDefinition('zero_0')}
    ${getMutationsTableDefinition('zero_0')}
  `);
});

test('update client mutation ID', async () => {
  const db = zeroPostgresJS(
    {
      relationships: {},
      tables: {},
    },
    sql,
  );

  await db.transaction(
    async (_tx, hooks) => {
      await hooks.updateClientMutationID();
    },
    {
      upstreamSchema: 'zero_0',
      clientGroupID: 'cg1',
      clientID: 'c1',
      mutationID: 1,
    },
  );

  // query for the mid
  const result =
    await sql`SELECT "lastMutationID" FROM zero_0.clients WHERE "clientGroupID" = 'cg1' AND "clientID" = 'c1'`;

  // expect the mid to be 1
  expect(result).toEqual([{lastMutationID: BigInt(1)}]);
});

test('write mutation result', async () => {
  const db = zeroPostgresJS(
    {
      relationships: {},
      tables: {},
    },
    sql,
  );

  await db.transaction(
    async (_tx, hooks) => {
      await hooks.writeMutationResult({
        id: {
          clientID: 'c1',
          id: 1,
        },
        result: {data: {foo: 'bar'}},
      });
    },
    {
      upstreamSchema: 'zero_0',
      clientGroupID: 'cg1',
      clientID: 'c1',
      mutationID: 1,
    },
  );

  // query for the mutation result
  const result =
    await sql`SELECT "result" FROM zero_0.mutations WHERE "clientGroupID" = 'cg1' AND "clientID" = 'c1' AND "mutationID" = 1`;
  // expect the result to match
  expect(result).toEqual([{result: {data: {foo: 'bar'}}}]);
});
