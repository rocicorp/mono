import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {processMutation, readLastMutationIDForUpdate} from './mutagen.js';

async function createTables(db: PostgresDB) {
  await db.unsafe(`
      CREATE TABLE idonly (
        id text,
        PRIMARY KEY(id)
      );
      CREATE TABLE id_and_cols (
        id text,
        col1 text,
        col2 text,
        PRIMARY KEY(id)
      );
      CREATE SCHEMA zero;
      CREATE TABLE zero.clients (
        "clientID" TEXT PRIMARY KEY,
        "lastMutationID" BIGINT
      );
    `);
}

describe('processMutation', () => {
  let db: PostgresDB;
  beforeEach(async () => {
    db = await testDBs.create('db_mutagen_test');
    await createTables(db);
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('new client with no last mutation id', async () => {
    await db.begin(async tx => {
      const mid = await readLastMutationIDForUpdate(tx, '1');
      expect(mid).toBe(0n);
    });

    await processMutation(undefined, db, {
      id: 1,
      clientID: '1',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op: 'create',
              entityType: 'idonly',
              id: {id: '1'},
              value: {},
            },
          ],
        },
      ],
      timestamp: Date.now(),
    });

    await db.begin(async tx => {
      const mid = await readLastMutationIDForUpdate(tx, '1');
      expect(mid).toBe(1n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([{id: '1'}]);
  });

  test('next sequential mutation for previously seen client', async () => {
    await db.begin(async tx => {
      await tx`INSERT INTO zero.clients ("clientID", "lastMutationID") VALUES ('1', 1)`;
    });

    await processMutation(undefined, db, {
      id: 2,
      clientID: '1',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op: 'create',
              entityType: 'idonly',
              id: {id: '1'},
              value: {},
            },
          ],
        },
      ],
      timestamp: Date.now(),
    });

    await db.begin(async tx => {
      const mid = await readLastMutationIDForUpdate(tx, '1');
      expect(mid).toBe(2n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([{id: '1'}]);
  });

  test('old mutations are skipped', async () => {
    await db.begin(async tx => {
      await tx`INSERT INTO zero.clients ("clientID", "lastMutationID") VALUES ('1', 2)`;
    });

    await processMutation(undefined, db, {
      id: 1,
      clientID: '1',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op: 'create',
              entityType: 'idonly',
              id: {id: '1'},
              value: {},
            },
          ],
        },
      ],
      timestamp: Date.now(),
    });

    await db.begin(async tx => {
      const mid = await readLastMutationIDForUpdate(tx, '1');
      expect(mid).toBe(2n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([]);
  });

  test('mutation id too far in the future throws', async () => {
    await db.begin(async tx => {
      await tx`INSERT INTO zero.clients ("clientID", "lastMutationID") VALUES ('1', 1)`;
    });

    await expect(
      processMutation(undefined, db, {
        id: 3,
        clientID: '1',
        name: '_zero_crud',
        args: [
          {
            ops: [
              {
                op: 'create',
                entityType: 'idonly',
                id: {id: '1'},
                value: {},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      }),
    ).rejects.toThrow('Mutation ID was out of order');
  });

  test('process create, set, update, delete all at once', async () => {
    await processMutation(undefined, db, {
      id: 1,
      clientID: '1',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op: 'create',
              entityType: 'id_and_cols',
              id: {id: '1'},
              value: {
                col1: 'create',
                col2: 'create',
              },
            },
            {
              op: 'set',
              entityType: 'id_and_cols',
              id: {id: '2'},
              value: {
                col1: 'set',
                col2: 'set',
              },
            },
            {
              op: 'update',
              entityType: 'id_and_cols',
              id: {id: '1'},
              partialValue: {
                col1: 'update',
              },
            },
            {
              op: 'set',
              entityType: 'id_and_cols',
              id: {id: '1'},
              value: {
                col2: 'set',
              },
            },
            {
              op: 'delete',
              entityType: 'id_and_cols',
              id: {id: '2'},
            },
          ],
        },
      ],
      timestamp: Date.now(),
    });

    const rows = await db`SELECT * FROM id_and_cols`;
    expect(rows).toEqual([
      {
        id: '1',
        col1: 'update',
        col2: 'set',
      },
    ]);
  });
});
