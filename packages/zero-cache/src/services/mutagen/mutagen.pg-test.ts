import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {processMutation, readLastMutationID} from './mutagen.js';
import {MutationType, type CRUDMutation} from 'zero-protocol';

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
      CREATE TABLE fk_ref (
        id text,
        ref text,
        PRIMARY KEY(id),
        FOREIGN KEY(ref) REFERENCES idonly(id)
      );
      CREATE SCHEMA zero;
      CREATE TABLE zero.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
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
      const mid = await readLastMutationID(tx, 'abc', '123');
      expect(mid).toBe(0n);
    });

    const error = await processMutation(undefined, db, 'abc', {
      type: MutationType.CRUD,
      id: 1,
      clientID: '123',
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

    expect(error).undefined;

    await db.begin(async tx => {
      const mid = await readLastMutationID(tx, 'abc', '123');
      expect(mid).toBe(1n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([{id: '1'}]);
  });

  test('next sequential mutation for previously seen client', async () => {
    await db.begin(async tx => {
      await tx`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 1)`;
    });

    const error = await processMutation(undefined, db, 'abc', {
      type: MutationType.CRUD,
      id: 2,
      clientID: '123',
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

    expect(error).undefined;

    await db.begin(async tx => {
      const mid = await readLastMutationID(tx, 'abc', '123');
      expect(mid).toBe(2n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([{id: '1'}]);
  });

  test('old mutations are skipped', async () => {
    await db.begin(async tx => {
      await tx`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 2)`;
    });

    const error = await processMutation(undefined, db, 'abc', {
      type: MutationType.CRUD,
      id: 1,
      clientID: '123',
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

    expect(error).undefined;

    await db.begin(async tx => {
      const mid = await readLastMutationID(tx, 'abc', '123');
      expect(mid).toBe(2n);
    });

    const rows = await db`SELECT * FROM idonly`;
    expect(rows).toEqual([]);
  });

  test('mutation id too far in the future throws', async () => {
    await db.begin(async tx => {
      await tx`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;
    });

    await expect(
      processMutation(undefined, db, 'abc', {
        type: MutationType.CRUD,
        id: 3,
        clientID: '123',
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
    const error = await processMutation(undefined, db, 'abc', {
      type: MutationType.CRUD,
      id: 1,
      clientID: '123',
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
    } satisfies CRUDMutation);

    expect(error).undefined;

    const rows = await db`SELECT * FROM id_and_cols`;
    expect(rows).toEqual([
      {
        id: '1',
        col1: 'update',
        col2: 'set',
      },
    ]);
  });

  test('fk failure', async () => {
    const error = await processMutation(undefined, db, 'abc', {
      type: MutationType.CRUD,
      id: 1,
      clientID: '123',
      name: '_zero_crud',
      args: [
        {
          ops: [
            {
              op: 'create',
              entityType: 'fk_ref',
              id: {id: '1'},
              value: {
                ref: '1',
              },
            },
          ],
        },
      ],
      timestamp: Date.now(),
    } satisfies CRUDMutation);

    expect(error).toEqual(
      'PostgresError: insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
    );
    console.log(error);

    const rows = await db`SELECT * FROM fk_ref`;
    expect(rows).toEqual([]);

    const clients = await db`SELECT * FROM zero.clients`;
    expect(clients).toEqual([
      {
        clientGroupID: 'abc',
        clientID: '123',
        lastMutationID: 1n,
        userID: null,
      },
    ]);
  });
});
