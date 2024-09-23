import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Mode} from 'zero-cache/src/db/transaction-pool.js';
import {MutationType, type CRUDMutation} from 'zero-protocol';
import {expectTables, testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {processMutation} from './mutagen.js';

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

  test(
    'new client with no last mutation id',
    async () => {
      await expectTables(db, {
        idonly: [],
        ['zero.clients']: [],
      });

      const error = await processMutation(
        undefined,
        db,
        'abc',
        {
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
        },
        {},
      );

      expect(error).undefined;

      await expectTables(db, {
        idonly: [{id: '1'}],
        ['zero.clients']: [
          {
            clientGroupID: 'abc',
            clientID: '123',
            lastMutationID: 1n,
            userID: null,
          },
        ],
      });
    },
    {},
  );

  test('next sequential mutation for previously seen client', async () => {
    await db`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
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
      },
      {},
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 3n,
          userID: null,
        },
      ],
    });
  });

  test('old mutations are skipped', async () => {
    await db`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
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
      },
      {},
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 2n,
          userID: null,
        },
      ],
    });
  });

  test('old mutations that would have errored are skipped', async () => {
    await db`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID")
        VALUES ('abc', '123', 2);
      INSERT INTO idonly (id) VALUES ('1');
      `.simple();

    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
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
                id: {id: '1'}, // This would result in a duplicate key value if applied.
                value: {},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      {},
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 2n,
          userID: null,
        },
      ],
    });
  });

  test('mutation id too far in the future throws', async () => {
    await db`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;

    await expect(
      processMutation(
        undefined,
        db,
        'abc',
        {
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
        },
        {},
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[Error: ["error","InvalidPush","Push contains unexpected mutation id 3 for client 123. Expected mutation id 2."]]`,
    );

    await expectTables(db, {
      idonly: [],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('process create, set, update, delete all at once', async () => {
    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
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
      } satisfies CRUDMutation,
      {},
    );

    expect(error).undefined;

    await expectTables(db, {
      ['id_and_cols']: [
        {
          id: '1',
          col1: 'update',
          col2: 'set',
        },
      ],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('fk failure', async () => {
    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
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
      } satisfies CRUDMutation,
      {},
    );

    expect(error).toEqual(
      'PostgresError: insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
    );
    console.log(error);

    await expectTables(db, {
      ['fk_ref']: [],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('retries on serialization error', async () => {
    const {promise, resolve} = resolver();
    await db`
      INSERT INTO zero.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    // Start a concurrent mutation that bumps the lmid from 2 => 3.
    void db.begin(Mode.SERIALIZABLE, async tx => {
      // Simulate holding a lock on the row.
      tx`SELECT * FROM zero.clients WHERE "clientGroupID" = 'abc' AND "clientID" = '123'`;

      await promise;

      // Update the row on signal.
      return tx`
      UPDATE zero.clients SET "lastMutationID" = 3 WHERE "clientGroupID" = 'abc'`;
    });

    const error = await processMutation(
      undefined,
      db,
      'abc',
      {
        type: MutationType.CRUD,
        id: 4,
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
      },
      {},
      resolve, // Finish the 2 => 3 transaction only after this 3 => 4 transaction begins.
    );

    expect(error).undefined;

    // 3 => 4 should succeed after internally retrying.
    await expectTables(db, {
      idonly: [{id: '1'}],
      ['zero.clients']: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 4n,
          userID: null,
        },
      ],
    });
  });
});
