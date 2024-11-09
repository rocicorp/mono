import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {
  ErrorKind,
  MutationType,
  type CRUDMutation,
} from '../../../../zero-protocol/src/mod.js';
import {Mode} from '../../db/transaction-pool.js';
import {expectTables, testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {processMutation} from './mutagen.js';
import type {WriteAuthorizer} from './write-authorizer.js';

const SHARD_ID = '0';

class MockWriteAuthorizer implements WriteAuthorizer {
  canInsert() {
    return true;
  }

  canUpdate() {
    return true;
  }

  canDelete() {
    return true;
  }

  canUpsert() {
    return true;
  }
}
const mockWriteAuthorizer = new MockWriteAuthorizer();

const TEST_SCHEMA_VERSION = 1;

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
      CREATE SCHEMA zero_${SHARD_ID};
      CREATE TABLE zero_${SHARD_ID}.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE SCHEMA zero;
      CREATE TABLE zero."schemaVersions" (
        "minSupportedVersion" INT4,
        "maxSupportedVersion" INT4,

        -- Ensure that there is only a single row in the table.
        -- Application code can be agnostic to this column, and
        -- simply invoke UPDATE statements on the version columns.
        "lock" BOOL PRIMARY KEY DEFAULT true,
        CONSTRAINT zero_schema_versions_single_row_constraint CHECK (lock)
      );
      INSERT INTO zero."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
        VALUES (true, 1, 1);
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
        [`zero_${SHARD_ID}.clients`]: [],
      });

      const error = await processMutation(
        undefined,
        {},
        db,
        SHARD_ID,
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
                  tableName: 'idonly',
                  primaryKey: ['id'],
                  value: {id: '1'},
                },
              ],
            },
          ],
          timestamp: Date.now(),
        },
        mockWriteAuthorizer,
        TEST_SCHEMA_VERSION,
      );

      expect(error).undefined;

      await expectTables(db, {
        idonly: [{id: '1'}],
        [`zero_${SHARD_ID}.clients`]: [
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
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      undefined,
      {},
      db,
      SHARD_ID,
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
                tableName: 'idonly',
                primaryKey: ['id'],
                value: {id: '1'},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      [`zero_${SHARD_ID}.clients`]: [
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
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      undefined,
      {},
      db,
      SHARD_ID,
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
                tableName: 'idonly',
                primaryKey: ['id'],
                value: {id: '1'},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [],
      [`zero_${SHARD_ID}.clients`]: [
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
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID")
        VALUES ('abc', '123', 2);`;
    await db`INSERT INTO idonly (id) VALUES ('1');`;

    const error = await processMutation(
      undefined,
      {},
      db,
      SHARD_ID,
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
                tableName: 'idonly',
                primaryKey: ['id'],
                value: {id: '1'}, // This would result in a duplicate key value if applied.
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      [`zero_${SHARD_ID}.clients`]: [
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
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;

    await expect(
      processMutation(
        undefined,
        {},
        db,
        SHARD_ID,
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
                  tableName: 'idonly',
                  primaryKey: ['id'],
                  value: {id: '1'},
                },
              ],
            },
          ],
          timestamp: Date.now(),
        },
        mockWriteAuthorizer,
        TEST_SCHEMA_VERSION,
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[Error: ["error","InvalidPush","Push contains unexpected mutation id 3 for client 123. Expected mutation id 2."]]`,
    );

    await expectTables(db, {
      idonly: [],
      [`zero_${SHARD_ID}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('schema version below supported range throws', async () => {
    await db`
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;

    await db`UPDATE zero."schemaVersions" SET "minSupportedVersion"=2, "maxSupportedVersion"=3`;

    await expect(
      processMutation(
        undefined,
        {},
        db,
        SHARD_ID,
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
                  tableName: 'idonly',
                  primaryKey: ['id'],
                  value: {id: '1'},
                },
              ],
            },
          ],
          timestamp: Date.now(),
        },
        mockWriteAuthorizer,
        1,
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[Error: ["error","SchemaVersionNotSupported","Schema version 1 is not in range of supported schema versions [2, 3]."]]`,
    );

    await expectTables(db, {
      idonly: [],
      [`zero_${SHARD_ID}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('schema version above supported range throws', async () => {
    await db`
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;

    await db`UPDATE zero."schemaVersions" SET "minSupportedVersion"=2, "maxSupportedVersion"=3`;

    await expect(
      processMutation(
        undefined,
        {},
        db,
        SHARD_ID,
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
                  tableName: 'idonly',
                  primaryKey: ['id'],
                  value: {id: '1'},
                },
              ],
            },
          ],
          timestamp: Date.now(),
        },
        mockWriteAuthorizer,
        4,
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[Error: ["error","SchemaVersionNotSupported","Schema version 4 is not in range of supported schema versions [2, 3]."]]`,
    );

    await expectTables(db, {
      idonly: [],
      [`zero_${SHARD_ID}.clients`]: [
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
      {},
      db,
      SHARD_ID,
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
                tableName: 'id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col1: 'create',
                  col2: 'create',
                },
              },
              {
                op: 'set',
                tableName: 'id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '2',
                  col1: 'set',
                  col2: 'set',
                },
              },
              {
                op: 'update',
                tableName: 'id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col1: 'update',
                },
              },
              {
                op: 'set',
                tableName: 'id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col2: 'set',
                },
              },
              {
                op: 'delete',
                tableName: 'id_and_cols',
                primaryKey: ['id'],
                value: {id: '2'},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      } satisfies CRUDMutation,
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
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
      [`zero_${SHARD_ID}.clients`]: [
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
      {},
      db,
      SHARD_ID,
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
                tableName: 'fk_ref',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  ref: '1',
                },
              },
            ],
          },
        ],
        timestamp: Date.now(),
      } satisfies CRUDMutation,
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
    );

    expect(error).toEqual([
      ErrorKind.MutationFailed,
      'PostgresError: insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
    ]);

    await expectTables(db, {
      ['fk_ref']: [],
      [`zero_${SHARD_ID}.clients`]: [
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
      INSERT INTO ${db(
        `zero_${SHARD_ID}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    // Start a concurrent mutation that bumps the lmid from 2 => 3.
    const done = db.begin(Mode.SERIALIZABLE, async tx => {
      // Simulate holding a lock on the row.
      tx`SELECT * FROM ${db(
        `zero_${SHARD_ID}`,
      )}.clients WHERE "clientGroupID" = 'abc' AND "clientID" = '123'`;

      await promise;

      // Update the row on signal.
      return tx`
      UPDATE ${db(
        `zero_${SHARD_ID}`,
      )}.clients SET "lastMutationID" = 3 WHERE "clientGroupID" = 'abc'`;
    });

    const error = await processMutation(
      undefined,
      {},
      db,
      SHARD_ID,
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
                tableName: 'idonly',
                primaryKey: ['id'],
                value: {
                  id: '1',
                },
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      mockWriteAuthorizer,
      TEST_SCHEMA_VERSION,
      async () => {
        // Finish the 2 => 3 transaction only after this 3 => 4 transaction begins.
        resolve();
        await done;
      },
    );

    expect(error).undefined;

    // 3 => 4 should succeed after internally retrying.
    await expectTables(db, {
      idonly: [{id: '1'}],
      [`zero_${SHARD_ID}.clients`]: [
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
