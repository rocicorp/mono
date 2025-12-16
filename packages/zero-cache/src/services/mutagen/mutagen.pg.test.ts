import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.ts';
import {
  type CRUDMutation,
  type CRUDOp,
  type UpsertOp,
} from '../../../../zero-protocol/src/push.ts';
import type {WriteAuthorizer} from '../../auth/write-authorizer.ts';
import * as Mode from '../../db/mode-enum.ts';
import {expectTables, type PgTest, test} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {zeroSchema} from './mutagen-test-shared.ts';
import {processMutation} from './mutagen.ts';

const APP_ID = 'zeeroh';
const SHARD_NUM = 0;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

class MockWriteAuthorizer implements WriteAuthorizer {
  canPreMutation() {
    return Promise.resolve(true);
  }
  reloadPermissions(): void {}
  canPostMutation() {
    return Promise.resolve(true);
  }
  normalizeOps(ops: CRUDOp[]) {
    return ops as Exclude<CRUDOp, UpsertOp>[];
  }
}
const mockWriteAuthorizer = new MockWriteAuthorizer();

async function createTables(db: PostgresDB) {
  await db.unsafe(`
      CREATE TABLE idonly (
        id text,
        PRIMARY KEY(id)
      );
      CREATE SCHEMA my_schema;
      CREATE TABLE my_schema.id_and_cols (
        id text,
        col1 text,
        col2 text,
        PRIMARY KEY(id)
      );
      CREATE TABLE types (
        id TEXT PRIMARY KEY,
        num NUMERIC,
        time1 TIMESTAMPTZ,
        time2 TIMESTAMPTZ
      );
      CREATE TABLE fk_ref (
        id text,
        ref text,
        PRIMARY KEY(id),
        FOREIGN KEY(ref) REFERENCES idonly(id)
      );
      ${zeroSchema(SHARD)}
    `);
}

describe('processMutation', {timeout: 15000}, () => {
  let lc: LogContext;
  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    db = await testDBs.create('db_mutagen_test');
    await createTables(db);

    return () => testDBs.drop(db);
  });

  test('new client with no last mutation id', async () => {
    await expectTables(db, {
      idonly: [],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [],
    });

    const error = await processMutation(
      lc,
      undefined,
      db,
      SHARD,
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
                op: 'insert',
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
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('next sequential mutation for previously seen client', async () => {
    await db`
      INSERT INTO ${db(
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      lc,
      {},
      db,
      SHARD,
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
                op: 'insert',
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
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
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
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 2)`;

    const error = await processMutation(
      lc,
      undefined,
      db,
      SHARD,
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
                op: 'insert',
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
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
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
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID")
        VALUES ('abc', '123', 2);`;
    await db`INSERT INTO idonly (id) VALUES ('1');`;

    const error = await processMutation(
      lc,
      undefined,
      db,
      SHARD,
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
                op: 'insert',
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
    );

    expect(error).undefined;

    await expectTables(db, {
      idonly: [{id: '1'}],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
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
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;

    await expect(
      processMutation(
        lc,
        undefined,
        db,
        SHARD,
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
                  op: 'insert',
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
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Push contains unexpected mutation id 3 for client 123. Expected mutation id 2.]`,
    );

    await expectTables(db, {
      idonly: [],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });

  test('mutation id too far in the future, while custom mutators are enabled, retries twice', async () => {
    await db`
      INSERT INTO ${db(
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
        VALUES ('abc', '123', 1)`;
    const testLogSink = new TestLogSink();
    const lc = new LogContext('debug', undefined, testLogSink);

    await expect(
      processMutation(
        lc,
        undefined,
        db,
        SHARD,
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
                  op: 'insert',
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

        undefined,
        true,
      ),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Push contains unexpected mutation id 3 for client 123. Expected mutation id 2.]`,
    );

    // check that we hit our retry logic for unexpected mutation id when CRUD and Custom are enabled.
    expect(
      testLogSink.messages
        .map(m => m[2][0])
        .filter(m => typeof m === 'string')
        .filter(m => m.includes('Both CRUD and Custom mutators')),
    ).toMatchInlineSnapshot(`
      [
        "Both CRUD and Custom mutators are being used at once. This is supported for now but IS NOT RECOMMENDED. Migrate completely to custom mutators.",
        "Both CRUD and Custom mutators are being used at once. This is supported for now but IS NOT RECOMMENDED. Migrate completely to custom mutators.",
      ]
    `);
  });

  test('process create, set, update, delete all at once', async () => {
    const error = await processMutation(
      lc,
      {},
      db,
      SHARD,
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
                op: 'insert',
                tableName: 'my_schema.id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col1: 'create',
                  col2: 'create',
                },
              },
              {
                op: 'upsert',
                tableName: 'my_schema.id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '2',
                  col1: 'set',
                  col2: 'set',
                },
              },
              {
                op: 'update',
                tableName: 'my_schema.id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col1: 'update',
                },
              },
              {
                op: 'update',
                tableName: 'my_schema.id_and_cols',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  col2: 'set',
                },
              },
              {
                op: 'delete',
                tableName: 'my_schema.id_and_cols',
                primaryKey: ['id'],
                value: {id: '2'},
              },
            ],
          },
        ],
        timestamp: Date.now(),
      } satisfies CRUDMutation,
      mockWriteAuthorizer,
    );

    expect(error).undefined;

    await expectTables(db, {
      ['my_schema.id_and_cols']: [
        {
          id: '1',
          col1: 'update',
          col2: 'set',
        },
      ],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
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
      lc,
      {},
      db,
      SHARD,
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
                op: 'insert',
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
    );

    expect(error).toEqual([
      ErrorKind.MutationFailed,
      'PostgresError: insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"',
    ]);

    await expectTables(db, {
      ['fk_ref']: [],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
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
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients ("clientGroupID", "clientID", "lastMutationID") 
         VALUES ('abc', '123', 2)`;

    // Start a concurrent mutation that bumps the lmid from 2 => 3.
    const done = db.begin(Mode.SERIALIZABLE, async tx => {
      // Simulate holding a lock on the row.
      void tx`SELECT * FROM ${db(
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients WHERE "clientGroupID" = 'abc' AND "clientID" = '123'`;

      await promise;

      // Update the row on signal.
      return tx`
      UPDATE ${db(
        `${APP_ID}_${SHARD_NUM}`,
      )}.clients SET "lastMutationID" = 3 WHERE "clientGroupID" = 'abc'`;
    });

    const error = await processMutation(
      lc,
      {},
      db,
      SHARD,
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
                op: 'insert',
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

      async () => {
        // Finish the 2 => 3 transaction only after this 3 => 4 transaction begins.
        resolve();
        await done;
      },
    );

    expect(error).toBeUndefined();

    // 3 => 4 should succeed after internally retrying.
    await expectTables(db, {
      idonly: [{id: '1'}],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 4n,
          userID: null,
        },
      ],
    });
  });

  test('data type handling', async () => {
    await expectTables(db, {
      types: [],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [],
    });

    const error = await processMutation(
      lc,
      undefined,
      db,
      SHARD,
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
                op: 'insert',
                tableName: 'types',
                primaryKey: ['id'],
                value: {
                  id: '1',
                  num: 23.45,
                  time1: 1742246216309,
                  time2: '2025-03-17T21:18:42.792Z',
                },
              },
            ],
          },
        ],
        timestamp: Date.now(),
      },
      mockWriteAuthorizer,
    );

    expect(error).undefined;

    await expectTables(db, {
      types: [
        {
          id: '1',
          num: 23.45,
          time1: 1742246216309,
          time2: 1742246322792,
        },
      ],
      [`${APP_ID}_${SHARD_NUM}.clients`]: [
        {
          clientGroupID: 'abc',
          clientID: '123',
          lastMutationID: 1n,
          userID: null,
        },
      ],
    });
  });
});
