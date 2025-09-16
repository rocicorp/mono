/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {expectTables, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {
  AutoResetSignal,
  ensureReplicationConfig,
  markResetRequired,
  setupCDCTables,
} from './tables.ts';

describe('change-streamer/schema/tables', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  const APP_ID = 'rezo';
  const SHARD_NUM = 8;
  const shard = {appID: APP_ID, shardNum: SHARD_NUM};

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('change_streamer_schema_tables');
    await db.begin(tx => setupCDCTables(lc, tx, shard));

    return () => testDBs.drop(db);
  });

  test('ensureReplicationConfig', async () => {
    const replica1 = new Database(lc, ':memory:');
    initReplicationState(replica1, ['zero_data', 'zero_metadata'], '123');

    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: '183',
        publications: ['zero_data', 'zero_metadata'],
        watermark: '183',
      },
      shard,
      true,
    );

    await expectTables(db, {
      ['rezo_8/cdc.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.replicationState']: [
        {
          lastWatermark: '183',
          owner: null,
          ownerAddress: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.changeLog']: [],
    });

    await db`
    INSERT INTO "rezo_8/cdc"."changeLog" (watermark, pos, change)
        values ('184', 1, JSONB('{"foo":"bar"}'));
    UPDATE "rezo_8/cdc"."replicationState" 
        SET "lastWatermark" = '184', owner = 'my-task';
    `.simple();

    // Should be a no-op.
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: '183',
        publications: ['zero_metadata', 'zero_data'],
        watermark: '183',
      },
      shard,
      true,
    );

    await expectTables(db, {
      ['rezo_8/cdc.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.replicationState']: [
        {
          lastWatermark: '184',
          owner: 'my-task',
          ownerAddress: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.changeLog']: [
        {
          watermark: '184',
          pos: 1n,
          change: {foo: 'bar'},
          precommit: null,
        },
      ],
    });

    await markResetRequired(db, shard);
    await expectTables(db, {
      ['rezo_8/cdc.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: true,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.replicationState']: [
        {
          lastWatermark: '184',
          owner: 'my-task',
          ownerAddress: null,
          lock: 1,
        },
      ],
    });

    // Should not affect auto-reset = false (i.e. no-op).
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: '183',
        publications: ['zero_metadata', 'zero_data'],
        watermark: '183',
      },
      shard,
      false,
    );

    // autoReset with the same version should throw.
    await expect(
      ensureReplicationConfig(
        lc,
        db,
        {
          replicaVersion: '183',
          publications: ['zero_metadata', 'zero_data'],
          watermark: '183',
        },
        shard,
        true,
      ),
    ).rejects.toThrow(AutoResetSignal);

    // Different replica version should wipe the tables.
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: '1g8',
        publications: ['zero_data', 'zero_metadata'],
        watermark: '1g8',
      },
      shard,
      true,
    );

    await expectTables(db, {
      ['rezo_8/cdc.replicationConfig']: [
        {
          replicaVersion: '1g8',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.replicationState']: [
        {
          lastWatermark: '1g8',
          owner: null,
          ownerAddress: null,
          lock: 1,
        },
      ],
      ['rezo_8/cdc.changeLog']: [],
    });

    // Different replica version at a non-initial watermark
    // should trigger a reset.
    await expect(
      ensureReplicationConfig(
        lc,
        db,
        {
          replicaVersion: '1gg',
          publications: ['zero_data', 'zero_metadata'],
          watermark: '1zz',
        },
        shard,
        true,
      ),
    ).rejects.toThrow(AutoResetSignal);
  });
});
