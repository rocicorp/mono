import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {expectTables, testDBs} from '../../../test/db.ts';
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

  const SHARD_ID = 'oiu';

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_schema_tables');
    await db.begin(tx => setupCDCTables(lc, tx, SHARD_ID));
  });

  afterEach(async () => {
    await testDBs.drop(db);
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
      },
      SHARD_ID,
      true,
    );

    await expectTables(db, {
      ['cdc_oiu.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['cdc_oiu.replicationState']: [
        {
          lastWatermark: '183',
          owner: null,
          lock: 1,
        },
      ],
      ['cdc_oiu.changeLog']: [],
    });

    await db`
    INSERT INTO cdc_oiu."changeLog" (watermark, pos, change)
        values ('184', 1, JSONB('{"foo":"bar"}'));
    UPDATE cdc_oiu."replicationState" 
        SET "lastWatermark" = '184', owner = 'my-task';
    `.simple();

    // Should be a no-op.
    await ensureReplicationConfig(
      lc,
      db,
      {
        replicaVersion: '183',
        publications: ['zero_metadata', 'zero_data'],
      },
      SHARD_ID,
      true,
    );

    await expectTables(db, {
      ['cdc_oiu.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['cdc_oiu.replicationState']: [
        {
          lastWatermark: '184',
          owner: 'my-task',
          lock: 1,
        },
      ],
      ['cdc_oiu.changeLog']: [
        {
          watermark: '184',
          pos: 1n,
          change: {foo: 'bar'},
          precommit: null,
        },
      ],
    });

    await markResetRequired(db, SHARD_ID);
    await expectTables(db, {
      ['cdc_oiu.replicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: true,
          lock: 1,
        },
      ],
      ['cdc_oiu.replicationState']: [
        {
          lastWatermark: '184',
          owner: 'my-task',
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
      },
      SHARD_ID,
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
        },
        SHARD_ID,
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
      },
      SHARD_ID,
      true,
    );

    await expectTables(db, {
      ['cdc_oiu.replicationConfig']: [
        {
          replicaVersion: '1g8',
          publications: ['zero_data', 'zero_metadata'],
          resetRequired: null,
          lock: 1,
        },
      ],
      ['cdc_oiu.replicationState']: [
        {
          lastWatermark: '1g8',
          owner: null,
          lock: 1,
        },
      ],
      ['cdc_oiu.changeLog']: [],
    });
  });
});
