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
  let sql: PostgresDB;

  const APP_ID = 'rezo';
  const SHARD_NUM = 8;
  const shard = {appID: APP_ID, shardNum: SHARD_NUM};

  beforeEach<PgTest>(async ({testDBs}) => {
    sql = await testDBs.create('change_streamer_schema_tables');
    await sql.begin(tx => setupCDCTables(lc, tx, shard));

    return () => testDBs.drop(sql);
  });

  test('ensureReplicationConfig', async () => {
    const replica1 = new Database(lc, ':memory:');
    initReplicationState(replica1, ['zero_data', 'zero_metadata'], '123');

    await ensureReplicationConfig(
      lc,
      sql,
      {
        replicaVersion: '183',
        publications: ['zero_data', 'zero_metadata'],
        watermark: '183',
      },
      shard,
      true,
    );

    await expectTables(sql, {
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

    await sql`
    INSERT INTO "rezo_8/cdc"."changeLog" (watermark, pos, change)
        VALUES ('184', 1, JSONB('{"foo":"bar"}'));
    UPDATE "rezo_8/cdc"."replicationState" 
        SET "lastWatermark" = '184', owner = 'my-task';
    INSERT INTO "rezo_8/cdc"."tableMetadata" (schema, "table", metadata)
        VALUES ('public', 'foo', '{"foo":"bar"}');
    INSERT INTO "rezo_8/cdc"."backfilling" (schema, "table", "column", backfill)
        VALUES ('public', 'foo', 'boo', '{"id":123}');
    `.simple();

    // Should be a no-op.
    await ensureReplicationConfig(
      lc,
      sql,
      {
        replicaVersion: '183',
        publications: ['zero_metadata', 'zero_data'],
        watermark: '183',
      },
      shard,
      true,
    );

    await expectTables(sql, {
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
      ['rezo_8/cdc.tableMetadata']: [
        {
          schema: 'public',
          table: 'foo',
          metadata: {foo: 'bar'},
        },
      ],
      ['rezo_8/cdc.backfilling']: [
        {
          schema: 'public',
          table: 'foo',
          column: 'boo',
          backfill: {id: 123},
        },
      ],
    });

    await markResetRequired(sql, shard);
    await expectTables(sql, {
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
      ['rezo_8/cdc.tableMetadata']: [
        {
          schema: 'public',
          table: 'foo',
          metadata: {foo: 'bar'},
        },
      ],
      ['rezo_8/cdc.backfilling']: [
        {
          schema: 'public',
          table: 'foo',
          column: 'boo',
          backfill: {id: 123},
        },
      ],
    });

    // Should not affect auto-reset = false (i.e. no-op).
    await ensureReplicationConfig(
      lc,
      sql,
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
        sql,
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
      sql,
      {
        replicaVersion: '1g8',
        publications: ['zero_data', 'zero_metadata'],
        watermark: '1g8',
      },
      shard,
      true,
    );

    await expectTables(sql, {
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
      ['rezo_8/cdc.tableMetadata']: [],
      ['rezo_8/cdc.backfilling']: [],
    });

    // Different replica version at a non-initial watermark
    // should trigger a reset.
    await expect(
      ensureReplicationConfig(
        lc,
        sql,
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
