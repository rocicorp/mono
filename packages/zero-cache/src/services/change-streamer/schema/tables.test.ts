import Database from 'better-sqlite3';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, test} from 'vitest';
import {expectTables, testDBs} from 'zero-cache/src/test/db.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {initReplicationState} from '../../replicator/schema/replication-state.js';
import {ensureReplicationConfig, setupCDCTables} from './tables.js';

describe('change-streamer/schema/tables', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_schema_tables');
    await db.begin(tx => setupCDCTables(lc, tx));
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('ensureReplicationConfig', async () => {
    const replica1 = new Database(':memory:');
    initReplicationState(replica1, ['zero_data', 'zero_metadata'], '0/123');

    await ensureReplicationConfig(lc, db, {
      replicaVersion: '183',
      publications: ['zero_data', 'zero_metadata'],
    });

    await expectTables(db, {
      ['cdc.ReplicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          lock: 1,
        },
      ],
      ['cdc.ChangeLog']: [],
    });

    await db`
    INSERT INTO cdc."ChangeLog" (watermark, pos, change)
        values ('184', 0, JSONB('{"foo":"bar"}'));
    `;

    // Should be a no-op.
    await ensureReplicationConfig(lc, db, {
      replicaVersion: '183',
      publications: ['zero_metadata', 'zero_data'],
    });

    await expectTables(db, {
      ['cdc.ReplicationConfig']: [
        {
          replicaVersion: '183',
          publications: ['zero_data', 'zero_metadata'],
          lock: 1,
        },
      ],
      ['cdc.ChangeLog']: [
        {
          watermark: '184',
          pos: 0n,
          change: {foo: 'bar'},
        },
      ],
    });

    // Different replica version should wipe the tables.
    await ensureReplicationConfig(lc, db, {
      replicaVersion: '1g8',
      publications: ['zero_data', 'zero_metadata'],
    });

    await expectTables(db, {
      ['cdc.ReplicationConfig']: [
        {
          replicaVersion: '1g8',
          publications: ['zero_data', 'zero_metadata'],
          lock: 1,
        },
      ],
      ['cdc.ChangeLog']: [],
    });
  });
});