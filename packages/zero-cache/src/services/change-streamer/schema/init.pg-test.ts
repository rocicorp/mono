import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {testDBs} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {getLastWatermarkV2} from './init.ts';
import {ensureReplicationConfig, setupCDCTables} from './tables.ts';

describe('change-streamer/schema/migration', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('change_streamer_schema_migration');
    await db.begin(tx => setupCDCTables(lc, tx));
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  test('getLastWatermarkV2', async () => {
    await ensureReplicationConfig(
      lc,
      db,
      {replicaVersion: '123', publications: []},
      true,
    );

    expect(await getLastWatermarkV2(db)).toEqual('123');

    await db`
    INSERT INTO cdc."changeLog" (watermark, pos, change)
       VALUES ('136', 2, '{"tag":"commit"}'::json);
    INSERT INTO cdc."changeLog" (watermark, pos, change)
       VALUES ('145', 0, '{"tag":"begin"}'::json);
    INSERT INTO cdc."changeLog" (watermark, pos, change)
       VALUES ('145', 1, '{"tag":"commit"}'::json);`.simple();

    expect(await getLastWatermarkV2(db)).toEqual('145');
  });
});
