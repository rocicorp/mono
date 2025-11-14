import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, expectTypeOf} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {initialSync} from '../services/change-source/pg/initial-sync.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {initViewSyncerSchema} from '../services/view-syncer/schema/init.ts';
import {getConnectionURI, initDB, test, type PgTest} from '../test/db.ts';
import type {PostgresDB} from '../types/pg.ts';
import type {ShardID} from '../types/shards.ts';
import {decommissionZero, type DecommissionConfig} from './decommission.ts';

const APP_ID = 'zeroout';
const SHARD_NUM = 13;
const SHARD_ID: ShardID = {appID: APP_ID, shardNum: SHARD_NUM} as const;

describe('decommission', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let cvr: PostgresDB;
  let cdc: PostgresDB;
  let replica: Database;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('decommission_test_upstream');
    cvr = await testDBs.create('decommission_test_cvr');
    cdc = await testDBs.create('decommission_test_cdc');
    replica = new Database(lc, ':memory:');

    return () => testDBs.drop(upstream, cvr, cdc);
  });

  test('decommission config is a subset of zero config', () => {
    // This ensures that the environment variables used for a zero-cache
    // will apply to zero-out.
    expectTypeOf<ZeroConfig>().toExtend<DecommissionConfig>();
  });

  async function runTest(
    upstream: PostgresDB,
    cvr: PostgresDB,
    cdc: PostgresDB,
    config: DecommissionConfig,
  ) {
    await initDB(
      upstream,
      `
      CREATE TABLE foo (id TEXT PRIMARY KEY);
      INSERT INTO foo (id) VALUES ('bar');
    `,
    );
    await initialSync(
      lc,
      {
        ...SHARD_ID,
        publications: [],
      },
      replica,
      getConnectionURI(upstream),
      {tableCopyWorkers: 5},
    );
    await initChangeStreamerSchema(lc, cdc, SHARD_ID);
    await initViewSyncerSchema(lc, cvr, SHARD_ID);

    expect(await upstream`SELECT pubname FROM pg_publication`.values()).toEqual(
      [['_zeroout_public_13'], ['_zeroout_metadata_13']],
    );
    expect(
      await upstream`SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'zeroout%'`.values(),
    ).toEqual([
      ['zeroout_ddl_start_13'],
      ['zeroout_create_table_13'],
      ['zeroout_alter_table_13'],
      ['zeroout_create_index_13'],
      ['zeroout_drop_table_13'],
      ['zeroout_drop_index_13'],
      ['zeroout_alter_publication_13'],
      ['zeroout_alter_schema_13'],
    ]);
    expect(
      await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'zeroout%'`.values(),
    ).toMatchObject([[expect.stringMatching('zeroout_13_')]]);
    expect(
      (
        await upstream`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values()
      ).flat(),
    ).toEqual(expect.arrayContaining(['zeroout_13', 'zeroout']));
    expect(
      (
        await cvr`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values()
      ).flat(),
    ).toEqual(expect.arrayContaining(['zeroout_13/cvr']));
    expect(
      (
        await cdc`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values()
      ).flat(),
    ).toEqual(expect.arrayContaining(['zeroout_13/cdc']));

    await decommissionZero(lc, config);

    expect(await upstream`SELECT pubname FROM pg_publication`.values()).toEqual(
      [],
    );
    expect(
      await upstream`SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'zeroout%'`.values(),
    ).toEqual([]);
    expect(
      await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'zeroout%'`.values(),
    ).toEqual([]);

    expect(
      await upstream`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values(),
    ).toEqual([]);
    expect(
      await cvr`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values(),
    ).toEqual([]);
    expect(
      await cdc`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values(),
    ).toEqual([]);
  }

  test('decommission zero, everything in upstream', () =>
    runTest(upstream, upstream, upstream, {
      app: {id: APP_ID},
      shard: {num: SHARD_NUM},
      upstream: {db: getConnectionURI(upstream), type: 'pg'},
      cvr: {},
      change: {},
      log: {level: 'debug', format: 'text'},
    }));

  test('decommission zero, all separate dbs', () =>
    runTest(upstream, cvr, cdc, {
      app: {id: APP_ID},
      shard: {num: SHARD_NUM},
      upstream: {db: getConnectionURI(upstream), type: 'pg'},
      cvr: {db: getConnectionURI(cvr)},
      change: {db: getConnectionURI(cdc)},
      log: {level: 'debug', format: 'text'},
    }));
});
