import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {getConnectionURI, initDB, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {decommissionShard} from './decommission.ts';
import {initialSync} from './initial-sync.ts';

const APP_ID = 'zeroout';
const SHARD_NUM = 13;

describe('decommission', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let replica: Database;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('decommission_test');
    replica = new Database(lc, ':memory:');

    return () => testDBs.drop(upstream);
  });

  test('decommission shard', async () => {
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
        appID: APP_ID,
        shardNum: SHARD_NUM,
        publications: [],
      },
      replica,
      getConnectionURI(upstream),
      {tableCopyWorkers: 5},
    );

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
      await upstream`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values(),
    ).toEqual([['zeroout_13'], ['zeroout']]);

    await decommissionShard(lc, upstream, APP_ID, SHARD_NUM);

    expect(await upstream`SELECT pubname FROM pg_publication`.values()).toEqual(
      [],
    );
    expect(
      await upstream`SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'zeroout%'`.values(),
    ).toEqual([]);
    expect(
      await upstream`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'zeroout%'`.values(),
    ).toEqual([]);

    // Note: The app schema remains, since it is not shard specific.
    expect(
      await upstream`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'zeroout%'`.values(),
    ).toEqual([['zeroout']]);
  });
});
