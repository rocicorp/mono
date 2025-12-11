import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {runSchemaMigrations} from '../../../db/migration.ts';
import {type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {cvrSchema} from '../../../types/shards.ts';
import {initViewSyncerSchema} from './init.ts';
import {setupCVRTables as setupCVRTablesv15} from './previous/cvr.v15.ts';

describe('view-syncer/schema/init', () => {
  const lc = new LogContext('debug', {}, consoleLogSink);
  createSilentLogContext();
  let sql: PostgresDB;

  const SHARD = {appID: 'abc', shardNum: 0};

  beforeEach<PgTest>(async ({testDBs}) => {
    sql = await testDBs.create('cvr_schema_init_test_db');
    return () => testDBs.drop(sql);
  });

  test('15 to 16', async () => {
    // Setup v15
    await runSchemaMigrations(
      lc,
      'view-syncer',
      cvrSchema(SHARD),
      sql,
      {migrateSchema: (lc, tx) => setupCVRTablesv15(lc, tx, SHARD)},
      {
        15: {
          migrateSchema: () => {
            throw new Error('unreachable');
          },
        },
      },
    );

    await sql`INSERT INTO ${sql(cvrSchema(SHARD))}.instances ${sql({
      clientGroupID: 'foo-bar',
      version: '123',
      lastActive: new Date(Date.UTC(2025, 10, 10)),
      ttlClock: 0,
    })}`;

    // Now migrate to 16
    await initViewSyncerSchema(lc, sql, SHARD);

    expect(
      await sql`
        SELECT * FROM ${sql(cvrSchema(SHARD))}.instances
      `,
    ).toMatchObject([
      {
        clientGroupID: 'foo-bar',
        deleted: false,
        profileID: 'cgfoo-bar', // profileID is backfilled from clientGroupID.
      },
    ]);

    expect(
      await sql`
        SELECT indexdef FROM pg_indexes 
          WHERE schemaname = ${cvrSchema(SHARD)}
          AND tablename = 'instances'
          ORDER BY indexname`.values(),
    ).toMatchInlineSnapshot(`
      Result [
        [
          "CREATE INDEX instances_last_active ON "abc_0/cvr".instances USING btree ("lastActive") WHERE (NOT deleted)",
        ],
        [
          "CREATE UNIQUE INDEX instances_pkey ON "abc_0/cvr".instances USING btree ("clientGroupID")",
        ],
        [
          "CREATE INDEX profile_ids_last_active ON "abc_0/cvr".instances USING btree ("lastActive", "profileID") WHERE ("profileID" IS NOT NULL)",
        ],
        [
          "CREATE INDEX tombstones_last_active ON "abc_0/cvr".instances USING btree ("lastActive") WHERE deleted",
        ],
      ]
    `);
  });
});
