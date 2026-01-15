import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {runSchemaMigrations} from '../../../db/migration.ts';
import {type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {cvrSchema} from '../../../types/shards.ts';
import {initViewSyncerSchema} from './init.ts';
import {setupCVRTables as setupCVRTablesV7} from './previous/cvr.v7.ts';

describe('view-syncer schema migration', () => {
  const lc = createSilentLogContext();
  let oldDB: PostgresDB;
  let newDB: PostgresDB;

  const SHARD = {appID: 'abc', shardNum: 0};

  beforeEach<PgTest>(async ({testDBs}) => {
    oldDB = await testDBs.create('cvr_schema_migration_test_old_db');
    newDB = await testDBs.create('cvr_schema_migration_test_new_db');
    return () => testDBs.drop(oldDB, newDB);
  });

  test('incremental vs full', async () => {
    // Initialize the oldDB at schema v7
    await runSchemaMigrations(
      lc,
      'view-syncer',
      cvrSchema(SHARD),
      oldDB,
      {migrateSchema: (lc, tx) => setupCVRTablesV7(lc, tx, SHARD)},
      {7: {}},
    );

    // Add an old row
    await oldDB`INSERT INTO ${oldDB(cvrSchema(SHARD))}.instances ${oldDB({
      clientGroupID: 'foo-bar',
      version: '123',
      lastActive: new Date(Date.UTC(2025, 10, 10)),
    })}`;

    // Now run incremental migrations to the present version.
    await initViewSyncerSchema(lc, oldDB, SHARD);

    expect(
      await oldDB`
        SELECT * FROM ${oldDB(cvrSchema(SHARD))}.instances
      `,
    ).toMatchObject([
      {
        clientGroupID: 'foo-bar',
        deleted: false,
        profileID: 'cgfoo-bar', // profileID is backfilled from clientGroupID.
      },
    ]);

    // Initialize a "newDB" directly to the present version.
    await initViewSyncerSchema(lc, newDB, SHARD);

    const [oldIndexes, newIndexes] = await Promise.all(
      [oldDB, newDB].map(sql =>
        sql`
        SELECT tablename, indexdef FROM pg_indexes 
          WHERE schemaname = ${cvrSchema(SHARD)}
          ORDER BY tablename, indexname`.values(),
      ),
    );

    expect(oldIndexes).toEqual(newIndexes);

    // For visual inspection
    expect(newIndexes.flat()).toMatchInlineSnapshot(`
      [
        "clients",
        "CREATE UNIQUE INDEX clients_pkey ON "abc_0/cvr".clients USING btree ("clientGroupID", "clientID")",
        "desires",
        "CREATE INDEX desires_inactivated_at ON "abc_0/cvr".desires USING btree ("inactivatedAt")",
        "desires",
        "CREATE INDEX desires_patch_version ON "abc_0/cvr".desires USING btree ("patchVersion")",
        "desires",
        "CREATE UNIQUE INDEX desires_pkey ON "abc_0/cvr".desires USING btree ("clientGroupID", "clientID", "queryHash")",
        "instances",
        "CREATE INDEX instances_last_active ON "abc_0/cvr".instances USING btree ("lastActive") WHERE (NOT deleted)",
        "instances",
        "CREATE UNIQUE INDEX instances_pkey ON "abc_0/cvr".instances USING btree ("clientGroupID")",
        "instances",
        "CREATE INDEX profile_ids_last_active ON "abc_0/cvr".instances USING btree ("lastActive", "profileID") WHERE ("profileID" IS NOT NULL)",
        "instances",
        "CREATE INDEX tombstones_last_active ON "abc_0/cvr".instances USING btree ("lastActive") WHERE deleted",
        "queries",
        "CREATE INDEX queries_patch_version ON "abc_0/cvr".queries USING btree ("patchVersion" NULLS FIRST)",
        "queries",
        "CREATE UNIQUE INDEX queries_pkey ON "abc_0/cvr".queries USING btree ("clientGroupID", "queryHash")",
        "rows",
        "CREATE INDEX row_patch_version ON "abc_0/cvr".rows USING btree ("patchVersion")",
        "rows",
        "CREATE INDEX row_ref_counts ON "abc_0/cvr".rows USING gin ("refCounts")",
        "rows",
        "CREATE UNIQUE INDEX rows_pkey ON "abc_0/cvr".rows USING btree ("clientGroupID", schema, "table", "rowKey")",
        "rowsVersion",
        "CREATE UNIQUE INDEX "rowsVersion_pkey" ON "abc_0/cvr"."rowsVersion" USING btree ("clientGroupID")",
        "versionHistory",
        "CREATE UNIQUE INDEX pk_schema_meta_lock ON "abc_0/cvr"."versionHistory" USING btree (lock)",
      ]
    `);

    const [oldTables, newTables] = await Promise.all(
      [oldDB, newDB].map(
        sql =>
          sql`
        SELECT table_name, column_name, data_type, column_default, is_nullable 
          FROM INFORMATION_SCHEMA.COLUMNS 
          WHERE table_schema = ${cvrSchema(SHARD)}
          ORDER BY table_name, column_name`,
      ),
    );
    expect(oldTables).toEqual(newTables);
  });
});
