import type {LogContext} from '@rocicorp/logger';
import type {PendingQuery, Row} from 'postgres';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {createRowsVersionTable, cvrSchema, setupCVRTables} from './cvr.ts';

export async function initViewSyncerSchema(
  log: LogContext,
  db: PostgresDB,
  shardID: string,
): Promise<void> {
  const schema = cvrSchema(shardID);

  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupCVRTables(lc, tx, shardID),
    minSafeVersion: 1,
  };

  const migrateV1toV2: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "replicaVersion" TEXT`;
    },
  };

  const migrateV2ToV3: Migration = {
    migrateSchema: async (_, tx) => {
      await tx.unsafe(createRowsVersionTable(shardID));
    },

    /** Populates the cvr.rowsVersion table with versions from cvr.instances. */
    migrateData: async (lc, tx) => {
      const pending: PendingQuery<Row[]>[] = [];
      for await (const versions of tx<
        {clientGroupID: string; version: string}[]
      >`
      SELECT "clientGroupID", "version" FROM ${tx(schema)}.instances`.cursor(
        5000,
      )) {
        for (const version of versions) {
          pending.push(
            tx`INSERT INTO ${tx(schema)}."rowsVersion" ${tx(version)} 
               ON CONFLICT ("clientGroupID")
               DO UPDATE SET ${tx(version)}`.execute(),
          );
        }
      }
      lc.info?.(`initializing rowsVersion for ${pending.length} cvrs`);
      await Promise.all(pending);
    },
  };

  const migrateV3ToV4: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "owner" TEXT`;
      await tx`ALTER TABLE ${tx(schema)}.instances ADD "grantedAt" TIMESTAMPTZ`;
    },
  };

  const migrateV5ToV6: Migration = {
    migrateSchema: async (_, tx) => {
      await tx`
      ALTER TABLE ${tx(schema)}."rows"
        DROP CONSTRAINT fk_rows_client_group`;
      await tx`
      ALTER TABLE ${tx(schema)}."rowsVersion"
        DROP CONSTRAINT fk_rows_version_client_group`;
    },
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: migrateV1toV2,
    3: migrateV2ToV3,
    4: migrateV3ToV4,
    // v5 enables asynchronous row-record flushing, and thus relies on
    // the logic that updates and checks the rowsVersion table in v3.
    5: {minSafeVersion: 3},
    6: migrateV5ToV6,
  };

  await runSchemaMigrations(
    log,
    'view-syncer',
    cvrSchema(shardID),
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
