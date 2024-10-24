import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.js';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.js';
import {PG_SCHEMA, setupCVRTables} from './cvr.js';

const setupMigration: Migration = {
  migrateSchema: setupCVRTables,
  minSafeVersion: 1,
};

export async function initViewSyncerSchema(
  log: LogContext,
  db: PostgresDB,
): Promise<void> {
  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: {migrateSchema: migrateV1toV2},
  };

  await runSchemaMigrations(
    log,
    'view-syncer',
    PG_SCHEMA,
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

async function migrateV1toV2(_: LogContext, tx: PostgresTransaction) {
  await tx`ALTER TABLE cvr.instances ADD "replicaVersion" TEXT`;
}
