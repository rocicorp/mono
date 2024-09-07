import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  runSchemaMigrations,
  type VersionMigrationMap,
} from '../../../db/migration.js';
import {PG_SCHEMA, setupCVRTables} from './cvr.js';

export async function initViewSyncerSchema(
  log: LogContext,
  db: postgres.Sql,
): Promise<void> {
  const schemaVersionMigrationMap: VersionMigrationMap = {
    1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
    2: {
      run: setupCVRTables,
    },
  };

  await runSchemaMigrations(
    log,
    'view-syncer',
    PG_SCHEMA,
    db,
    schemaVersionMigrationMap,
  );
}
