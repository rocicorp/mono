import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  runSchemaMigrations,
  type VersionMigrationMap,
} from '../../../db/migration.js';
import {setupCVRTables} from './cvr.js';

export async function initViewSyncerSchema(
  log: LogContext,
  debugName: string,
  schemaName: string,
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
    debugName,
    schemaName,
    db,
    schemaVersionMigrationMap,
  );
}
