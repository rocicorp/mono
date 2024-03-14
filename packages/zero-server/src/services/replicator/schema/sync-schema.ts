import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  runSyncSchemaMigrations,
  type VersionMigrationMap,
} from './migration.js';

const SCHEMA_VERSION_MIGRATION_MAP: VersionMigrationMap = {
  1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
};

export async function initSyncSchema(
  log: LogContext,
  db: postgres.Sql,
  upstreamUri: string,
): Promise<void> {
  await runSyncSchemaMigrations(
    log,
    db,
    upstreamUri,
    SCHEMA_VERSION_MIGRATION_MAP,
  );
}
