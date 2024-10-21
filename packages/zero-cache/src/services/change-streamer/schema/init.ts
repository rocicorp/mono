import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  runSchemaMigrations,
  type VersionMigrationMap,
} from '../../../db/old-migration.js';
import {PG_SCHEMA, setupCDCTables} from './tables.js';

export async function initChangeStreamerSchema(
  log: LogContext,
  db: postgres.Sql,
): Promise<void> {
  const schemaVersionMigrationMap: VersionMigrationMap = {
    1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
    2: {run: setupCDCTables},
  };

  await runSchemaMigrations(
    log,
    'change-streamer',
    PG_SCHEMA,
    db,
    schemaVersionMigrationMap,
  );
}
