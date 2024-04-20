import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  handoffPostgresReplication,
  startPostgresReplication,
  waitForInitialDataSynchronization,
} from '../initial-sync.js';
import {setupReplicationTables} from '../tables/replication.js';
import {
  runSyncSchemaMigrations,
  type VersionMigrationMap,
} from './migration.js';

const SCHEMA_VERSION_MIGRATION_MAP: VersionMigrationMap = {
  1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
  2: {run: startPostgresReplication},
  3: {
    pre: waitForInitialDataSynchronization,
    run: handoffPostgresReplication,
  },
  4: {run: setupReplicationTables},
};

export async function initSyncSchema(
  log: LogContext,
  replicaID: string,
  db: postgres.Sql,
  upstreamUri: string,
): Promise<void> {
  await runSyncSchemaMigrations(
    log,
    replicaID,
    db,
    upstreamUri,
    SCHEMA_VERSION_MIGRATION_MAP,
  );
}
