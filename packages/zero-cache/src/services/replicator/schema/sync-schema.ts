import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  runSyncSchemaMigrations,
  type VersionMigrationMap,
} from '../../../db/migration.js';
import {
  handoffPostgresReplication,
  startPostgresReplication,
  waitForInitialDataSynchronization,
} from '../initial-sync.js';
import {setupReplicationTables} from './replication.js';

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
  debugName: string,
  schemaName: string,
  replicaID: string,
  replica: postgres.Sql,
  upstream: postgres.Sql,
  upstreamUri: string,
): Promise<void> {
  await runSyncSchemaMigrations(
    log,
    debugName,
    schemaName,
    replicaID,
    replica,
    upstream,
    upstreamUri,
    SCHEMA_VERSION_MIGRATION_MAP,
  );
}
