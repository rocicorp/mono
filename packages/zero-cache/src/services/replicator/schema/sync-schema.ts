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

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  schemaName: string,
  replicaID: string,
  replica: postgres.Sql,
  upstream: postgres.Sql,
  upstreamURI: string,
): Promise<void> {
  const schemaVersionMigrationMap: VersionMigrationMap = {
    1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
    2: {
      run: (log, replicaID, tx) =>
        startPostgresReplication(log, replicaID, tx, upstream, upstreamURI),
    },
    3: {
      pre: (log, _replicaID, db) =>
        waitForInitialDataSynchronization(log, db, upstreamURI),
      run: (log, _replicaID, tx) =>
        handoffPostgresReplication(log, tx, upstreamURI),
    },
    4: {
      run: (log, _replicaID, tx) =>
        setupReplicationTables(log, tx, upstreamURI),
    },
  };

  await runSyncSchemaMigrations(
    log,
    debugName,
    schemaName,
    replicaID,
    replica,
    // upstream,
    // upstreamUri,
    schemaVersionMigrationMap,
  );
}
