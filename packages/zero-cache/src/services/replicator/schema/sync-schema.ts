import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  VersionMigrationMap,
} from 'zero-cache/src/db/migration-lite.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {initialSync} from '../initial-sync.js';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  replicaID: string,
  dbPath: string,
  upstream: PostgresDB,
  upstreamURI: string,
): Promise<void> {
  const schemaVersionMigrationMap: VersionMigrationMap = {
    1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
    2: {
      run: (log, tx) => initialSync(log, replicaID, tx, upstream, upstreamURI),
    },
  };

  await runSchemaMigrations(log, debugName, dbPath, schemaVersionMigrationMap);
}
