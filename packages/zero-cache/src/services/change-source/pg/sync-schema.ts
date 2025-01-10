import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.js';
import {initialSync, type InitialSyncOptions} from './initial-sync.js';
import type {ShardConfig} from './shard-config.js';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) =>
      initialSync(log, shard, tx, upstreamURI, syncOptions),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    1: setupMigration,
    // There are no incremental migrations yet, but if we were to, say introduce
    // another column, initialSync would be updated to create the table with
    // the new column, and then there would be an incremental migration here at
    // version `2` that adds the column for databases that were initialized to
    // version `1`.
  };

  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
