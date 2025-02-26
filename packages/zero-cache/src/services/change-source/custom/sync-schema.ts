import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.ts';
import type {ShardConfig} from '../pg/shard-config.ts';
import {initialSync} from './change-source.ts';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  appID: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) => initialSync(log, appID, shard, tx, upstreamURI),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    1: setupMigration,
  };

  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
