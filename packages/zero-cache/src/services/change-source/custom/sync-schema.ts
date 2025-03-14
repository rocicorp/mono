import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type Migration,
} from '../../../db/migration-lite.ts';
import type {ShardConfig} from '../../../types/shards.ts';
// TODO: Move this to a common location rather than depending on pg
import {schemaVersionMigrationMap} from '../pg/sync-schema.ts';
import {initialSync} from './change-source.ts';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) => initialSync(log, shard, tx, upstreamURI),
    minSafeVersion: 1,
  };

  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
