import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {initialSync, type InitialSyncOptions} from './initial-sync.ts';
import type {ShardConfig} from './shard-config.ts';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  appID: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) =>
      initialSync(log, appID, shard, tx, upstreamURI, syncOptions),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    // There's no incremental migration from v1. Just reset the replica.
    4: {
      migrateSchema: () => {
        throw new AutoResetSignal('upgrading replica to new schema');
      },
      minSafeVersion: 3,
    },
  };

  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
