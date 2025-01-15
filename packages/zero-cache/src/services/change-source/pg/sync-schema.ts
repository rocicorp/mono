import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.js';
import {AutoResetSignal} from '../../change-streamer/schema/tables.js';
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
    // There's no incremental migration from v1. Just reset the replica.
    2: {
      migrateSchema: () => {
        throw new AutoResetSignal('resetting replica at obsolete v1');
      },
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
