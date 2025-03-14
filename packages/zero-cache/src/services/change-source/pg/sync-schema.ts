import type {LogContext} from '@rocicorp/logger';

import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../../replicator/schema/replication-state.ts';
import {initialSync, type InitialSyncOptions} from './initial-sync.ts';

export const schemaVersionMigrationMap: IncrementalMigrationMap = {
  // There's no incremental migration from v1. Just reset the replica.
  4: {
    migrateSchema: () => {
      throw new AutoResetSignal('upgrading replica to new schema');
    },
    minSafeVersion: 3,
  },

  5: {
    migrateSchema: (_, db) => {
      db.exec(CREATE_RUNTIME_EVENTS_TABLE);
    },
    migrateData: (_, db) => {
      recordEvent(db, 'upgrade');
    },
  },
};

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

  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

export async function upgradeSyncSchema(
  log: LogContext,
  debugName: string,
  dbPath: string,
) {
  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    // setupMigration should never be invoked
    {
      migrateSchema: () => {
        throw new Error(
          'This should only be called for already synced replicas',
        );
      },
    },
    schemaVersionMigrationMap,
  );
}
