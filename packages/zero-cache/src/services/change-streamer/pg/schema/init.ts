import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.js';
import type {PostgresDB} from '../../../../types/pg.js';
import type {ShardConfig} from '../shard-config.js';
import {setupTablesAndReplication, unescapedSchema} from './shard.js';

export async function initShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shardConfig: ShardConfig,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shardConfig),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    1: setupMigration,
    // There are no incremental migrations yet, but if we were to, say introduce
    // another column, setupTablesAndReplication would be updated the table with
    // the new column, and then there would be an incremental migration here at
    // version `2` that adds the column for databases that were initialized to
    // version `1`.
  };

  await runSchemaMigrations(
    lc,
    'upstream-shard',
    unescapedSchema(shardConfig.id),
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
