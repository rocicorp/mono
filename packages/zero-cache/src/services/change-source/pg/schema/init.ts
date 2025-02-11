import type {LogContext} from '@rocicorp/logger';
import {
  getVersionHistory,
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {AutoResetSignal} from '../../../change-streamer/schema/tables.ts';
import type {ShardConfig} from '../shard-config.ts';
import {
  dropShard,
  ensureGlobalTables,
  setupTablesAndReplication,
  unescapedSchema,
} from './shard.ts';

/**
 * Initializes a shard for initial sync.
 * This will drop any existing shard setup.
 */
export async function initShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shardConfig: ShardConfig,
): Promise<void> {
  await db.unsafe(dropShard(shardConfig.id));
  return runShardMigrations(lc, db, shardConfig);
}

/**
 * Updates the schema for an existing shard.
 */
export async function updateShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shardConfig: ShardConfig,
): Promise<void> {
  const {id} = shardConfig;
  const {schemaVersion} = await getVersionHistory(db, unescapedSchema(id));
  if (schemaVersion === 0) {
    throw new AutoResetSignal(`upstream shard ${id} is not initialized`);
  }
  return runShardMigrations(lc, db, shardConfig);
}

async function runShardMigrations(
  lc: LogContext,
  db: PostgresDB,
  shardConfig: ShardConfig,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shardConfig),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    3: {
      migrateSchema: () => {
        throw new AutoResetSignal('resetting to upgrade shard schema');
      },
      minSafeVersion: 3,
    },
    // The zero.permissions table was added to the global zero shard.
    4: {migrateSchema: (_, tx) => ensureGlobalTables(tx)},
  };

  await runSchemaMigrations(
    lc,
    `upstream-shard-${shardConfig.id}`,
    unescapedSchema(shardConfig.id),
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}
