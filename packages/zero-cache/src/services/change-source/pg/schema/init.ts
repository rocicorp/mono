import type {LogContext} from '@rocicorp/logger';
import {
  getVersionHistory,
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {AutoResetSignal} from '../../../change-streamer/schema/tables.ts';
import {decommissionShard} from '../decommission.ts';
import {dropShard, setupTablesAndReplication} from './shard.ts';

/**
 * Initializes a shard for initial sync.
 * This will drop any existing shard setup.
 */
export async function initShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
): Promise<void> {
  await db.unsafe(dropShard(shard.appID, shard.shardNum));
  return runShardMigrations(lc, db, shard);
}

/**
 * Updates the schema for an existing shard.
 */
export async function updateShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
): Promise<void> {
  const {appID, shardNum} = shard;
  const versionHistory = await getVersionHistory(db, upstreamSchema(shard));
  if (versionHistory === null) {
    throw new AutoResetSignal(
      `upstream shard ${appID}_${shardNum} is not initialized`,
    );
  }
  await runShardMigrations(lc, db, shard);

  // The decommission check is run in updateShardSchema so that it happens
  // after initial sync, and not when the shard schema is initially set up.
  await decommissionLegacyShard(lc, db, shard);
}

async function runShardMigrations(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shard),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    4: {
      migrateSchema: () => {
        throw new AutoResetSignal('resetting to upgrade shard schema');
      },
      minSafeVersion: 3,
    },

    // v5 changes the upstream schema organization from "zero_{SHARD_ID}" to
    // the "{APP_ID}_0". An incremental migration indicates that the previous
    // SHARD_ID was "0" and the new APP_ID is "zero" (i.e. the default values
    // for those options). In this case, the upstream format is identical, and
    // no migration is necessary. However, the version is bumped to v5 to
    // indicate that it was created with the {APP_ID} configuration and should
    // not be decommissioned as a legacy shard.
    5: {},
  };

  await runSchemaMigrations(
    lc,
    `upstream-shard-${shard.appID}`,
    upstreamSchema(shard),
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

export async function decommissionLegacyShard(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
) {
  if (shard.appID !== 'zero') {
    // When migration from non-default shard ids, e.g. "zero_prod" => "prod_0",
    // clean up the old "zero_prod" shard if it is pre-v5. Note that the v5
    // check is important to guard against cleaning up a **new** "zero_0" app
    // that coexists with the current App (with app-id === "0").
    const versionHistory = await getVersionHistory(db, `zero_${shard.appID}`);
    if (versionHistory !== null && versionHistory.schemaVersion < 5) {
      await decommissionShard(lc, db, 'zero', shard.appID);
    }
  }
}
