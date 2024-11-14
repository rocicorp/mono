import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../../db/migration.js';
import type {PostgresDB, PostgresTransaction} from '../../../../types/pg.js';
import type {ShardConfig} from '../shard-config.js';
import {getPublicationInfo, type PublishedSchema} from './published.js';
import {
  dropShard,
  setupTablesAndReplication,
  unescapedSchema,
} from './shard.js';

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
  return updateShardSchema(lc, db, shardConfig);
}

/**
 * Updates the schema for an existing shard.
 */
export async function updateShardSchema(
  lc: LogContext,
  db: PostgresDB,
  shardConfig: ShardConfig,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shardConfig),
    minSafeVersion: 1,
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: {migrateSchema: (_, tx) => migrateV1toV2(tx, shardConfig.id)},
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

// v1 required superuser / event trigger installation.
// Therefore, to migrate from v1 to v2:
//
// - Add the "ddlDetection" and "initialSchema" columns to "shardConfig"
// - Set "ddlDetection" to true
// - Populate the "initialSchema" to the current published schema
//
// The last step technically may not match the "initial" schema, but
// currently it is only used for "ddlDetection = false" setups, so
// it does not matter.
async function migrateV1toV2(tx: PostgresTransaction, shardID: string) {
  const s = unescapedSchema(shardID);
  const [{publications}] = await tx<{publications: string[]}[]>`
    SELECT publications FROM ${tx(s)}."shardConfig"
  `;
  const {tables, indexes} = await getPublicationInfo(tx, publications);
  const publishedSchema: PublishedSchema = {tables, indexes};

  void tx`
  ALTER TABLE ${tx(s)}."shardConfig" ADD "ddlDetection" BOOL`.execute();
  void tx`
  ALTER TABLE ${tx(s)}."shardConfig" ADD "initialSchema" JSON`.execute();
  await tx`
    UPDATE ${tx(s)}."shardConfig"
      SET "ddlDetection"  = true, 
          "initialSchema" = ${publishedSchema}`;
}
