import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {
  cdcSchema,
  createReplicationStateTable,
  setupCDCTables,
  type ReplicationState,
} from './tables.ts';

async function migrateFromLegacySchema(
  lc: LogContext,
  db: PostgresDB,
  newSchema: string,
) {
  const result = await db`SELECT * FROM pg_namespace WHERE nspname = 'cdc'`;
  if (result.length > 0) {
    lc.info?.(`Migrating cdc to ${newSchema}`);
    await db`ALTER SCHEMA cdc RENAME TO ${db(newSchema)}`;
  }
}
export async function initChangeStreamerSchema(
  log: LogContext,
  db: PostgresDB,
  shardID: string,
): Promise<void> {
  const schema = cdcSchema(shardID);
  await migrateFromLegacySchema(log, db, schema);

  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupCDCTables(lc, tx, shardID),
    minSafeVersion: 1,
  };

  async function migrateV1toV2(_: LogContext, db: PostgresTransaction) {
    await db`
    ALTER TABLE ${db(schema)}."replicationConfig" ADD "resetRequired" BOOL`;
  }

  const migrateV2ToV3 = {
    migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
      await db.unsafe(createReplicationStateTable(shardID));
    },

    migrateData: async (_: LogContext, db: PostgresTransaction) => {
      const lastWatermark = await getLastWatermarkV2(db, shardID);
      const replicationState: Partial<ReplicationState> = {lastWatermark};
      await db`
      TRUNCATE TABLE ${db(schema)}."replicationState"`;
      await db`
      INSERT INTO ${db(schema)}."replicationState" ${db(replicationState)}`;
    },
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: {migrateSchema: migrateV1toV2},
    3: migrateV2ToV3,
  };

  await runSchemaMigrations(
    log,
    'change-streamer',
    schema,
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

export async function getLastWatermarkV2(
  db: PostgresDB,
  shardID: string,
): Promise<string> {
  const schema = cdcSchema(shardID);
  const [{max}] = await db<{max: string | null}[]>`
    SELECT MAX(watermark) as max FROM ${db(schema)}."changeLog"`;
  if (max !== null) {
    return max;
  }
  // The changeLog is only empty if nothing has been synced since initial-sync.
  // In this case, the last watermark is the replicaVersion.
  const [{replicaVersion}] = await db<{replicaVersion: string}[]>`
    SELECT "replicaVersion" FROM ${db(schema)}."replicationConfig"
    `;
  return replicaVersion;
}
