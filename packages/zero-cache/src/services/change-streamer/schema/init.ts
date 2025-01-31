import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {
  CREATE_REPLICATION_STATE_TABLE,
  PG_SCHEMA,
  setupCDCTables,
  type ReplicationState,
} from './tables.ts';

const setupMigration: Migration = {
  migrateSchema: setupCDCTables,
  minSafeVersion: 1,
};

async function migrateV1toV2(_: LogContext, db: PostgresTransaction) {
  await db`ALTER TABLE cdc."replicationConfig" ADD "resetRequired" BOOL`;
}

const migrateV2ToV3 = {
  migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
    await db.unsafe(CREATE_REPLICATION_STATE_TABLE);
  },

  migrateData: async (_: LogContext, db: PostgresTransaction) => {
    const lastWatermark = await getLastWatermarkV2(db);
    const replicationState: Partial<ReplicationState> = {lastWatermark};
    await db`TRUNCATE TABLE cdc."replicationState"`;
    await db`INSERT INTO cdc."replicationState" ${db(replicationState)}`;
  },
};

const schemaVersionMigrationMap: IncrementalMigrationMap = {
  2: {migrateSchema: migrateV1toV2},
  3: migrateV2ToV3,
};

export async function initChangeStreamerSchema(
  log: LogContext,
  db: PostgresDB,
): Promise<void> {
  await runSchemaMigrations(
    log,
    'change-streamer',
    PG_SCHEMA,
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

export async function getLastWatermarkV2(db: PostgresDB): Promise<string> {
  const [{max}] = await db<{max: string | null}[]>`
    SELECT MAX(watermark) as max FROM cdc."changeLog"`;
  if (max !== null) {
    return max;
  }
  // The changeLog is only empty if nothing has been synced since initial-sync.
  // In this case, the last watermark is the replicaVersion.
  const [{replicaVersion}] = await db<{replicaVersion: string}[]>`
    SELECT "replicaVersion" FROM cdc."replicationConfig"
    `;
  return replicaVersion;
}
