import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../../types/shards.ts';
import {
  createBackfillTables,
  createReplicationStateTable,
  setupCDCTables,
  type ReplicationState,
} from './tables.ts';

async function migrateFromLegacySchemas(
  lc: LogContext,
  db: PostgresDB,
  newSchema: string,
  ...legacy: string[]
) {
  const rows = await db<{nspname: string}[]>`
    SELECT nspname FROM pg_namespace 
      WHERE nspname IN ${db([newSchema, ...legacy])}`.values();
  // oxlint-disable-next-line unicorn/prefer-set-has -- Small collection, array is more appropriate
  const names = rows.flat();
  if (names.includes(newSchema)) {
    return; // already migrated
  }
  for (const schema of legacy) {
    if (names.includes(schema)) {
      lc.info?.(`Migrating ${schema} to ${newSchema}`);
      await db`ALTER SCHEMA ${db(schema)} RENAME TO ${db(newSchema)}`;
      break;
    }
  }
}

export async function initChangeStreamerSchema(
  log: LogContext,
  db: PostgresDB,
  shard: ShardID,
): Promise<void> {
  const schema = cdcSchema(shard);
  const {appID} = shard;
  await migrateFromLegacySchemas(
    log,
    db,
    schema,
    appID === 'zero' ? `cdc_0` : `cdc_${appID}`,
    'cdc',
  );

  const setupMigration: Migration = {
    migrateSchema: (lc, tx) => setupCDCTables(lc, tx, shard),
    minSafeVersion: 1,
  };

  async function migrateV1toV2(_: LogContext, db: PostgresTransaction) {
    await db`
    ALTER TABLE ${db(schema)}."replicationConfig" ADD "resetRequired" BOOL`;
  }

  const migrateV2ToV3 = {
    migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
      await db.unsafe(createReplicationStateTable(shard));
    },

    migrateData: async (_: LogContext, db: PostgresTransaction) => {
      const lastWatermark = await getLastWatermarkV2(db, shard);
      const replicationState: Partial<ReplicationState> = {lastWatermark};
      await db`
      TRUNCATE TABLE ${db(schema)}."replicationState"`;
      await db`
      INSERT INTO ${db(schema)}."replicationState" ${db(replicationState)}`;
    },
  };

  const migrateV3ToV4 = {
    migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
      await db`
      ALTER TABLE ${db(schema)}."changeLog" ALTER "change" TYPE JSON;
      `;
    },
  };

  const migrateV4ToV5 = {
    migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
      await db`
      ALTER TABLE ${db(schema)}."replicationState" ADD "ownerAddress" TEXT;
      `;
    },
  };

  const migrateV5ToV6 = {
    migrateSchema: async (_: LogContext, db: PostgresTransaction) => {
      await db.unsafe(createBackfillTables(shard));
    },
  };

  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: {migrateSchema: migrateV1toV2},
    3: migrateV2ToV3,
    4: migrateV3ToV4,
    5: migrateV4ToV5,
    6: migrateV5ToV6,
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
  shard: ShardID,
): Promise<string> {
  const schema = cdcSchema(shard);
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
