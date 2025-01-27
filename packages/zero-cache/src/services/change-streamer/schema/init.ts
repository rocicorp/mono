import type {LogContext} from '@rocicorp/logger';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.ts';
import type {PostgresDB, PostgresTransaction} from '../../../types/pg.ts';
import {PG_SCHEMA, setupCDCTables} from './tables.ts';

const setupMigration: Migration = {
  migrateSchema: setupCDCTables,
  minSafeVersion: 1,
};

const schemaVersionMigrationMap: IncrementalMigrationMap = {
  2: {migrateSchema: migrateV1toV2},
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

async function migrateV1toV2(_: LogContext, db: PostgresTransaction) {
  await db`ALTER TABLE cdc."replicationConfig" ADD "resetRequired" BOOL`;
}
