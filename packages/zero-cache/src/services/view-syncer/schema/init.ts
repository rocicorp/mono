import type {LogContext} from '@rocicorp/logger';
import type {PendingQuery, Row} from 'postgres';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration.js';
import type {PostgresDB} from '../../../types/pg.js';
import {
  CREATE_CVR_ROWS_VERSION_TABLE,
  PG_SCHEMA,
  setupCVRTables,
} from './cvr.js';

const setupMigration: Migration = {
  migrateSchema: setupCVRTables,
  minSafeVersion: 1,
};

export async function initViewSyncerSchema(
  log: LogContext,
  db: PostgresDB,
): Promise<void> {
  const schemaVersionMigrationMap: IncrementalMigrationMap = {
    2: migrateV1toV2,
    3: migrateV2ToV3,
  };

  await runSchemaMigrations(
    log,
    'view-syncer',
    PG_SCHEMA,
    db,
    setupMigration,
    schemaVersionMigrationMap,
  );
}

const migrateV1toV2: Migration = {
  migrateSchema: async (_, tx) => {
    await tx`ALTER TABLE cvr.instances ADD "replicaVersion" TEXT`;
  },
};

const migrateV2ToV3: Migration = {
  migrateSchema: async (_, tx) => {
    await tx.unsafe(CREATE_CVR_ROWS_VERSION_TABLE);
  },

  /** Populates the cvr.rowsVersion table with versions from cvr.instances. */
  migrateData: async (lc, tx) => {
    const pending: PendingQuery<Row[]>[] = [];
    for await (const versions of tx<{clientGroupID: string; version: string}[]>`
      SELECT "clientGroupID", "version" FROM cvr.instances`.cursor(5000)) {
      for (const version of versions) {
        pending.push(
          tx`INSERT INTO cvr."rowsVersion" ${tx(version)} 
               ON CONFLICT ("clientGroupID")
               DO UPDATE SET ${tx(version)}`.execute(),
        );
      }
    }
    lc.info?.(`initializing rowsVersion for ${pending.length} cvrs`);
    await Promise.all(pending);
  },
};
