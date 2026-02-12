import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {listTables} from '../../../db/lite-tables.ts';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {populateFromExistingTables} from '../../replicator/schema/column-metadata.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../../replicator/schema/replication-state.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../../replicator/schema/table-metadata.ts';

export async function initReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
  initialSync: (lc: LogContext, tx: Database) => Promise<void>,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) => initialSync(log, tx),
    minSafeVersion: 1,
  };

  try {
    await runSchemaMigrations(
      log,
      debugName,
      dbPath,
      setupMigration,
      schemaVersionMigrationMap,
    );
  } catch (e) {
    if (e instanceof SqliteError && e.code === 'SQLITE_CORRUPT') {
      throw new AutoResetSignal(e.message);
    }
    throw e;
  }
}

export async function upgradeReplica(
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

export const CREATE_V6_COLUMN_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "_zero.column_metadata" (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    upstream_type TEXT NOT NULL,
    is_not_null INTEGER NOT NULL,
    is_enum INTEGER NOT NULL,
    is_array INTEGER NOT NULL,
    character_max_length INTEGER,
    PRIMARY KEY (table_name, column_name)
  );
`;

export const CREATE_V7_CHANGE_LOG = /*sql*/ `
  CREATE TABLE "_zero.changeLog2" (
    "stateVersion"              TEXT NOT NULL,
    "pos"                       INT  NOT NULL,
    "table"                     TEXT NOT NULL,
    "rowKey"                    TEXT NOT NULL,
    "op"                        TEXT NOT NULL,
    PRIMARY KEY("stateVersion", "pos"),
    UNIQUE("table", "rowKey")
  );
`;

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

  // Revised in the migration to v8 because the v6 code was incomplete.
  6: {},

  7: {
    migrateSchema: (_, db) => {
      // Note: The original "changeLog" table is kept so that the replica file
      // is compatible with older zero-caches. However, it is truncated for
      // space savings (since historic changes were never read).
      db.exec(`DELETE FROM "_zero.changeLog"`);
      // First version of changeLog2
      db.exec(CREATE_V7_CHANGE_LOG);
    },
  },

  8: {
    migrateSchema: (_, db) => {
      const tableExists = db
        .prepare(
          `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_zero.column_metadata'`,
        )
        .get();

      if (!tableExists) {
        db.exec(CREATE_V6_COLUMN_METADATA_TABLE);
      }
    },
    migrateData: (_, db) => {
      // Re-populate the ColumnMetadataStore; the original migration
      // at v6 was incomplete, as covered replicas migrated from earlier
      // versions but did not initialize the table for new replicas.
      db.exec(/*sql*/ `DELETE FROM "_zero.column_metadata"`);

      const tables = listTables(db, false);
      populateFromExistingTables(db, tables);
    },
  },

  9: {
    migrateSchema: (_, db) => {
      db.exec(
        /*sql*/ `
        ALTER TABLE "_zero.changeLog2" 
          ADD COLUMN "backfillingColumnVersions" TEXT DEFAULT '{}';
        ALTER TABLE "_zero.column_metadata"
          ADD COLUMN backfill TEXT;
      ` + CREATE_TABLE_METADATA_TABLE,
      );
    },
  },
};
