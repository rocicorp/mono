import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {must} from '../../../../shared/src/must.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../db/migration-lite.ts';
import {AutoResetSignal} from '../change-streamer/schema/tables.ts';
import {CREATE_CHANGELOG_SCHEMA} from '../replicator/schema/change-log.ts';
import {
  ColumnMetadataStore,
  CREATE_COLUMN_METADATA_TABLE,
} from '../replicator/schema/column-metadata.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../replicator/schema/replication-state.ts';

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
      db.exec(CREATE_CHANGELOG_SCHEMA); // Creates _zero.changeLog2
    },
  },

  8: {
    migrateSchema: (_, db) => {
      let store = ColumnMetadataStore.getInstance(db);
      if (!store) {
        db.exec(CREATE_COLUMN_METADATA_TABLE);
      }
    },
    migrateData: (_, db) => {
      // Re-populate the ColumnMetadataStore; the original migration
      // at v6 was incomplete, as covered replicas migrated from earlier
      // versions but did not initialize the table for new replicas.
      db.exec(/*sql*/ `DELETE FROM "_zero.column_metadata"`);

      const store = ColumnMetadataStore.getInstance(db);
      const tables = listTables(db);
      must(store).populateFromExistingTables(tables);
    },
  },
};
