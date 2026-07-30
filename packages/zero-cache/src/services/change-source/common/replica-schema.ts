import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../../shared/src/asserts.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {listTables} from '../../../db/lite-tables.ts';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../../db/migration-lite.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
} from '../../../db/sqlite-corruption.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {populateFromExistingTables} from '../../replicator/schema/column-metadata.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../../replicator/schema/replication-state.ts';

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
    throwAutoResetForCorruption(log, debugName, dbPath, e);
  }
}

export async function upgradeReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
) {
  try {
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
  } catch (e) {
    throwAutoResetForCorruption(log, debugName, dbPath, e);
  }
}

function throwAutoResetForCorruption(
  log: LogContext,
  debugName: string,
  dbPath: string,
  e: unknown,
): never {
  if (isSQLiteCorruption(e)) {
    logSQLiteCorruptionDiagnostics(log, debugName, dbPath, e);
    throw new AutoResetSignal(
      `replica database appears corrupt: ${String(e)}`,
      {cause: e},
    );
  }
  throw e;
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

export const CREATE_V9_TABLE_METADATA_TABLE = /*sql*/ `
  CREATE TABLE "_zero.tableMetadata" (
    "schema"    TEXT NOT NULL,
    "table"     TEXT NOT NULL,
    "metadata"  TEXT NOT NULL,
    PRIMARY KEY ("schema", "table")
  );
`;

// Deliberately shadows the same names in `replicator/change-log-db.ts`: these
// are the v14 replica's, that module's are the change-log database's, and the
// two are frozen apart.
export const V14_CHANGE_LOG_STREAM_TABLE = '_zero.changeLogStream';
const V14_CHANGE_LOG_STREAM_WRITE_TIME_INDEX =
  '_zero.changeLogStream_writeTimeMs';

// The change-log stream table, as migration 14 created it and migration 16
// drops it. Frozen at its v14 shape: a migration is history, so this must not
// be kept in sync with the live definition in `replicator/change-log-db.ts`,
// which now describes a table in a different database.
export const CREATE_V14_CHANGE_LOG_STREAM = /*sql*/ `
  CREATE TABLE "${V14_CHANGE_LOG_STREAM_TABLE}" (
    "watermark"   TEXT NOT NULL,
    "pos"         INTEGER NOT NULL,
    "change"      TEXT NOT NULL,
    "precommit"   TEXT,
    "writeTimeMs" INTEGER,
    PRIMARY KEY ("watermark", "pos")
  );

  CREATE INDEX "${V14_CHANGE_LOG_STREAM_WRITE_TIME_INDEX}"
    ON "${V14_CHANGE_LOG_STREAM_TABLE}" ("writeTimeMs", "watermark")
    WHERE "writeTimeMs" IS NOT NULL;
`;

// Idempotent because migrateData may run again after a rollback followed by a
// roll-forward, and both rows go in with one statement so a partial seed is
// not reachable.
const SEED_V14_CHANGE_LOG_STREAM = /*sql*/ `
  INSERT INTO "${V14_CHANGE_LOG_STREAM_TABLE}"
    ("watermark", "pos", "change", "precommit", "writeTimeMs")
  VALUES
    (@stateVersion, 0, '{"tag":"begin"}', NULL, NULL),
    (@stateVersion, 1, '{"tag":"commit"}', @stateVersion, @writeTimeMs)
  ON CONFLICT ("watermark", "pos") DO NOTHING
`;

function seedV14ChangeLogStream(db: Database): void {
  const state = db
    .prepare(/*sql*/ `
      SELECT "stateVersion", "writeTimeMs" FROM "_zero.replicationState"
    `)
    .get<{stateVersion: string; writeTimeMs: number | null} | undefined>();
  assert(state !== undefined, 'replication state must be initialized');
  assert(
    state.writeTimeMs !== null,
    'replication state writeTimeMs must be initialized',
  );
  db.prepare(SEED_V14_CHANGE_LOG_STREAM).run(state);
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

      const tables = listTables(db, false, false);
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
      ` + CREATE_V9_TABLE_METADATA_TABLE,
      );
    },
  },

  10: {
    migrateSchema: (_, db) => {
      db.exec(/*sql*/ `
        ALTER TABLE "_zero.replicationConfig" 
          ADD COLUMN "initialSyncContext" TEXT DEFAULT '{}';
      `);
    },
  },

  11: {
    migrateSchema: (_, db) => {
      db.exec(/*sql*/ `
        ALTER TABLE "_zero.tableMetadata"
          ADD COLUMN "minRowVersion" TEXT NOT NULL DEFAULT '00';

        -- Removing the NOT NULL constraint from "metadata" requires copying
        -- the column. We piggyback the rename to "upstreamMetadata" here.
        ALTER TABLE "_zero.tableMetadata"
          ADD COLUMN "upstreamMetadata" TEXT;
        UPDATE "_zero.tableMetadata" SET "upstreamMetadata" = "metadata";
        ALTER TABLE "_zero.tableMetadata" DROP "metadata";
      `);
    },
  },

  12: {
    migrateSchema: (_, db) => {
      // Bring back the "metadata" column removed in v11, but as a NULL-able column.
      // It is needed for backwards compatibility.
      db.exec(/*sql*/ `
        ALTER TABLE "_zero.tableMetadata"
          ADD COLUMN "metadata" TEXT;
      `);
    },

    migrateData: (_, db) => {
      // For rollback then roll forward, re-copy anything written to metadata.
      db.exec(/*sql*/ `
        UPDATE "_zero.tableMetadata" 
          SET "upstreamMetadata" = COALESCE("metadata", "upstreamMetadata"),
              "metadata" = NULL;
      `);
    },
  },

  13: {
    migrateSchema: (_, db) => {
      db.exec(/*sql*/ `
        ALTER TABLE "_zero.replicationState" ADD COLUMN writeTimeMs INTEGER;
      `);
    },

    migrateData: (_, db) => {
      db.exec(/*sql*/ `
        UPDATE "_zero.replicationState" 
          SET writeTimeMs = COALESCE(writeTimeMs, unixepoch('subsec') * 1000)`);
    },
  },

  // Deliberately left as it shipped, even though v16 immediately undoes it.
  // Rewriting it to a no-op would strand replicas already at v14: they roll
  // back to v14 code, which expects the table this created.
  14: {
    migrateSchema: (_, db) => {
      db.exec(CREATE_V14_CHANGE_LOG_STREAM);
    },

    migrateData: (_, db) => {
      seedV14ChangeLogStream(db);
    },
  },

  15: {
    migrateSchema: (_, db) => {
      // Make writeTimeMs NOT NULL. In SQLite the only way to do this is to
      // create a new table, copy the data over, delete the old table, and
      // rename. This is cheap because it's a single row table.
      db.exec(/*sql*/ `
        CREATE TABLE "_zero.replicationState2" (
          stateVersion TEXT NOT NULL,
          writeTimeMs INTEGER NOT NULL,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );

        INSERT INTO "_zero.replicationState2" (stateVersion, writeTimeMs, lock)
          SELECT stateVersion, writeTimeMs, lock FROM "_zero.replicationState";

        DROP TABLE "_zero.replicationState";

        ALTER TABLE "_zero.replicationState2" RENAME TO "_zero.replicationState";
      `);
    },
  },

  // The change log moved out of the replica and into its own database
  // (`${replicaFile}-change-log`), so the replica's copy is dead weight in
  // every file the backup ships and every follower downloads. Nothing reads it
  // as of this version.
  //
  // `IF EXISTS` because a fresh replica never runs the incremental migrations
  // at all — its setup migration creates the current schema directly, which no
  // longer includes this table — and because a rollback to v14 followed by a
  // roll-forward re-runs `migrateData` while skipping `migrateSchema`.
  //
  // The freed pages are reclaimed by the existing `vacuumIntervalHours`
  // trigger in `workers/replicator.ts`; no special handling here.
  //
  // No `minSafeVersion`: an older zero-cache still runs against a v16 replica,
  // and with `sqliteChangeLogMode=off` (the default) it never looks for this
  // table. Bumping the rollback limit would turn every rollback into a hard
  // startup failure to protect a configuration that is not enabled anywhere.
  16: {
    migrateSchema: (_, db) => {
      db.exec(/*sql*/ `
        DROP INDEX IF EXISTS "${V14_CHANGE_LOG_STREAM_WRITE_TIME_INDEX}";
        DROP TABLE IF EXISTS "${V14_CHANGE_LOG_STREAM_TABLE}";
      `);
    },
  },
};

// Referenced in tests.
export const CURRENT_SCHEMA_VERSION = Object.keys(
  schemaVersionMigrationMap,
).reduce((prev, curr) => Math.max(prev, parseInt(curr)), 0);
