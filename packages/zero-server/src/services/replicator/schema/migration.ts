import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {randInt} from 'shared/src/rand.js';
import * as v from 'shared/src/valita.js';

/**
 * Encapsulates the logic for upgrading to a new schema. After the
 * Migration code successfully completes, {@link runSyncSchemaMigrations}
 * will update the schema version and commit the transaction.
 */
export type Migration =
  | ((log: LogContext, tx: postgres.Sql) => Promise<void>)
  // A special Migration type that pushes the rollback limit forward.
  | {minSafeRollbackVersion: number};

/** Mapping from schema version to their respective migrations. */
export type VersionMigrationMap = {
  [destinationVersion: number]: Migration;
};

/**
 * Ensures that the sync schema is compatible with the current code,
 * updating and migrating the schema if necessary.
 */
export async function runSyncSchemaMigrations(
  log: LogContext,
  sql: postgres.Sql,
  versionMigrationMap: VersionMigrationMap,
): Promise<void> {
  log = log.withContext(
    'initSyncSchema',
    randInt(0, Number.MAX_SAFE_INTEGER).toString(36),
  );
  try {
    const versionMigrations = sorted(versionMigrationMap);
    if (versionMigrations.length === 0) {
      log.info?.(`No versions/migrations to manage.`);
      return;
    }
    const codeSchemaVersion =
      versionMigrations[versionMigrations.length - 1][0];
    log.info?.(
      `Checking schema for compatibility with server at schema v${codeSchemaVersion}`,
    );

    await ensureSyncSchemaMetaTable(sql);

    let meta = await sql.begin(async tx => {
      const meta = await getSyncSchemaMeta(tx);
      if (codeSchemaVersion < meta.minSafeRollbackVersion) {
        throw new Error(
          `Cannot run server at schema v${codeSchemaVersion} because rollback limit is v${meta.minSafeRollbackVersion}`,
        );
      }

      if (meta.version > codeSchemaVersion) {
        log.info?.(
          `Schema is at v${meta.version}. Resetting to v${codeSchemaVersion}`,
        );
        return setSyncSchemaVersion(tx, meta, codeSchemaVersion);
      }
      return meta;
    });

    if (meta.version < codeSchemaVersion) {
      for (const [dest, migration] of versionMigrations) {
        if (meta.version < dest) {
          log.info?.(`Migrating schema from v${meta.version} to v${dest}`);
          await log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          meta = await sql.begin(async tx => {
            // Fetch meta from with the transaction to make the migration atomic.
            let meta = await getSyncSchemaMeta(tx);
            if (meta.version < dest) {
              meta = await migrateSyncSchemaVersion(
                log,
                tx,
                meta,
                dest,
                migration,
              );
              assert(meta.version === dest);
            }
            return meta;
          });
        }
      }
    }

    assert(meta.version === codeSchemaVersion);
    log.info?.(`Running server at schema v${codeSchemaVersion}`);
  } catch (e) {
    log.error?.('Error in ensureSyncSchemaMigrated', e);
    throw e;
  } finally {
    await log.flush();
  }
}

function sorted(
  versionMigrationMap: VersionMigrationMap,
): [number, Migration][] {
  const versionMigrations: [number, Migration][] = [];
  for (const [v, m] of Object.entries(versionMigrationMap)) {
    versionMigrations.push([Number(v), m]);
  }
  return versionMigrations.sort(([a], [b]) => a - b);
}

// Exposed for tests.
export const syncSchemaMeta = v.object({
  version: v.number(),
  maxVersion: v.number(),
  minSafeRollbackVersion: v.number(),
});

// Exposed for tests.
export type SyncSchemaMeta = v.Infer<typeof syncSchemaMeta>;

export async function ensureSyncSchemaMetaTable(
  sql: postgres.Sql,
): Promise<void> {
  await sql.begin(tx => [
    tx`
    CREATE SCHEMA IF NOT EXISTS zero`,

    // Note: The `lock` column transparently ensures that only one row exists.
    tx`
    CREATE TABLE IF NOT EXISTS zero.schema_meta (
      version int NOT NULL,
      max_version int NOT NULL,
      min_safe_rollback_version int NOT NULL,

      lock char(1) NOT NULL CONSTRAINT DF_schema_meta_lock DEFAULT 'v',
      CONSTRAINT PK_schema_meta_lock PRIMARY KEY (lock),
      CONSTRAINT CK_schema_meta_lock CHECK (lock='v')
    )
    `,
  ]);
}

// Exposed for tests
export async function getSyncSchemaMeta(
  sql: postgres.Sql,
): Promise<SyncSchemaMeta> {
  const rows = await sql`
    SELECT version, max_version, min_safe_rollback_version FROM zero.schema_meta
  `;
  if (rows.count === 0) {
    return {version: 0, maxVersion: 0, minSafeRollbackVersion: 0};
  }
  return v.parse(rows[0], syncSchemaMeta);
}

async function setSyncSchemaVersion(
  sql: postgres.Sql,
  prev: SyncSchemaMeta,
  newVersion: number,
): Promise<SyncSchemaMeta> {
  assert(newVersion > 0);
  const meta = {
    ...prev,
    version: newVersion,
    maxVersion: Math.max(newVersion, prev.maxVersion),
  };

  if (prev.version === 0) {
    await sql`INSERT INTO zero.schema_meta ${sql(meta)}`;
  } else {
    await sql`UPDATE zero.schema_meta set ${sql(meta)}`;
  }
  return meta;
}

async function migrateSyncSchemaVersion(
  log: LogContext,
  tx: postgres.Sql,
  meta: SyncSchemaMeta,
  destinationVersion: number,
  migration: Migration,
): Promise<SyncSchemaMeta> {
  if (typeof migration === 'function') {
    await migration(log, tx);
  } else {
    meta = ensureRollbackLimit(migration.minSafeRollbackVersion, log, meta);
  }
  return setSyncSchemaVersion(tx, meta, destinationVersion);
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
function ensureRollbackLimit(
  toAtLeast: number,
  log: LogContext,
  meta: SyncSchemaMeta,
): SyncSchemaMeta {
  // Sanity check to maintain the invariant that running code is never
  // earlier than the rollback limit.
  assert(toAtLeast <= meta.version + 1);

  if (meta.minSafeRollbackVersion >= toAtLeast) {
    // The rollback limit must never move backwards.
    log.debug?.(
      `rollback limit is already at ${meta.minSafeRollbackVersion}, don't need to bump to ${toAtLeast}`,
    );
    return meta;
  }
  log.info?.(
    `bumping rollback limit from ${meta.minSafeRollbackVersion} to ${toAtLeast}`,
  );
  return {
    ...meta,
    minSafeRollbackVersion: toAtLeast,
  };
}
