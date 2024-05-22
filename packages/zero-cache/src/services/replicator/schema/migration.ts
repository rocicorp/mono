import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {randInt} from 'shared/src/rand.js';
import * as v from 'shared/src/valita.js';

/**
 * A PreMigrationFn executes logic outside of a database transaction, and is
 * suitable for potentially long running polling operations.
 */
type PreMigrationFn = (
  log: LogContext,
  replicaID: string,
  replica: postgres.Sql,
  upstream: postgres.Sql,
  upstreamUri: string,
) => Promise<void>;

type MigrationFn = (
  log: LogContext,
  replicaID: string,
  tx: postgres.TransactionSql,
  upstream: postgres.Sql,
  upstreamUri: string,
) => Promise<void>;

/**
 * Encapsulates the logic for upgrading to a new schema. After the
 * Migration code successfully completes, {@link runSyncSchemaMigrations}
 * will update the schema version and commit the transaction.
 */
export type Migration =
  | {pre?: PreMigrationFn; run: MigrationFn}
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
  replicaID: string,
  replica: postgres.Sql,
  upstream: postgres.Sql,
  upstreamUri: string,
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
      `Checking schema for compatibility with replicator at schema v${codeSchemaVersion}`,
    );

    let meta = await replica.begin(async tx => {
      const meta = await getSyncSchemaVersions(tx);
      if (codeSchemaVersion < meta.minSafeRollbackVersion) {
        throw new Error(
          `Cannot run replicator at schema v${codeSchemaVersion} because rollback limit is v${meta.minSafeRollbackVersion}`,
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
          void log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          // Run the optional PreMigration step before starting the transaction.
          if ('pre' in migration) {
            await migration.pre(log, replicaID, replica, upstream, upstreamUri);
          }

          meta = await replica.begin(async tx => {
            // Fetch meta from within the transaction to make the migration atomic.
            let meta = await getSyncSchemaVersions(tx);
            if (meta.version < dest) {
              meta = await migrateSyncSchemaVersion(
                log,
                replicaID,
                tx,
                upstream,
                upstreamUri,
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
    log.info?.(`Running replicator at schema v${codeSchemaVersion}`);
  } catch (e) {
    log.error?.('Error in ensureSyncSchemaMigrated', e);
    throw e;
  } finally {
    void log.flush(); // Flush the logs but do not block server progress on it.
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
export const syncSchemaVersions = v.object({
  version: v.number(),
  maxVersion: v.number(),
  minSafeRollbackVersion: v.number(),
});

// Exposed for tests.
export type SyncSchemaVersions = v.Infer<typeof syncSchemaVersions>;

// Exposed for tests
export async function getSyncSchemaVersions(
  sql: postgres.Sql,
): Promise<SyncSchemaVersions> {
  // Note: The `schema_meta.lock` column transparently ensures that at most one row exists.
  const results = await sql`
    CREATE SCHEMA IF NOT EXISTS _zero;
    CREATE TABLE IF NOT EXISTS _zero."SchemaVersions" (
      version int NOT NULL,
      "maxVersion" int NOT NULL,
      "minSafeRollbackVersion" int NOT NULL,

      lock char(1) NOT NULL CONSTRAINT DF_schema_meta_lock DEFAULT 'v',
      CONSTRAINT PK_schema_meta_lock PRIMARY KEY (lock),
      CONSTRAINT CK_schema_meta_lock CHECK (lock='v')
    );
    SELECT version, "maxVersion", "minSafeRollbackVersion" FROM _zero."SchemaVersions";
  `.simple();
  const rows = results[1];
  if (rows.length === 0) {
    return {version: 0, maxVersion: 0, minSafeRollbackVersion: 0};
  }
  return v.parse(rows[0], syncSchemaVersions);
}

async function setSyncSchemaVersion(
  sql: postgres.Sql,
  prev: SyncSchemaVersions,
  newVersion: number,
): Promise<SyncSchemaVersions> {
  assert(newVersion > 0);
  const meta = {
    ...prev,
    version: newVersion,
    maxVersion: Math.max(newVersion, prev.maxVersion),
  };

  if (prev.version === 0) {
    await sql`INSERT INTO _zero."SchemaVersions" ${sql(meta)}`;
  } else {
    await sql`UPDATE _zero."SchemaVersions" set ${sql(meta)}`;
  }
  return meta;
}

async function migrateSyncSchemaVersion(
  log: LogContext,
  replicaID: string,
  tx: postgres.TransactionSql,
  upstream: postgres.Sql,
  upstreamUri: string,
  meta: SyncSchemaVersions,
  destinationVersion: number,
  migration: Migration,
): Promise<SyncSchemaVersions> {
  if ('run' in migration) {
    await migration.run(log, replicaID, tx, upstream, upstreamUri);
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
  meta: SyncSchemaVersions,
): SyncSchemaVersions {
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
