import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  disableStatementTimeout,
  type PostgresDB,
  type PostgresTransaction,
} from '../types/pg.ts';

type Operations = (log: LogContext, tx: PostgresTransaction) => Promise<void>;

/**
 * Encapsulates the logic for setting up or upgrading to a new schema. After the
 * Migration code successfully completes, {@link runSchemaMigrations}
 * will update the schema version and commit the transaction.
 */
export type Migration = {
  /**
   * Perform database operations that create or alter table structure. This is
   * called at most once during lifetime of the application. If a `migrateData()`
   * operation is defined, that will be performed after `migrateSchema()` succeeds.
   */
  migrateSchema?: Operations;

  /**
   * Perform database operations to migrate data to the new schema. This is
   * called after `migrateSchema()` (if defined), and may be called again
   * to re-migrate data after the server was rolled back to an earlier version,
   * and rolled forward again.
   *
   * Consequently, the logic in `migrateData()` must be idempotent.
   */
  migrateData?: Operations;

  /**
   * Sets the `minSafeVersion` to the specified value, prohibiting running
   * any earlier code versions.
   */
  minSafeVersion?: number;
};

/**
 * Mapping of incremental migrations to move from the previous old code
 * version to next one. Versions must be non-zero.
 *
 * The schema resulting from performing incremental migrations should be
 * equivalent to that of the `setupMigration` on a blank database.
 *
 * The highest destinationVersion of this map denotes the current
 * "code version", and is also used as the destination version when
 * running the initial setup migration on a blank database.
 */
export type IncrementalMigrationMap = {
  [destinationVersion: number]: Migration;
};

/**
 * Ensures that the schema is compatible with the current code, updating and
 * migrating the schema if necessary.
 */
export async function runSchemaMigrations(
  log: LogContext,
  debugName: string,
  schemaName: string,
  db: PostgresDB,
  setupMigration: Migration,
  incrementalMigrationMap: IncrementalMigrationMap,
): Promise<void> {
  log = log.withContext('initSchema', schemaName);
  try {
    const versionMigrations = sorted(incrementalMigrationMap);
    assert(
      versionMigrations.length,
      `Must specify at least one version migration`,
    );
    assert(
      versionMigrations[0][0] > 0,
      `Versions must be non-zero positive numbers`,
    );
    const codeVersion = versionMigrations[versionMigrations.length - 1][0];
    log.info?.(
      `Checking schema for compatibility with ${debugName} at schema v${codeVersion}`,
    );

    let versions = await db.begin(async tx => {
      const versions = await ensureVersionHistory(tx, schemaName);
      if (codeVersion < versions.minSafeVersion) {
        throw new Error(
          `Cannot run ${debugName} at schema v${codeVersion} because rollback limit is v${versions.minSafeVersion}`,
        );
      }

      if (versions.dataVersion > codeVersion) {
        log.info?.(
          `Data is at v${versions.dataVersion}. Resetting to v${codeVersion}`,
        );
        return updateVersionHistory(log, tx, schemaName, versions, codeVersion);
      }
      return versions;
    });

    if (versions.dataVersion < codeVersion) {
      const migrations =
        versions.dataVersion === 0
          ? // For the empty database v0, only run the setup migration.
            ([[codeVersion, setupMigration]] as const)
          : versionMigrations;

      for (const [dest, migration] of migrations) {
        if (versions.dataVersion < dest) {
          log.info?.(
            `Migrating schema from v${versions.dataVersion} to v${dest}`,
          );
          void log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          versions = await db.begin(async tx => {
            disableStatementTimeout(tx);

            // Fetch meta from within the transaction to make the migration atomic.
            let versions = await ensureVersionHistory(tx, schemaName);
            if (versions.dataVersion < dest) {
              versions = await runMigration(
                log,
                schemaName,
                tx,
                versions,
                dest,
                migration,
              );
              assert(
                versions.dataVersion === dest,
                () =>
                  `Migration did not reach target version: expected ${dest}, got ${versions.dataVersion}`,
              );
            }
            return versions;
          });
        }
      }
    }

    assert(
      versions.dataVersion === codeVersion,
      () =>
        `Final dataVersion (${versions.dataVersion}) does not match codeVersion (${codeVersion})`,
    );
    log.info?.(`Running ${debugName} at schema v${codeVersion}`);
  } catch (e) {
    log.error?.('Error in ensureSchemaMigrated', e);
    throw e;
  } finally {
    void log.flush(); // Flush the logs but do not block server progress on it.
  }
}

function sorted(
  incrementalMigrationMap: IncrementalMigrationMap,
): [number, Migration][] {
  const versionMigrations: [number, Migration][] = [];
  for (const [v, m] of Object.entries(incrementalMigrationMap)) {
    versionMigrations.push([Number(v), m]);
  }
  return versionMigrations.sort(([a], [b]) => a - b);
}

// Exposed for tests.
export const versionHistory = v.object({
  /**
   * The `schemaVersion` is highest code version that has ever been run
   * on the database, and is used to delineate the structure of the tables
   * in the database. A schemaVersion only moves forward; rolling back to
   * an earlier (safe) code version does not revert schema changes that
   * have already been applied.
   */
  schemaVersion: v.number(),

  /**
   * The data version is the code version of the latest server that ran.
   * Note that this may be less than the schemaVersion in the case that
   * a server is rolled back to an earlier version after a schema change.
   * In such a case, data (but not schema), may need to be re-migrated
   * when rolling forward again.
   */
  dataVersion: v.number(),

  /**
   * The minimum code version that is safe to run. This is used when
   * a schema migration is not backwards compatible with an older version
   * of the code.
   */
  minSafeVersion: v.number(),
});

// Exposed for tests.
export type VersionHistory = v.Infer<typeof versionHistory>;

// Exposed for tests.
export async function createVersionHistoryTable(
  sql: postgres.Sql,
  schemaName: string,
) {
  // Note: The `lock` column transparently ensures that at most one row exists.
  await sql`
    CREATE SCHEMA IF NOT EXISTS ${sql(schemaName)};
    CREATE TABLE IF NOT EXISTS ${sql(schemaName)}."versionHistory" (
      "dataVersion" int NOT NULL,
      "schemaVersion" int NOT NULL,
      "minSafeVersion" int NOT NULL,

      lock char(1) NOT NULL CONSTRAINT DF_schema_meta_lock DEFAULT 'v',
      CONSTRAINT PK_schema_meta_lock PRIMARY KEY (lock),
      CONSTRAINT CK_schema_meta_lock CHECK (lock='v')
    );`.simple();
}

async function ensureVersionHistory(
  sql: postgres.Sql,
  schemaName: string,
): Promise<VersionHistory> {
  return must(await getVersionHistory(sql, schemaName, true));
}

export async function getVersionHistory(
  sql: postgres.Sql,
  schemaName: string,
  create = false,
): Promise<VersionHistory | null> {
  const exists = await sql`
  SELECT nspname, relname FROM pg_class 
    JOIN pg_namespace ON relnamespace = pg_namespace.oid
    WHERE nspname = ${schemaName} AND relname = ${'versionHistory'}`;

  if (exists.length === 0) {
    if (create) {
      await createVersionHistoryTable(sql, schemaName);
    } else {
      return null;
    }
  }
  const rows = await sql`
    SELECT "dataVersion", "schemaVersion", "minSafeVersion" 
       FROM ${sql(schemaName)}."versionHistory"`;

  if (rows.length === 0) {
    return create
      ? {schemaVersion: 0, dataVersion: 0, minSafeVersion: 0}
      : null;
  }
  return v.parse(rows[0], versionHistory);
}

async function updateVersionHistory(
  log: LogContext,
  sql: postgres.Sql,
  schemaName: string,
  prev: VersionHistory,
  newVersion: number,
  minSafeVersion?: number,
): Promise<VersionHistory> {
  assert(newVersion > 0, 'newVersion must be positive');
  const versions = {
    dataVersion: newVersion,
    // The schemaVersion never moves backwards.
    schemaVersion: Math.max(newVersion, prev.schemaVersion),
    minSafeVersion: getMinSafeVersion(log, prev, minSafeVersion),
  } satisfies VersionHistory;

  await sql`
    INSERT INTO ${sql(schemaName)}."versionHistory" ${sql(versions)}
      ON CONFLICT (lock) DO UPDATE SET ${sql(versions)}
  `;
  return versions;
}

async function runMigration(
  log: LogContext,
  schemaName: string,
  tx: PostgresTransaction,
  versions: VersionHistory,
  destinationVersion: number,
  migration: Migration,
): Promise<VersionHistory> {
  if (versions.schemaVersion < destinationVersion) {
    await migration.migrateSchema?.(log, tx);
  }
  if (versions.dataVersion < destinationVersion) {
    await migration.migrateData?.(log, tx);
  }
  return updateVersionHistory(
    log,
    tx,
    schemaName,
    versions,
    destinationVersion,
    migration.minSafeVersion,
  );
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
function getMinSafeVersion(
  log: LogContext,
  current: VersionHistory,
  proposedSafeVersion?: number,
): number {
  if (proposedSafeVersion === undefined) {
    return current.minSafeVersion;
  }
  if (current.minSafeVersion >= proposedSafeVersion) {
    // The rollback limit must never move backwards.
    log.debug?.(
      `rollback limit is already at ${current.minSafeVersion}, ` +
        `don't need to bump to ${proposedSafeVersion}`,
    );
    return current.minSafeVersion;
  }
  log.info?.(
    `bumping rollback limit from ${current.minSafeVersion} to ${proposedSafeVersion}`,
  );
  return proposedSafeVersion;
}
