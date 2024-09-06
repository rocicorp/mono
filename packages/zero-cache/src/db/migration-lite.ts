import type {LogContext} from '@rocicorp/logger';
import type {Database as Db} from 'zqlite/src/db.js';
import {Database} from 'zqlite/src/db.js';
import {assert} from 'shared/src/asserts.js';
import {randInt} from 'shared/src/rand.js';
import * as v from 'shared/src/valita.js';

/**
 * A PreMigrationFn executes logic outside of a database transaction, and is
 * suitable for potentially long running polling operations.
 */
type PreMigrationFn = (log: LogContext, db: Db) => Promise<void> | void;

type MigrationFn = (log: LogContext, tx: Db) => Promise<void> | void;

/**
 * Encapsulates the logic for upgrading to a new schema. After the
 * Migration code successfully completes, {@link runSchemaMigrations}
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
 * Ensures that the schema is compatible with the current code, updating and
 * migrating the schema if necessary.
 */
export async function runSchemaMigrations(
  log: LogContext,
  debugName: string,
  dbPath: string,
  versionMigrationMap: VersionMigrationMap,
): Promise<void> {
  log = log.withContext(
    'initSchema',
    randInt(0, Number.MAX_SAFE_INTEGER).toString(36),
  );
  const db = new Database(log, dbPath);
  db.pragma('foreign_keys = OFF');
  db.pragma('journal_mode = WAL');

  try {
    const versionMigrations = sorted(versionMigrationMap);
    if (versionMigrations.length === 0) {
      log.info?.(`No versions/migrations to manage.`);
      return;
    }
    const codeSchemaVersion =
      versionMigrations[versionMigrations.length - 1][0];
    log.info?.(
      `Checking schema for compatibility with ${debugName} at schema v${codeSchemaVersion}`,
    );

    let meta = await runTransaction(log, db, tx => {
      const meta = getSchemaVersions(tx);
      if (codeSchemaVersion < meta.minSafeRollbackVersion) {
        throw new Error(
          `Cannot run ${debugName} at schema v${codeSchemaVersion} because rollback limit is v${meta.minSafeRollbackVersion}`,
        );
      }

      if (meta.version > codeSchemaVersion) {
        log.info?.(
          `Schema is at v${meta.version}. Resetting to v${codeSchemaVersion}`,
        );
        return setSchemaVersion(tx, meta, codeSchemaVersion);
      }
      return meta;
    });

    if (meta.version < codeSchemaVersion) {
      for (const [dest, migration] of versionMigrations) {
        if (meta.version < dest) {
          log.info?.(`Migrating schema from v${meta.version} to v${dest}`);
          void log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          db.pragma('synchronous = OFF'); // For schema migrations we'll wait for the disk flush after the migration.

          // Run the optional PreMigration step before starting the transaction.
          if ('pre' in migration) {
            await migration.pre(log, db);
          }

          meta = await runTransaction(log, db, async tx => {
            // Fetch meta from within the transaction to make the migration atomic.
            let meta = getSchemaVersions(tx);
            if (meta.version < dest) {
              meta = await migrateSchemaVersion(log, tx, meta, dest, migration);
              assert(meta.version === dest);
            }
            return meta;
          });

          db.pragma('synchronous = NORMAL');
          db.exec('VACUUM');
          log.debug?.('VACUUM completed');
        }
      }
    }

    assert(meta.version === codeSchemaVersion);
    log.info?.(`Running ${debugName} at schema v${codeSchemaVersion}`);
  } catch (e) {
    log.error?.('Error in ensureSchemaMigrated', e);
    throw e;
  } finally {
    db.close();
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
export const schemaVersions = v.object({
  version: v.number(),
  maxVersion: v.number(),
  minSafeRollbackVersion: v.number(),
});

// Exposed for tests.
export type SchemaVersions = v.Infer<typeof schemaVersions>;

// Exposed for tests
export function getSchemaVersions(db: Db): SchemaVersions {
  // Note: The `lock` column transparently ensures that at most one row exists.
  db.prepare(
    `
    CREATE TABLE IF NOT EXISTS "_zero.SchemaVersions" (
      version INTEGER NOT NULL,
      maxVersion INTEGER NOT NULL,
      minSafeRollbackVersion INTEGER NOT NULL,

      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
  `,
  ).run();
  const result = db
    .prepare(
      'SELECT version, maxVersion, minSafeRollbackVersion FROM "_zero.SchemaVersions"',
    )
    .get() as SchemaVersions;
  return result ?? {version: 0, maxVersion: 0, minSafeRollbackVersion: 0};
}

function setSchemaVersion(
  db: Db,
  prev: SchemaVersions,
  newVersion: number,
): SchemaVersions {
  assert(newVersion > 0);
  const meta = {
    ...prev,
    version: newVersion,
    maxVersion: Math.max(newVersion, prev.maxVersion),
  };

  db.prepare(
    `
    INSERT INTO "_zero.SchemaVersions" (version, maxVersion, minSafeRollbackVersion, lock)
    VALUES (@version, @maxVersion, @minSafeRollbackVersion, 1)
    ON CONFLICT (lock) DO UPDATE
    SET version=EXCLUDED.version, 
        maxVersion=EXCLUDED.maxVersion,
        minSafeRollbackVersion=EXCLUDED.minSafeRollbackVersion
  `,
  ).run(meta);

  return meta;
}

async function migrateSchemaVersion(
  log: LogContext,
  tx: Db,
  meta: SchemaVersions,
  destinationVersion: number,
  migration: Migration,
): Promise<SchemaVersions> {
  if ('run' in migration) {
    await migration.run(log, tx);
  } else {
    meta = ensureRollbackLimit(migration.minSafeRollbackVersion, log, meta);
  }
  return setSchemaVersion(tx, meta, destinationVersion);
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
function ensureRollbackLimit(
  toAtLeast: number,
  log: LogContext,
  meta: SchemaVersions,
): SchemaVersions {
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

// Note: We use a custom transaction wrapper (instead of db.begin(...)) in order
// to support async operations within the transaction.
async function runTransaction<T>(
  log: LogContext,
  db: Db,
  tx: (db: Db) => Promise<T> | T,
): Promise<T> {
  db.prepare('BEGIN EXCLUSIVE').run();
  try {
    const result = await tx(db);
    db.prepare('COMMIT').run();
    return result;
  } catch (e) {
    db.prepare('ROLLBACK').run();
    log.error?.('Aborted transaction due to error', e);
    throw e;
  }
}
