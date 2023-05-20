import * as v from 'shared/valita.js';
import {assert} from 'shared/asserts.js';
import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from './durable-storage.js';
import type {Storage} from './storage.js';

/**
 * Encapsulates schema upgrade logic to move to another schema version.
 * Note that Migrations do *not* need to flush (i.e. await their mutations),
 * as the schema update code will always follow a Migration with a flushing
 * update to the version. However, Migrations *can* flush if necessary
 * (e.g. for large, incremental migrations).
 */
export type Migration = (
  log: LogContext,
  storage: DurableStorage,
) => Promise<void>;

/** Mapping from schema version to their respective migrations. */
export type VersionMigrationMap = {
  [destinationVersion: number]: Migration;
};

/**
 * Ensures that the storage schema is compatible with the code, updating
 * and migrating the schema if necessary.
 */
export async function initStorageSchema(
  log: LogContext,
  storage: DurableStorage,
  versionMigrationMap: VersionMigrationMap,
): Promise<void> {
  const versionMigrations = sorted(versionMigrationMap);
  if (versionMigrations.length === 0) {
    log.debug?.(`No versions/migrations to manage.`);
    return;
  }
  const codeSchemaVersion = versionMigrations[versionMigrations.length - 1][0];
  log.debug?.(`Running server at schema v${codeSchemaVersion}`);

  let meta = await getStorageSchemaMeta(storage);
  if (codeSchemaVersion < meta.minSafeRollbackVersion) {
    throw new Error(
      `Cannot run server at schema v${codeSchemaVersion} because rollback limit is v${meta.minSafeRollbackVersion}`,
    );
  }

  if (meta.version > codeSchemaVersion) {
    log.info?.(
      `Storage is at v${meta.version}. Resetting to v${codeSchemaVersion}`,
    );
    meta = await setStorageSchemaVersion(storage, codeSchemaVersion);
  } else {
    for (const [dest, migration] of versionMigrations) {
      if (meta.version < dest) {
        log.info?.(`Migrating storage from v${meta.version} to v${dest}`);
        meta = await migrateStorageSchemaVersion(log, storage, dest, migration);
        assert(meta.version === dest);
      }
    }
  }

  assert(meta.version === codeSchemaVersion);
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

const STORAGE_SCHEMA_META_KEY = 'storage_schema_meta';

// Exposed for tests.
export const storageSchemaMeta = v.object({
  version: v.number(),
  maxVersion: v.number(),
  minSafeRollbackVersion: v.number(),
});

// Exposed for tests.
export type StorageSchemaMeta = v.Infer<typeof storageSchemaMeta>;

async function getStorageSchemaMeta(
  storage: Storage,
): Promise<StorageSchemaMeta> {
  return (
    (await storage.get(STORAGE_SCHEMA_META_KEY, storageSchemaMeta)) ?? {
      version: 0,
      maxVersion: 0,
      minSafeRollbackVersion: 0,
    }
  );
}

async function setStorageSchemaVersion(
  storage: DurableStorage,
  newVersion: number,
): Promise<StorageSchemaMeta> {
  const meta = await getStorageSchemaMeta(storage);
  meta.version = newVersion;
  meta.maxVersion = Math.max(newVersion, meta.maxVersion);

  await storage.put(STORAGE_SCHEMA_META_KEY, meta);
  return meta;
}

async function migrateStorageSchemaVersion(
  log: LogContext,
  storage: DurableStorage,
  destinationVersion: number,
  migration: Migration,
): Promise<StorageSchemaMeta> {
  await migration(log, storage);
  return setStorageSchemaVersion(storage, destinationVersion);
}

/**
 * Creates a Migration that bumps the rollback limit [[toAtLeast]]
 * the specified version. Leaves the rollback limit unchanged if it
 * is equal or greater.
 */
export function rollbackLimitMigration(toAtLeast: number): Migration {
  return async (log: LogContext, storage: DurableStorage) => {
    const meta = await getStorageSchemaMeta(storage);

    // Sanity check to maintain the invariant that running code is never
    // earlier than the rollback limit.
    assert(toAtLeast <= meta.version + 1);

    if (meta.minSafeRollbackVersion >= toAtLeast) {
      // The rollback limit must never move backwards.
      log.debug?.(
        `rollback limit is already at ${meta.minSafeRollbackVersion}`,
      );
    } else {
      log.info?.(
        `bumping rollback limit from ${meta.minSafeRollbackVersion} to ${toAtLeast}`,
      );
      // Don't [[await]]. Let the put() be atomically flushed with the version update.
      void storage.put(STORAGE_SCHEMA_META_KEY, {
        ...meta,
        minSafeRollbackVersion: toAtLeast,
      });
    }
  };
}
