import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import * as v from 'shared/src/valita.js';
import type {DurableStorage} from './durable-storage.js';
import type {Storage} from './storage.js';

/**
 * Encapsulates the logic for upgrading to a new schema. After the
 * Migration code successfully completes, {@link initStorageSchema}
 * will update the storage version and flush() all mutations.
 *
 * Note that this means that a Migration need not flush manually,
 * as its changes will be flushed atomically with the version update.
 *
 * However, Migrations are free to flush mutations if needed. For example,
 * this may be necessary for large migrations that must be flushed
 * incrementally in order to avoid exceeding memory or cpu limits.
 *
 * @see https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#in-memory-state
 */
export type Migration =
  | ((log: LogContext, storage: DurableStorage) => Promise<void>)
  // A special Migration type that pushes the rollback limit forward.
  | {minSafeRollbackVersion: number};

/** Mapping from schema version to their respective migrations. */
export type VersionMigrationMap = {
  [destinationVersion: number]: Migration;
};

/**
 * Ensures that the storage schema is compatible with the current code,
 * updating and migrating the schema if necessary.
 *
 * @param schemaRoot The root of the storage schema, with no trailing slash.
 *        The storage schema meta information will be stored at
 *        `${schemaRoot}/storage_schema_meta`.
 */
export async function initStorageSchema(
  log: LogContext,
  service: string,
  storage: DurableStorage,
  schemaRoot: string,
  versionMigrationMap: VersionMigrationMap,
): Promise<void> {
  try {
    const versionMigrations = sorted(versionMigrationMap);
    if (versionMigrations.length === 0) {
      log.info?.(`No versions/migrations to manage.`);
      return;
    }
    const codeSchemaVersion =
      versionMigrations[versionMigrations.length - 1][0];
    log.info?.(
      `Checking schema for compatibility with ${service} at schema v${codeSchemaVersion}`,
    );

    let meta = await getStorageSchemaMeta(storage, schemaRoot);
    if (codeSchemaVersion < meta.minSafeRollbackVersion) {
      throw new Error(
        `Cannot run ${service} at schema v${codeSchemaVersion} because rollback limit is v${meta.minSafeRollbackVersion}`,
      );
    }

    if (meta.version > codeSchemaVersion) {
      log.info?.(
        `Storage is at v${meta.version}. Resetting to v${codeSchemaVersion}`,
      );
      meta = await setStorageSchemaVersion(
        storage,
        schemaRoot,
        codeSchemaVersion,
      );
    } else {
      for (const [dest, migration] of versionMigrations) {
        if (meta.version < dest) {
          log.info?.(`Migrating storage from v${meta.version} to v${dest}`);
          await log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          meta = await migrateStorageSchemaVersion(
            log,
            storage,
            schemaRoot,
            dest,
            migration,
          );
          assert(meta.version === dest);
        }
      }
    }
    assert(meta.version === codeSchemaVersion);
    log.info?.(`Running ${service} at schema v${codeSchemaVersion}`);
  } catch (e) {
    log.error?.('Error in ensureStorageSchemaMigrated', e);
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
  schemaRoot: string,
): Promise<StorageSchemaMeta> {
  return (
    (await storage.get(
      `${schemaRoot}/${STORAGE_SCHEMA_META_KEY}`,
      storageSchemaMeta,
    )) ?? {
      version: 0,
      maxVersion: 0,
      minSafeRollbackVersion: 0,
    }
  );
}

async function setStorageSchemaVersion(
  storage: DurableStorage,
  schemaRoot: string,
  newVersion: number,
): Promise<StorageSchemaMeta> {
  const meta = await getStorageSchemaMeta(storage, schemaRoot);
  meta.version = newVersion;
  meta.maxVersion = Math.max(newVersion, meta.maxVersion);

  // No need to await the put; flush() will take care of it.
  void storage.put(`${schemaRoot}/${STORAGE_SCHEMA_META_KEY}`, meta);
  await storage.flush();
  return meta;
}

async function migrateStorageSchemaVersion(
  log: LogContext,
  storage: DurableStorage,
  schemaRoot: string,
  destinationVersion: number,
  migration: Migration,
): Promise<StorageSchemaMeta> {
  if (typeof migration === 'function') {
    await migration(log, storage);
  } else {
    await ensureRollbackLimit(
      migration.minSafeRollbackVersion,
      log,
      storage,
      schemaRoot,
    );
  }
  return setStorageSchemaVersion(storage, schemaRoot, destinationVersion);
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
async function ensureRollbackLimit(
  toAtLeast: number,
  log: LogContext,
  storage: DurableStorage,
  schemaRoot: string,
): Promise<void> {
  const meta = await getStorageSchemaMeta(storage, schemaRoot);

  // Sanity check to maintain the invariant that running code is never
  // earlier than the rollback limit.
  assert(toAtLeast <= meta.version + 1);

  if (meta.minSafeRollbackVersion >= toAtLeast) {
    // The rollback limit must never move backwards.
    log.debug?.(
      `rollback limit is already at ${meta.minSafeRollbackVersion}, don't need to bump to ${toAtLeast}`,
    );
  } else {
    log.info?.(
      `bumping rollback limit from ${meta.minSafeRollbackVersion} to ${toAtLeast}`,
    );
    // Don't [[await]]. Let the put() be atomically flushed with the version update.
    void storage.put(`${schemaRoot}/${STORAGE_SCHEMA_META_KEY}`, {
      ...meta,
      minSafeRollbackVersion: toAtLeast,
    });
  }
}
