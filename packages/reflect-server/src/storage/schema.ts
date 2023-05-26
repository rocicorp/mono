import * as v from 'shared/valita.js';
import {assert} from 'shared/asserts.js';
import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from './durable-storage.js';
import type {Storage} from './storage.js';
import {randomID} from '../util/rand.js';

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
 */
export async function initStorageSchema(
  log: LogContext,
  storage: DurableStorage,
  versionMigrationMap: VersionMigrationMap,
): Promise<void> {
  log = log.withContext('initStorageSchema', randomID());
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
          await log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          meta = await migrateStorageSchemaVersion(
            log,
            storage,
            dest,
            migration,
          );
          assert(meta.version === dest);
        }
      }
    }
    assert(meta.version === codeSchemaVersion);
    log.info?.(`Running server at schema v${codeSchemaVersion}`);
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

  // No need to await the put; flush() will take care of it.
  void storage.put(STORAGE_SCHEMA_META_KEY, meta);
  await storage.flush();
  return meta;
}

async function migrateStorageSchemaVersion(
  log: LogContext,
  storage: DurableStorage,
  destinationVersion: number,
  migration: Migration,
): Promise<StorageSchemaMeta> {
  if (typeof migration === 'function') {
    await migration(log, storage);
  } else {
    await ensureRollbackLimit(migration.minSafeRollbackVersion, log, storage);
  }
  return setStorageSchemaVersion(storage, destinationVersion);
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
async function ensureRollbackLimit(
  toAtLeast: number,
  log: LogContext,
  storage: DurableStorage,
): Promise<void> {
  const meta = await getStorageSchemaMeta(storage);

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
    void storage.put(STORAGE_SCHEMA_META_KEY, {
      ...meta,
      minSafeRollbackVersion: toAtLeast,
    });
  }
}
