import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from '../storage/durable-storage.js';
import {VersionMigrationMap, initStorageSchema} from '../storage/schema.js';

const ROOM_VERSION_MIGRATION_MAP: VersionMigrationMap = {
  1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
  2: () => Promise.resolve(), // Do not use v2 (was used for an obsolete migration)
};

export async function initRoomSchema(
  log: LogContext,
  storage: DurableStorage,
): Promise<void> {
  await initStorageSchema(log, storage, ROOM_VERSION_MIGRATION_MAP);
}
