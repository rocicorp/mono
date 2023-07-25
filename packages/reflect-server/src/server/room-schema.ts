import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from '../storage/durable-storage.js';
import {VersionMigrationMap, initStorageSchema} from '../storage/schema.js';
import {backfillVersionIndex} from '../ff/backfill-version-index.js';

const ROOM_VERSION_MIGRATION_MAP: VersionMigrationMap = {
  // The inaugural v1 understands the rollback limit.
  1: {minSafeRollbackVersion: 1},

  // Initialize / fix version index for Fast(er) Forward.
  2: backfillVersionIndex,
};

export async function initRoomSchema(
  log: LogContext,
  storage: DurableStorage,
): Promise<void> {
  await initStorageSchema(log, storage, ROOM_VERSION_MIGRATION_MAP);
}
