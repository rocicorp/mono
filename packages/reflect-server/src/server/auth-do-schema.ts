import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from '../storage/durable-storage.js';
import {initStorageSchema, VersionMigrationMap} from '../storage/schema.js';

const VERSION_MIGRATION_MAP: VersionMigrationMap = {
  // The inaugural v1 understands the rollback limit.
  0: {minSafeRollbackVersion: 0},
};

export async function initAuthDOSchema(
  log: LogContext,
  storage: DurableStorage,
): Promise<void> {
  await initStorageSchema(log, storage, VERSION_MIGRATION_MAP);
}
