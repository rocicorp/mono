import type {VersionMigrationMap} from '../../../storage/schema.js';

export const SCHEMA_MIGRATIONS: VersionMigrationMap = {
  1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
};
