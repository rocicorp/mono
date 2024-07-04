import type {DurableObjectVersionMigrationMap} from '../../../storage/do-schema.js';

export const SCHEMA_MIGRATIONS: DurableObjectVersionMigrationMap = {
  1: {minSafeRollbackVersion: 1}, // The inaugural v1 understands the rollback limit.
};
