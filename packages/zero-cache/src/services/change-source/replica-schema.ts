/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  runSchemaMigrations,
  type IncrementalMigrationMap,
  type Migration,
} from '../../db/migration-lite.ts';
import {AutoResetSignal} from '../change-streamer/schema/tables.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../replicator/schema/replication-state.ts';

export async function initReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
  initialSync: (lc: LogContext, tx: Database) => Promise<void>,
): Promise<void> {
  const setupMigration: Migration = {
    migrateSchema: (log, tx) => initialSync(log, tx),
    minSafeVersion: 1,
  };

  try {
    await runSchemaMigrations(
      log,
      debugName,
      dbPath,
      setupMigration,
      schemaVersionMigrationMap,
    );
  } catch (e) {
    if (e instanceof SqliteError && e.code === 'SQLITE_CORRUPT') {
      throw new AutoResetSignal(e.message);
    }
    throw e;
  }
}

export async function upgradeReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
) {
  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    // setupMigration should never be invoked
    {
      migrateSchema: () => {
        throw new Error(
          'This should only be called for already synced replicas',
        );
      },
    },
    schemaVersionMigrationMap,
  );
}

export const schemaVersionMigrationMap: IncrementalMigrationMap = {
  // There's no incremental migration from v1. Just reset the replica.
  4: {
    migrateSchema: () => {
      throw new AutoResetSignal('upgrading replica to new schema');
    },
    minSafeVersion: 3,
  },

  5: {
    migrateSchema: (_, db) => {
      db.exec(CREATE_RUNTIME_EVENTS_TABLE);
    },
    migrateData: (_, db) => {
      recordEvent(db, 'upgrade');
    },
  },
};
