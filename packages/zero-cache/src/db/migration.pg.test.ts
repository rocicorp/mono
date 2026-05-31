import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {type PgTest, test} from '../test/db.ts';
import {postgresTypeConfig, type PostgresDB} from '../types/pg.ts';
import {
  type IncrementalMigrationMap,
  type Migration,
  type VersionHistory,
  getVersionHistory,
  runSchemaMigrations,
} from './migration.ts';

describe('db/migration', () => {
  const schemaName = '_zero';
  const debugName = 'debug-name';

  type Case = {
    name: string;
    preSchema?: VersionHistory;
    setup?: Migration;
    migrations: IncrementalMigrationMap;
    postSchema: VersionHistory;
    expectedErr?: string;
    expectedMigrationHistory?: {event: string}[];
  };

  const logMigrationHistory =
    (name: string) => async (_log: LogContext, sql: postgres.Sql) => {
      const meta = await getVersionHistory(sql, schemaName, true);
      await sql`INSERT INTO "MigrationHistory" ${sql({
        event: `${name}-at(${meta?.dataVersion})`,
      })}`;
    };

  const cases: Case[] = [
    {
      name: 'sorts and runs multiple migrations',
      preSchema: {
        dataVersion: 2,
        schemaVersion: 2,
        minSafeVersion: 1,
      },
      migrations: {
        5: {
          migrateSchema: logMigrationHistory('second-schema'),
          migrateData: logMigrationHistory('second-data'),
        },
        4: {migrateSchema: logMigrationHistory('first-schema')},
        7: {minSafeVersion: 2},
        8: {migrateSchema: logMigrationHistory('third-schema')},
      },
      expectedMigrationHistory: [
        {event: 'first-schema-at(2)'},
        {event: 'second-schema-at(4)'},
        {event: 'second-data-at(4)'},
        {event: 'third-schema-at(7)'},
      ],
      postSchema: {
        dataVersion: 8,
        schemaVersion: 8,
        minSafeVersion: 2,
      },
    },
    {
      name: 'initial setup',
      setup: {
        migrateSchema: logMigrationHistory('initial-schema'),
        migrateData: logMigrationHistory('initial-data'),
        minSafeVersion: 1,
      },
      migrations: {
        3: {migrateSchema: () => Promise.reject('should not be called')},
      },
      expectedMigrationHistory: [
        {event: 'initial-schema-at(0)'},
        {event: 'initial-data-at(0)'},
      ],
      postSchema: {
        dataVersion: 3,
        schemaVersion: 3,
        minSafeVersion: 1,
      },
    },
    {
      name: 'updates schema version',
      preSchema: {
        dataVersion: 12,
        schemaVersion: 12,
        minSafeVersion: 6,
      },
      migrations: {13: {migrateData: () => Promise.resolve()}},
      postSchema: {
        dataVersion: 13,
        schemaVersion: 13,
        minSafeVersion: 6,
      },
    },
    {
      name: 'preserves other versions',
      preSchema: {
        dataVersion: 12,
        schemaVersion: 14,
        minSafeVersion: 6,
      },
      migrations: {13: {migrateData: () => Promise.resolve()}},
      postSchema: {
        dataVersion: 13,
        schemaVersion: 14,
        minSafeVersion: 6,
      },
    },
    {
      name: 'rollback to earlier version',
      preSchema: {
        dataVersion: 10,
        schemaVersion: 10,
        minSafeVersion: 8,
      },
      migrations: {8: {migrateData: () => Promise.reject('should not be run')}},
      postSchema: {
        dataVersion: 8,
        schemaVersion: 10,
        minSafeVersion: 8,
      },
    },
    {
      name: 'disallows rollback before rollback limit',
      preSchema: {
        dataVersion: 10,
        schemaVersion: 10,
        minSafeVersion: 8,
      },
      migrations: {7: {migrateData: () => Promise.reject('should not be run')}},
      postSchema: {
        dataVersion: 10,
        schemaVersion: 10,
        minSafeVersion: 8,
      },
      expectedErr: `Error: Cannot run ${debugName} at schema v7 because rollback limit is v8`,
    },
    {
      name: 'bump rollback limit',
      preSchema: {
        dataVersion: 10,
        schemaVersion: 10,
        minSafeVersion: 0,
      },
      migrations: {11: {minSafeVersion: 3}},
      postSchema: {
        dataVersion: 11,
        schemaVersion: 11,
        minSafeVersion: 3,
      },
    },
    {
      name: 'bump rollback limit past current version',
      preSchema: {
        dataVersion: 1,
        schemaVersion: 1,
        minSafeVersion: 0,
      },
      migrations: {11: {minSafeVersion: 11}},
      postSchema: {
        dataVersion: 11,
        schemaVersion: 11,
        minSafeVersion: 11,
      },
    },
    {
      name: 'rollback limit bump does not move backwards',
      preSchema: {
        dataVersion: 10,
        schemaVersion: 10,
        minSafeVersion: 6,
      },
      migrations: {11: {minSafeVersion: 3}},
      postSchema: {
        dataVersion: 11,
        schemaVersion: 11,
        minSafeVersion: 6,
      },
    },
    {
      name: 'failed migration rolls back entire transaction',
      preSchema: {
        dataVersion: 12,
        schemaVersion: 12,
        minSafeVersion: 6,
      },
      migrations: {
        13: {migrateData: logMigrationHistory('successful')},
        14: {migrateData: () => Promise.reject('fails to get to 14')},
      },
      // With single transaction, failed migration rolls back everything
      postSchema: {
        dataVersion: 12,
        schemaVersion: 12,
        minSafeVersion: 6,
      },
      expectedMigrationHistory: [],
      expectedErr: 'fails to get to 14',
    },
  ];

  let db: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    db = await testDBs.create('migration_test');
    await db`CREATE TABLE "MigrationHistory" (event TEXT)`;

    return () => testDBs.drop(db);
  });

  for (const c of cases) {
    test(c.name, async () => {
      if (c.preSchema) {
        await getVersionHistory(db, schemaName, true); // Ensures that the table is created.
        await db`INSERT INTO ${db(schemaName)}."versionHistory" ${db(
          c.preSchema,
        )}`;
      }

      let err: string | undefined;
      try {
        await runSchemaMigrations(
          createSilentLogContext(),
          debugName,
          schemaName,
          db,
          c.setup ?? {
            migrateSchema: () => Promise.reject('not expected to run'),
          },
          c.migrations,
        );
      } catch (e) {
        if (!c.expectedErr) {
          throw e;
        }
        err = String(e);
      }
      expect(err).toBe(c.expectedErr);

      expect(await getVersionHistory(db, schemaName)).toEqual(c.postSchema);
      expect(await db`SELECT * FROM "MigrationHistory"`).toEqual(
        c.expectedMigrationHistory ?? [],
      );
    });
  }

  test<PgTest>('concurrent migrations are serialized by advisory lock', async ({
    testDBs,
  }) => {
    // This test verifies that concurrent calls to runSchemaMigrations
    // are serialized using pg_advisory_lock, preventing race conditions
    // during rolling deployments.
    //
    // We need two separate database connections because advisory locks
    // are session-based and reentrant within the same connection.

    const db1 = await testDBs.create('migration_concurrent_test');
    await db1`CREATE TABLE "MigrationHistory" (event TEXT)`;

    // Create a second connection to the same database
    const {host, port, user: username, pass, database} = db1.options;
    const db2: PostgresDB = postgres({
      host: host[0],
      port: port[0],
      username,
      password: pass ?? undefined,
      database,
      ...postgresTypeConfig(),
    });

    try {
      const migration1AcquiredLock = resolver<void>();
      const migration1CanProceed = resolver<void>();

      const events: string[] = [];

      // Migration 1: signals when it starts its schema migration,
      // then waits for permission to proceed
      const migration1 = runSchemaMigrations(
        createSilentLogContext(),
        'migration-1',
        schemaName,
        db1,
        {
          migrateSchema: async () => {
            events.push('migration1-schema');
            migration1AcquiredLock.resolve();
            await migration1CanProceed.promise;
          },
        },
        {1: {}},
      );

      // Wait for migration 1 to acquire the lock and start its migration
      await migration1AcquiredLock.promise;

      // Migration 2: will block on advisory lock, then see schema is already
      // at version 1 and skip the setup (this is correct behavior)
      const migration2Promise = runSchemaMigrations(
        createSilentLogContext(),
        'migration-2',
        schemaName,
        db2,
        {
          migrateSchema: () => {
            // This should NOT be called because migration 1 already migrated
            events.push('migration2-schema');
            return Promise.resolve();
          },
        },
        {1: {}},
      );

      // Give migration 2 a chance to try to acquire the lock
      await new Promise(r => setTimeout(r, 50));

      // Migration 2 should be blocked on the advisory lock, so only
      // migration 1's schema event should have fired
      expect(events).toEqual(['migration1-schema']);

      // Let migration 1 complete and release the lock
      migration1CanProceed.resolve();
      await migration1;
      events.push('migration1-done');

      // Now migration 2 can proceed (it will skip migrateSchema since
      // the schema is already at version 1)
      await migration2Promise;
      events.push('migration2-done');

      // Verify the migrations completed in order
      expect(events).toEqual([
        'migration1-schema',
        'migration1-done',
        'migration2-done',
      ]);

      // Verify final state is correct
      expect(await getVersionHistory(db1, schemaName)).toEqual({
        dataVersion: 1,
        schemaVersion: 1,
        minSafeVersion: 0,
      });
    } finally {
      await db2.end();
      await testDBs.drop(db1);
    }
  });
});
