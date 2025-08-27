import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {type PgTest, test} from '../test/db.ts';
import type {PostgresDB} from '../types/pg.ts';
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
      name: 'only updates version for successful migrations',
      preSchema: {
        dataVersion: 12,
        schemaVersion: 12,
        minSafeVersion: 6,
      },
      migrations: {
        13: {migrateData: logMigrationHistory('successful')},
        14: {migrateData: () => Promise.reject('fails to get to 14')},
      },
      postSchema: {
        dataVersion: 13,
        schemaVersion: 13,
        minSafeVersion: 6,
      },
      expectedMigrationHistory: [{event: 'successful-at(12)'}],
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
});
