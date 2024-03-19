import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {TestDBs} from '../../../test/db.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {
  SyncSchemaMeta,
  VersionMigrationMap,
  getSyncSchemaMeta,
  runSyncSchemaMigrations,
} from './migration.js';

describe('schema/migration', () => {
  type Case = {
    name: string;
    preSchema?: SyncSchemaMeta;
    migrations: VersionMigrationMap;
    postSchema: SyncSchemaMeta;
    expectedErr?: string;
    expectedMigrationHistory?: {event: string}[];
  };

  const logMigrationHistory =
    (name: string) =>
    async (_log: LogContext, _id: string, sql: postgres.Sql) => {
      const meta = await getSyncSchemaMeta(sql);
      await sql`INSERT INTO migration_history ${sql({
        event: `${name}-at(${meta.version})`,
      })}`;
    };

  const cases: Case[] = [
    {
      name: 'sorts and runs multiple migrations',
      preSchema: {
        version: 2,
        maxVersion: 2,
        minSafeRollbackVersion: 1,
      },
      migrations: {
        5: {
          pre: logMigrationHistory('pre-second'),
          run: logMigrationHistory('second'),
        },
        4: {run: logMigrationHistory('first')},
        7: {minSafeRollbackVersion: 2},
        8: {run: logMigrationHistory('third')},
      },
      expectedMigrationHistory: [
        {event: 'first-at(2)'},
        {event: 'pre-second-at(4)'},
        {event: 'second-at(4)'},
        {event: 'third-at(7)'},
      ],
      postSchema: {
        version: 8,
        maxVersion: 8,
        minSafeRollbackVersion: 2,
      },
    },
    {
      name: 'initial migration',
      migrations: {1: {run: () => Promise.resolve()}},
      postSchema: {
        version: 1,
        maxVersion: 1,
        minSafeRollbackVersion: 0,
      },
    },
    {
      name: 'updates max version',
      preSchema: {
        version: 12,
        maxVersion: 12,
        minSafeRollbackVersion: 6,
      },
      migrations: {13: {run: () => Promise.resolve()}},
      postSchema: {
        version: 13,
        maxVersion: 13,
        minSafeRollbackVersion: 6,
      },
    },
    {
      name: 'preserves other versions',
      preSchema: {
        version: 12,
        maxVersion: 14,
        minSafeRollbackVersion: 6,
      },
      migrations: {13: {run: () => Promise.resolve()}},
      postSchema: {
        version: 13,
        maxVersion: 14,
        minSafeRollbackVersion: 6,
      },
    },
    {
      name: 'rollback to earlier version',
      preSchema: {
        version: 10,
        maxVersion: 10,
        minSafeRollbackVersion: 8,
      },
      migrations: {8: {run: () => Promise.reject('should not be run')}},
      postSchema: {
        version: 8,
        maxVersion: 10,
        minSafeRollbackVersion: 8,
      },
    },
    {
      name: 'disallows rollback before rollback limit',
      preSchema: {
        version: 10,
        maxVersion: 10,
        minSafeRollbackVersion: 8,
      },
      migrations: {7: {run: () => Promise.reject('should not be run')}},
      postSchema: {
        version: 10,
        maxVersion: 10,
        minSafeRollbackVersion: 8,
      },
      expectedErr:
        'Error: Cannot run server at schema v7 because rollback limit is v8',
    },
    {
      name: 'bump rollback limit',
      preSchema: {
        version: 10,
        maxVersion: 10,
        minSafeRollbackVersion: 0,
      },
      migrations: {11: {minSafeRollbackVersion: 3}},
      postSchema: {
        version: 11,
        maxVersion: 11,
        minSafeRollbackVersion: 3,
      },
    },
    {
      name: 'rollback limit bump does not move backwards',
      preSchema: {
        version: 10,
        maxVersion: 10,
        minSafeRollbackVersion: 6,
      },
      migrations: {11: {minSafeRollbackVersion: 3}},
      postSchema: {
        version: 11,
        maxVersion: 11,
        minSafeRollbackVersion: 6,
      },
    },
    {
      name: 'only updates version for successful migrations',
      preSchema: {
        version: 12,
        maxVersion: 12,
        minSafeRollbackVersion: 6,
      },
      migrations: {
        13: {run: logMigrationHistory('successful')},
        14: {run: () => Promise.reject('fails to get to 14')},
      },
      postSchema: {
        version: 13,
        maxVersion: 13,
        minSafeRollbackVersion: 6,
      },
      expectedMigrationHistory: [{event: 'successful-at(12)'}],
      expectedErr: 'fails to get to 14',
    },
  ];

  const testDBs = new TestDBs();
  let db: postgres.Sql;

  beforeEach(async () => {
    db = await testDBs.create('migration_test');
    await db`CREATE TABLE migration_history (event TEXT)`;
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  afterAll(async () => {
    await testDBs.end();
  });

  for (const c of cases) {
    test(c.name, async () => {
      if (c.preSchema) {
        await getSyncSchemaMeta(db); // Ensures that the table is created.
        await db`INSERT INTO zero.schema_meta ${db(c.preSchema)}`;
      }

      let err: string | undefined;
      try {
        await runSyncSchemaMigrations(
          createSilentLogContext(),
          'foo-bar-replica-id',
          db,
          'postgres://upstream',
          c.migrations,
        );
      } catch (e) {
        if (!c.expectedErr) {
          throw e;
        }
        err = String(e);
      }
      expect(err).toBe(c.expectedErr);

      expect(await getSyncSchemaMeta(db)).toEqual(c.postSchema);
      expect(await db`SELECT * FROM migration_history`).toEqual(
        c.expectedMigrationHistory ?? [],
      );
    });
  }
});
