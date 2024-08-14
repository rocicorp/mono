import type {LogContext} from '@rocicorp/logger';
import type {Database as Db} from 'better-sqlite3';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile} from '../test/lite.js';
import {
  SchemaVersions,
  VersionMigrationMap,
  getSchemaVersions,
  runSchemaMigrations,
} from './migration-lite.js';

describe('db/migration-lite', () => {
  const debugName = 'debug-name';

  type Case = {
    name: string;
    preSchema?: SchemaVersions;
    migrations: VersionMigrationMap;
    postSchema: SchemaVersions;
    expectedErr?: string;
    expectedMigrationHistory?: {event: string}[];
  };

  const logMigrationHistory = (name: string) => (_log: LogContext, db: Db) => {
    const meta = getSchemaVersions(db);
    db.prepare(`INSERT INTO MigrationHistory (event) VALUES (?)`).run(
      `${name}-at(${meta.version})`,
    );
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
      expectedErr: `Error: Cannot run ${debugName} at schema v7 because rollback limit is v8`,
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

  let dbFile: DbFile;
  let db: Db;

  beforeEach(() => {
    dbFile = new DbFile('migration-test');
    db = dbFile.connect();
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.prepare(`CREATE TABLE "MigrationHistory" (event TEXT)`).run();
  });

  afterEach(async () => {
    db.close();
    await dbFile.unlink();
  });

  for (const c of cases) {
    test(c.name, async () => {
      if (c.preSchema) {
        getSchemaVersions(db); // Ensures that the table is created.
        db.prepare(
          `
          INSERT INTO "_zero.SchemaVersions" (version, maxVersion, minSafeRollbackVersion)
          VALUES (@version, @maxVersion, @minSafeRollbackVersion)
        `,
        ).run(c.preSchema);
      }

      let err: string | undefined;
      try {
        await runSchemaMigrations(
          createSilentLogContext(),
          debugName,
          dbFile.path,
          c.migrations,
        );
      } catch (e) {
        if (!c.expectedErr) {
          throw e;
        }
        err = String(e);
      }
      expect(err).toBe(c.expectedErr);

      expect(getSchemaVersions(db)).toEqual(c.postSchema);
      expect(db.prepare(`SELECT * FROM "MigrationHistory"`).all()).toEqual(
        c.expectedMigrationHistory ?? [],
      );
    });
  }
});
