import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import type {Database as Db} from '../../../zqlite/src/db.js';
import {DbFile} from '../test/lite.js';
import {
  type IncrementalMigrationMap,
  type Migration,
  type VersionHistory,
  getVersionHistory,
  runSchemaMigrations,
} from './migration-lite.js';

describe('db/migration-lite', () => {
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

  const logMigrationHistory = (name: string) => (_log: LogContext, db: Db) => {
    const meta = getVersionHistory(db);
    db.prepare(`INSERT INTO MigrationHistory (event) VALUES (?)`).run(
      `${name}-at(${meta.dataVersion})`,
    );
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
      name: 'initial migration',
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
      name: 'updates max version',
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

  let dbFile: DbFile;
  let db: Db;

  beforeEach(() => {
    dbFile = new DbFile('migration-test');
    db = dbFile.connect(createSilentLogContext());
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.prepare(`CREATE TABLE "MigrationHistory" (event TEXT)`).run();
  });

  afterEach(() => {
    db.close();
    dbFile.delete();
  });

  for (const c of cases) {
    test(c.name, async () => {
      if (c.preSchema) {
        getVersionHistory(db); // Ensures that the table is created.
        db.prepare(
          `
          INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion)
          VALUES (@dataVersion, @schemaVersion, @minSafeVersion)
        `,
        ).run(c.preSchema);
      }

      let err: string | undefined;
      try {
        await runSchemaMigrations(
          createSilentLogContext(),
          debugName,
          dbFile.path,
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

      expect(getVersionHistory(db)).toEqual(c.postSchema);
      expect(db.prepare(`SELECT * FROM "MigrationHistory"`).all()).toEqual(
        c.expectedMigrationHistory ?? [],
      );
    });
  }
});
