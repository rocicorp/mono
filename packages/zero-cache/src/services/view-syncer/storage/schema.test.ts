import {describe, expect, test} from '@jest/globals';
import type {LogContext} from '@rocicorp/logger';
import * as v from 'shared/src/valita.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {DurableStorage} from './durable-storage.js';
import {
  StorageSchemaMeta,
  VersionMigrationMap,
  initStorageSchema,
  storageSchemaMeta,
} from './schema.js';

describe('storage schema', () => {
  type Case = {
    name: string;
    preSchema?: StorageSchemaMeta;
    migrations: VersionMigrationMap;
    postSchema: StorageSchemaMeta;
    expectedErr?: string;
    expectedMigrationHistory?: string;
  };

  const logMigrationHistory =
    (name: string) => async (_log: LogContext, storage: DurableStorage) => {
      const meta = await storage.get('storage_schema_meta', storageSchemaMeta);
      const history = await storage.get('migration_history', v.string());
      void storage.put(
        'migration_history',
        `${history ?? ''} ${name}-at(${meta?.version})`,
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
        5: logMigrationHistory('second'),
        4: logMigrationHistory('first'),
        7: {minSafeRollbackVersion: 2},
        8: logMigrationHistory('third'),
      },
      expectedMigrationHistory: ' first-at(2) second-at(4) third-at(7)',
      postSchema: {
        version: 8,
        maxVersion: 8,
        minSafeRollbackVersion: 2,
      },
    },
    {
      name: 'initial migration',
      migrations: {1: () => Promise.resolve()},
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
      migrations: {13: () => Promise.resolve()},
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
      migrations: {13: () => Promise.resolve()},
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
      migrations: {8: () => Promise.reject('should not be run')},
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
      migrations: {7: () => Promise.reject('should not be run')},
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
        13: logMigrationHistory('successful'),
        14: () => Promise.reject('fails to get to 14'),
      },
      postSchema: {
        version: 13,
        maxVersion: 13,
        minSafeRollbackVersion: 6,
      },
      expectedMigrationHistory: ' successful-at(12)',
      expectedErr: 'fails to get to 14',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const {runnerDO} = getMiniflareBindings();
      const id = runnerDO.newUniqueId();
      const storage = await getMiniflareDurableObjectStorage(id);

      if (c.preSchema) {
        await storage.put('storage_schema_meta', c.preSchema);
      }

      let err: string | undefined;
      try {
        await initStorageSchema(
          createSilentLogContext(),
          new DurableStorage(storage),
          c.migrations,
        );
      } catch (e) {
        err = String(e);
      }
      expect(err).toBe(c.expectedErr);

      expect(await storage.get('storage_schema_meta')).toEqual(c.postSchema);
      expect(await storage.get('migration_history')).toBe(
        c.expectedMigrationHistory,
      );
    });
  }
});
