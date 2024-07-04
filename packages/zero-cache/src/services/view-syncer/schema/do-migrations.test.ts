import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {describe, expect, test} from 'vitest';
import {initDurableObjectStorageSchema} from '../../../storage/do-schema.js';
import {DurableStorage} from '../../../storage/durable-storage.js';
import {runWithDurableObjectStorage} from '../../../test/do.js';
import {SCHEMA_MIGRATIONS} from './do-migrations.js';
import {schemaRoot} from './paths.js';

describe('view-syncer/migrations', () => {
  type Case = {
    name: string;
    preState?: object;
    postState: object;
  };

  const cases: Case[] = [
    {
      name: 'storage schema meta',
      postState: {
        ['/vs/storage_schema_meta']: {
          // Update these as necessary.
          version: 1,
          maxVersion: 1,
          minSafeRollbackVersion: 1,
        },
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await runWithDurableObjectStorage(async storage => {
        for (const [key, value] of Object.entries(c.preState ?? {})) {
          await storage.put(key, value);
        }

        await initDurableObjectStorageSchema(
          createSilentLogContext(),
          'view-syncer',
          new DurableStorage(storage),
          schemaRoot,
          SCHEMA_MIGRATIONS,
        );

        const storageState = Object.fromEntries(
          (await storage.list({})).entries(),
        );
        expect(c.postState).toEqual(storageState);
      });
    });
  }
});
