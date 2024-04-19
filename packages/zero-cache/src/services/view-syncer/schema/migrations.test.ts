import {describe, expect, test} from 'vitest';
import {DurableStorage} from '../../../storage/durable-storage.js';
import {initStorageSchema} from '../../../storage/schema.js';
import {runWithDurableObjectStorage} from '../../../test/do.js';
import {createSilentLogContext} from '../../../test/logger.js';
import {SCHEMA_MIGRATIONS} from './migrations.js';
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

        await initStorageSchema(
          createSilentLogContext(),
          new DurableStorage(storage),
          schemaRoot,
          SCHEMA_MIGRATIONS,
        );

        const storageState = Object.fromEntries(
          (await storage.list()).entries(),
        );
        expect(c.postState).toEqual(storageState);
      });
    });
  }
});
