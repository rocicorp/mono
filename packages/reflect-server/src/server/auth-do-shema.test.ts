import {describe, expect, test} from '@jest/globals';
import {DurableStorage} from '../storage/durable-storage.js';
import {createSilentLogContext} from '../util/test-utils.js';
import {initAuthDOSchema} from './auth-do-schema.js';

describe('auth do schema migration', () => {
  type Case = {
    name: string;
    // default is {}
    preState?: object;
    // by default asserts postState is same as preState
    postState?: object;
    errorMsg?: string;
  };

  const cases: Case[] = [
    {
      name: 'migrate from none to v0',
      preState: {},
    },
    {
      name: 'migrate from v0 to v0',
      preState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          // Update these as necessary.
          version: 0,
          maxVersion: 0,
          minSafeRollbackVersion: 0,
        },
      },
    },
    {
      name: 'migrate from v1 with minSafeRollbackVersion 0 to v0',
      preState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 1,
          maxVersion: 1,
          minSafeRollbackVersion: 0,
        },
      },
      postState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 0,
          maxVersion: 1,
          minSafeRollbackVersion: 0,
        },
      },
    },
    {
      name: 'migrate from v1 with minSafeRollbackVersion 1 to v0',
      preState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 1,
          maxVersion: 1,
          minSafeRollbackVersion: 1,
        },
      },
      errorMsg: 'Cannot run server at schema v0 because rollback limit is v1',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const {roomDO} = getMiniflareBindings();
      const id = roomDO.newUniqueId();
      const storage = await getMiniflareDurableObjectStorage(id);

      for (const [key, value] of Object.entries(c.preState ?? {})) {
        await storage.put(key, value);
      }

      let caughtErrMessage: string | undefined;
      try {
        await initAuthDOSchema(
          createSilentLogContext(),
          new DurableStorage(storage),
        );
      } catch (e) {
        caughtErrMessage = (e as Error).message;
      }

      expect(c.errorMsg).toEqual(caughtErrMessage);

      const postState = c.postState ?? c.preState ?? {};
      const storageState = Object.fromEntries((await storage.list()).entries());
      expect(postState).toEqual(storageState);
    });
  }
});
