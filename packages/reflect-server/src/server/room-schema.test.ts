import {describe, expect, test} from '@jest/globals';
import {DurableStorage} from '../storage/durable-storage.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {initRoomSchema} from './room-schema.js';

describe('room schema', () => {
  type Case = {
    name: string;
    preState?: object;
    postState: object;
  };

  const cases: Case[] = [
    {
      name: 'storage schema meta',
      postState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          // Update these as necessary.
          version: 2,
          maxVersion: 2,
          minSafeRollbackVersion: 1,
        },
      },
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

      await initRoomSchema(
        createSilentLogContext(),
        new DurableStorage(storage),
      );

      const storageState = Object.fromEntries((await storage.list()).entries());
      expect(c.postState).toEqual(storageState);
    });
  }
});
