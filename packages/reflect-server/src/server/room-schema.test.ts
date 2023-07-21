import {describe, expect, test} from '@jest/globals';
import {DurableStorage} from '../storage/durable-storage.js';
import {createSilentLogContext} from '../util/test-utils.js';
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
    {
      name: 'initialize version index at v2',
      preState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 1,
          maxVersion: 1,
          minSafeRollbackVersion: 1,
        },
        ['user/foo']: {version: 123, deleted: false, value: 'bar'},
        ['user/bar']: {version: 150, deleted: true, value: 'baz'},
        ['user/baz']: {version: 170, deleted: false, value: 'foo'},
      },
      postState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 2,
          maxVersion: 2,
          minSafeRollbackVersion: 1,
        },
        ['user/foo']: {version: 123, deleted: false, value: 'bar'},
        ['user/bar']: {version: 150, deleted: true, value: 'baz'},
        ['user/baz']: {version: 170, deleted: false, value: 'foo'},
        ['v/13f/foo']: {},
        ['v/146/bar']: {deleted: true},
        ['v/14q/baz']: {},
      },
    },
    {
      name: 'fix corrupted version index at v2',
      preState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 1,
          maxVersion: 3,
          minSafeRollbackVersion: 1,
        },
        ['user/foo']: {version: 123, deleted: false, value: 'bar'},
        ['user/bar']: {version: 150, deleted: true, value: 'baz'},
        ['user/baz']: {version: 170, deleted: false, value: 'foo'},
        // Incomplete / corrupt version index.
        ['v/02/foo']: {deleted: true},
        ['v/14q/baz']: {},
      },
      postState: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        storage_schema_meta: {
          version: 2,
          maxVersion: 3,
          minSafeRollbackVersion: 1,
        },
        ['user/foo']: {version: 123, deleted: false, value: 'bar'},
        ['user/bar']: {version: 150, deleted: true, value: 'baz'},
        ['user/baz']: {version: 170, deleted: false, value: 'foo'},
        ['v/13f/foo']: {},
        ['v/146/bar']: {deleted: true},
        ['v/14q/baz']: {},
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
