import {describe, expect, test} from '@jest/globals';
import {jsonSchema} from 'shared/src/json-schema.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {Storage} from '../storage/storage.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import {putConnectedClients} from '../types/connected-clients.js';
import {putUserValue, userValueKey} from '../types/user-value.js';
import {putVersion} from '../types/version.js';
import {createSilentLogContext} from '../util/test-utils.js';
import {
  collectClients,
  collectOldUserSpaceClientKeys,
  updateLastSeen,
} from './client-gc.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

async function setUserEntries(
  cache: Storage,
  version: number,
  entries: Record<string, ReadonlyJSONValue>,
) {
  for (const [k, value] of Object.entries(entries)) {
    await cache.put(userValueKey(k), {
      value,
      deleted: false,
      version,
    });
  }
}

async function setLastSeenEntries(
  cache: Storage,
  entries: Record<ClientID, number | undefined>,
) {
  for (const [clientID, v] of Object.entries(entries)) {
    await putClientRecord(
      clientID,
      {
        ...(v === undefined ? {} : {lastSeen: v}),
        baseCookie: 1,
        clientGroupID: 'client-group-id',
        lastMutationID: 2,
        lastMutationIDVersion: 3,
      },
      cache,
    );
  }
}

describe('collectOldUserSpaceClientKeys', () => {
  test('normal operations', async () => {
    const lc = createSilentLogContext();

    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const cache = new EntryCache(storage);
    const version = 1;

    //  This test does not care about the lastSeen or the version
    await setUserEntries(cache, version, {
      '-/c/client-a': 1,
      '-/c/client-a/': 2,
      '-/c/client-a/more': 3,
      '-/c/client-b': 4,
      '-/c/client-b/': 5,
      '-/c/client-b/more': 6,
      '-/c/client-c': 7,
      '-/c/client-c/': 8,
      '-/c/client-c/more': 9,
      '-/c/client-d': 10,
      '-/c/client-d/': 11,
      '-/c/client-d/more': 12,
    });
    await cache.flush();

    const clientsToCollect = ['client-a', 'client-c'];
    await collectOldUserSpaceClientKeys(
      lc,
      cache,
      clientsToCollect,
      version + 1,
    );
    await cache.flush();

    const allEntries = await storage.list({}, jsonSchema);
    expect(allEntries).toMatchInlineSnapshot(`
Map {
  "user/-/c/client-a" => {
    "deleted": true,
    "value": 1,
    "version": 2,
  },
  "user/-/c/client-a/" => {
    "deleted": true,
    "value": 2,
    "version": 2,
  },
  "user/-/c/client-a/more" => {
    "deleted": true,
    "value": 3,
    "version": 2,
  },
  "user/-/c/client-b" => {
    "deleted": false,
    "value": 4,
    "version": 1,
  },
  "user/-/c/client-b/" => {
    "deleted": false,
    "value": 5,
    "version": 1,
  },
  "user/-/c/client-b/more" => {
    "deleted": false,
    "value": 6,
    "version": 1,
  },
  "user/-/c/client-c" => {
    "deleted": true,
    "value": 7,
    "version": 2,
  },
  "user/-/c/client-c/" => {
    "deleted": true,
    "value": 8,
    "version": 2,
  },
  "user/-/c/client-c/more" => {
    "deleted": true,
    "value": 9,
    "version": 2,
  },
  "user/-/c/client-d" => {
    "deleted": false,
    "value": 10,
    "version": 1,
  },
  "user/-/c/client-d/" => {
    "deleted": false,
    "value": 11,
    "version": 1,
  },
  "user/-/c/client-d/more" => {
    "deleted": false,
    "value": 12,
    "version": 1,
  },
}
`);
  });

  test('double delete', async () => {
    const lc = createSilentLogContext();

    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const cache = new EntryCache(storage);
    const version = 2;

    //  This test does not care about the lastSeen or the version
    await setUserEntries(cache, version, {
      '-/c/client-a': 1,
      '-/c/client-b': 2,
      '-/c/client-c': 3,
    });
    await putUserValue(
      '-/c/client-a',
      {
        deleted: true,
        value: 1,
        version,
      },
      cache,
    );
    await cache.flush();
    {
      const allEntries = await storage.list({}, jsonSchema);
      expect(allEntries).toMatchInlineSnapshot(`
Map {
  "user/-/c/client-a" => {
    "deleted": true,
    "value": 1,
    "version": 2,
  },
  "user/-/c/client-b" => {
    "deleted": false,
    "value": 2,
    "version": 2,
  },
  "user/-/c/client-c" => {
    "deleted": false,
    "value": 3,
    "version": 2,
  },
}
`);
    }

    const clientsToCollect = ['client-a', 'client-c'];
    await collectOldUserSpaceClientKeys(
      lc,
      cache,
      clientsToCollect,
      version + 1,
    );
    await cache.flush();

    const allEntries = await storage.list({}, jsonSchema);
    expect(allEntries).toMatchInlineSnapshot(`
Map {
  "user/-/c/client-a" => {
    "deleted": true,
    "value": 1,
    "version": 2,
  },
  "user/-/c/client-b" => {
    "deleted": false,
    "value": 2,
    "version": 2,
  },
  "user/-/c/client-c" => {
    "deleted": true,
    "value": 3,
    "version": 3,
  },
}
`);
  });
});

describe('collectClients', () => {
  test('empty storage', async () => {
    const lc = createSilentLogContext();
    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const connectedClients: Set<string> = new Set();
    const now = 123;
    const maxAge = 456;
    const version = 789;

    await putVersion(version, storage);

    await collectClients(
      lc,
      storage,
      connectedClients,
      now,
      maxAge,
      version + 1,
    );

    expect(await storage.list({}, jsonSchema)).toMatchInlineSnapshot(`
Map {
  "version" => 789,
}
`);
  });

  test('normal operation', async () => {
    const lc = createSilentLogContext();
    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const version = 1;
    const now = 4500;
    const maxAge = 2000;
    const connectedClients = new Set(['client-a', 'client-c']);

    await putVersion(version, storage);
    await setUserEntries(storage, version, {
      '-/c/client-a': 1,
      '-/c/client-a/more': 2,
      '-/c/client-b': 3,
      '-/c/client-b/more': 4,
      '-/c/client-c': 5,
      '-/c/client-c/more': 6,
      '-/c/client-d': 7,
      '-/c/client-d/more': 8,
    });
    await setLastSeenEntries(storage, {
      'client-a': 1000,
      'client-b': 2000,
      'client-c': 3000,
      'client-d': 4000,
    });
    await putConnectedClients(connectedClients, storage);
    await storage.flush();

    await collectClients(
      lc,
      storage,
      connectedClients,
      now,
      maxAge,
      version + 1,
    );
    await storage.flush();

    expect(await storage.list({}, jsonSchema)).toMatchInlineSnapshot(`
Map {
  "clientV1/client-a" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 1000,
  },
  "clientV1/client-c" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 3000,
  },
  "clientV1/client-d" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 4000,
  },
  "connectedclients" => [
    "client-a",
    "client-c",
  ],
  "user/-/c/client-a" => {
    "deleted": false,
    "value": 1,
    "version": 1,
  },
  "user/-/c/client-a/more" => {
    "deleted": false,
    "value": 2,
    "version": 1,
  },
  "user/-/c/client-b" => {
    "deleted": true,
    "value": 3,
    "version": 2,
  },
  "user/-/c/client-b/more" => {
    "deleted": true,
    "value": 4,
    "version": 2,
  },
  "user/-/c/client-c" => {
    "deleted": false,
    "value": 5,
    "version": 1,
  },
  "user/-/c/client-c/more" => {
    "deleted": false,
    "value": 6,
    "version": 1,
  },
  "user/-/c/client-d" => {
    "deleted": false,
    "value": 7,
    "version": 1,
  },
  "user/-/c/client-d/more" => {
    "deleted": false,
    "value": 8,
    "version": 1,
  },
  "version" => 2,
}
`);
  });

  test('no client key space used', async () => {
    const lc = createSilentLogContext();
    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const version = 1;
    const now = 4500;
    const maxAge = 2000;
    const connectedClients = new Set(['client-a', 'client-c']);

    await putVersion(version, storage);
    await setLastSeenEntries(storage, {
      'client-a': 1000,
      'client-b': 2000,
      'client-c': 3000,
      'client-d': 4000,
    });
    await putConnectedClients(connectedClients, storage);
    await storage.flush();

    await collectClients(
      lc,
      storage,
      connectedClients,
      now,
      maxAge,
      version + 1,
    );

    expect(await storage.list({}, jsonSchema)).toMatchInlineSnapshot(`
Map {
  "clientV1/client-a" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 1000,
  },
  "clientV1/client-c" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 3000,
  },
  "clientV1/client-d" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 4000,
  },
  "connectedclients" => [
    "client-a",
    "client-c",
  ],
  "version" => 2,
}
`);
  });

  test('client record missing lastSeen', async () => {
    const lc = createSilentLogContext();
    const durable = await getMiniflareDurableObjectStorage(id);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    const version = 1;
    const now = 4500;
    const maxAge = 2000;
    const connectedClients = new Set(['client-a', 'client-c']);

    await putVersion(version, storage);
    await setLastSeenEntries(storage, {
      'client-a': 1000,
      'client-b': undefined,
      'client-c': 3000,
      'client-d': 1500, // different from previous test, so it gets collected
    });
    await setUserEntries(storage, version, {
      '-/c/client-a': 1,
      '-/c/client-a/more': 2,
      '-/c/client-b': 3,
      '-/c/client-b/more': 4,
      '-/c/client-c': 5,
      '-/c/client-c/more': 6,
      '-/c/client-d': 7,
      '-/c/client-d/more': 8,
    });
    await putConnectedClients(connectedClients, storage);
    await storage.flush();

    // no lastSeen
    expect(await getClientRecord('client-b', storage)).toEqual({
      baseCookie: 1,
      clientGroupID: 'client-group-id',
      lastMutationID: 2,
      lastMutationIDVersion: 3,
    });

    // client-b does not get deleted because it has no lastSeen so it just now
    // gets a lastSeen value.
    await collectClients(
      lc,
      storage,
      connectedClients,
      now,
      maxAge,
      version + 1,
    );
    await storage.flush();

    // client-b gets a lastSeen of now
    expect(await storage.list({}, jsonSchema)).toMatchInlineSnapshot(`
Map {
  "clientV1/client-a" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 1000,
  },
  "clientV1/client-b" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 4500,
  },
  "clientV1/client-c" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 3000,
  },
  "connectedclients" => [
    "client-a",
    "client-c",
  ],
  "user/-/c/client-a" => {
    "deleted": false,
    "value": 1,
    "version": 1,
  },
  "user/-/c/client-a/more" => {
    "deleted": false,
    "value": 2,
    "version": 1,
  },
  "user/-/c/client-b" => {
    "deleted": false,
    "value": 3,
    "version": 1,
  },
  "user/-/c/client-b/more" => {
    "deleted": false,
    "value": 4,
    "version": 1,
  },
  "user/-/c/client-c" => {
    "deleted": false,
    "value": 5,
    "version": 1,
  },
  "user/-/c/client-c/more" => {
    "deleted": false,
    "value": 6,
    "version": 1,
  },
  "user/-/c/client-d" => {
    "deleted": true,
    "value": 7,
    "version": 2,
  },
  "user/-/c/client-d/more" => {
    "deleted": true,
    "value": 8,
    "version": 2,
  },
  "version" => 2,
}
`);
  });
});

test('touchClients', async () => {
  const durable = await getMiniflareDurableObjectStorage(id);
  await durable.deleteAll();
  const storage = new DurableStorage(durable);
  const landed = 1969;

  for (const c of 'abcd') {
    await putClientRecord(
      `client-${c}`,
      {
        baseCookie: 1,
        clientGroupID: 'client-group-id',
        lastMutationID: 2,
        lastMutationIDVersion: 3,
      },
      storage,
    );
  }

  await updateLastSeen(
    new Set(['client-a', 'client-b']),
    new Set(['client-b', 'client-c']),
    storage,
    landed,
  );
  await storage.flush();

  expect(await storage.list({}, jsonSchema)).toMatchInlineSnapshot(`
Map {
  "clientV1/client-a" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
    "lastSeen": 1969,
  },
  "clientV1/client-b" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
  },
  "clientV1/client-c" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
  },
  "clientV1/client-d" => {
    "baseCookie": 1,
    "clientGroupID": "client-group-id",
    "lastMutationID": 2,
    "lastMutationIDVersion": 3,
  },
}
`);
});
