import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {resetAllConfig, setConfig} from 'reflect-shared/src/config.js';
import type {ClientID} from 'reflect-shared/src/mod.js';
import {CLOSE_BEACON_PATH} from 'reflect-shared/src/paths.js';
import type {MutatorDefs, WriteTransaction} from 'reflect-shared/src/types.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {newCreateRoomRequest} from '../client/room.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  ClientRecord,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import {putConnectedClients} from '../types/connected-clients.js';
import {UserValue, userValueKey, userValueSchema} from '../types/user-value.js';
import {putVersion} from '../types/version.js';
import {TestLogSink, setUserEntries} from '../util/test-utils.js';
import {createTestDurableObjectState} from './do-test-utils.js';
import {addRoomIDHeader} from './internal-headers.js';
import {BaseRoomDO} from './room-do.js';

async function createRoom<MD extends MutatorDefs>(
  roomDO: BaseRoomDO<MD>,
  roomID: string,
  expectedStatus = 200,
  apiKey = 'API KEY',
) {
  const createRoomRequest = addRoomIDHeader(
    newCreateRoomRequest('http://test.roci.dev/', apiKey, roomID),
    roomID,
  );
  const createResponse = await roomDO.fetch(createRoomRequest);
  expect(createResponse.status).toBe(expectedStatus);
}

const noopHandlers = {
  roomStartHandler: () => Promise.resolve(),
  onClientDisconnect: () => Promise.resolve(),
  onClientDelete: () => Promise.resolve(),
} as const;

async function makeBaseRoomDO(state?: DurableObjectState) {
  const testLogSink = new TestLogSink();
  return new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state: state ?? (await createTestDurableObjectState('test-do-id')),
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });
}

beforeEach(() => {
  setConfig('closeBeacon', true);
});

afterEach(() => {
  resetAllConfig();
});

describe('Close beacon behavior', () => {
  const roomID = 'testRoomID';
  const clientID = 'testClientID';
  const userID = 'testUserID';
  const clientGroupID = 'testClientGroupID';

  const cases: {
    name: string;
    enabled?: boolean;
    expectedStatus: number;
    storedLastMutationID: number;
    body: unknown;
    entries?: Record<string, ReadonlyJSONValue>;
    expectedEntries?: Record<string, UserValue>;
    connectedClients?: ClientID[];
    expectedClientRecords: Record<ClientID, ClientRecord>;
    onClientDelete?: (tx: WriteTransaction) => Promise<void>;
  }[] = [
    {
      name: 'Config not enabled',
      enabled: false,
      expectedStatus: 404,
      storedLastMutationID: 0,
      body: {lastMutationID: 0},
      expectedClientRecords: {
        [clientID]: {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 0,
          lastMutationIDVersion: null,
          lastSeen: 0,
          userID,
        },
      },
    },
    {
      name: 'Same lmid sent',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      expectedClientRecords: {},
    },
    {
      name: 'Same lmid sent remove old presence keys',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      entries: {
        [`-/p/${clientID}`]: 1,
        [`-/p/${clientID}/2`]: 2,
        [`-/p/someOtherClientID`]: 3,
      },
      expectedEntries: {
        'user/-/p/someOtherClientID': {
          deleted: false,
          value: 3,
          version: 100,
        },
        [`user/-/p/${clientID}`]: {
          deleted: true,
          value: 1,
          version: 101,
        },
        [`user/-/p/${clientID}/2`]: {
          deleted: true,
          value: 2,
          version: 101,
        },
      },
      expectedClientRecords: {},
    },
    {
      name: 'sent lmid is less than stored lmid',
      expectedStatus: 500,
      storedLastMutationID: 2,
      body: {lastMutationID: 1},
      expectedClientRecords: {
        [clientID]: {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 2,
          lastMutationIDVersion: null,
          lastSeen: 0,
          userID,
        },
      },
    },
    {
      name: 'sent lmid is greater than stored lmid',
      expectedStatus: 200,
      storedLastMutationID: 3,
      body: {lastMutationID: 4},
      expectedClientRecords: {
        [clientID]: {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 3,
          lastMutationIDVersion: null,
          lastSeen: 0,
          userID,
        },
      },
    },
    {
      name: 'bad body',
      expectedStatus: 400,
      storedLastMutationID: 2,
      body: {invalidBody: true},
      expectedClientRecords: {
        [clientID]: {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 2,
          lastMutationIDVersion: null,
          lastSeen: 0,
          userID,
        },
      },
    },

    {
      name: 'Same lmid sent but still connected so do not remove old presence keys',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      connectedClients: [clientID],
      entries: {
        [`-/p/${clientID}`]: 1,
        [`-/p/${clientID}/2`]: 2,
        [`-/p/someOtherClientID`]: 3,
      },
      expectedEntries: {
        'user/-/p/someOtherClientID': {
          deleted: false,
          value: 3,
          version: 100,
        },
        [`user/-/p/${clientID}`]: {
          deleted: false,
          value: 1,
          version: 100,
        },
        [`user/-/p/${clientID}/2`]: {
          deleted: false,
          value: 2,
          version: 100,
        },
      },
      expectedClientRecords: {
        [clientID]: {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 10,
          lastMutationIDVersion: null,
          lastSeen: 0,
          lastMutationIDAtClose: 10,
          userID,
        },
      },
    },

    {
      name: 'Same lmid sent with a onClientDelete',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      expectedClientRecords: {},
      async onClientDelete(tx) {
        await tx.set('x/hold', 'door');
        await tx.set(`-/p/${tx.clientID}/collect`, 'me');
      },
      entries: {
        [`-/p/${clientID}`]: 1,
        [`-/p/someOtherClientID`]: 2,
      },
      expectedEntries: {
        'user/x/hold': {
          deleted: false,
          value: 'door',
          version: 101,
        },
        [`user/-/p/${clientID}/collect`]: {
          deleted: true,
          value: 'me',
          version: 101,
        },
        [`user/-/p/${clientID}`]: {
          deleted: true,
          value: 1,
          version: 101,
        },
        'user/-/p/someOtherClientID': {
          deleted: false,
          value: 2,
          version: 100,
        },
      },
    },

    {
      name: 'Same lmid sent with a onClientDelete throws',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      expectedClientRecords: {},
      onClientDelete: () =>
        Promise.reject(new Error('onClientDelete intentional error')),
    },
  ];
  for (const c of cases) {
    test(c.name, async () => {
      const {
        enabled = true,
        body,
        entries = {},
        expectedEntries = entries,
        connectedClients,
        expectedClientRecords,
        onClientDelete = () => Promise.resolve(),
      } = c;
      const version = 100;
      setConfig('closeBeacon', enabled);

      const state = await createTestDurableObjectState('test-do-id');
      const storage = new DurableStorage(state.storage);

      const testLogSink = new TestLogSink();
      const roomDO = new BaseRoomDO({
        mutators: {},
        ...noopHandlers,
        onClientDelete,
        state,
        logSink: testLogSink,
        logLevel: 'info',
        allowUnconfirmedWrites: true,
        maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
        env: {foo: 'bar'},
      });

      await createRoom(roomDO, roomID, undefined);

      await setUserEntries(storage, version, entries);

      await putVersion(version, storage);

      if (connectedClients) {
        await putConnectedClients(new Set(connectedClients), storage);
      }

      const clientRecord: ClientRecord = {
        clientGroupID,
        baseCookie: null,
        lastMutationID: c.storedLastMutationID,
        lastMutationIDVersion: null,
        lastSeen: 0,
        userID,
      };
      await putClientRecord(clientID, clientRecord, storage);

      const url = `http://test.roci.dev${CLOSE_BEACON_PATH}?roomID=${roomID}&clientID=${clientID}&userID=${userID}`;
      const request = addRoomIDHeader(
        new Request(url, {method: 'POST', body: JSON.stringify(body)}),
        roomID,
      );
      const response = await roomDO.fetch(request);

      expect(response.status).toBe(c.expectedStatus);

      expect(Object.fromEntries(await listClientRecords(storage))).toEqual(
        expectedClientRecords,
      );
      expect(
        await storage.list({prefix: userValueKey('')}, userValueSchema),
      ).toEqual(new Map(Object.entries(expectedEntries)));
    });
  }
});

describe('Missing search params', () => {
  const roomID = 'testRoomID';
  const clientID = 'testClientID';
  const userID = 'testUserID';
  for (const url of [
    `http://test.roci.dev${CLOSE_BEACON_PATH}?clientID=${clientID}&userID=${userID}`,
    `http://test.roci.dev${CLOSE_BEACON_PATH}?clientID=${clientID}&roomID=${roomID}`,
    `http://test.roci.dev${CLOSE_BEACON_PATH}?userID=${userID}&roomID=${roomID}`,
  ]) {
    test(`url: ${url}`, async () => {
      const roomDO = await makeBaseRoomDO();

      await createRoom(roomDO, roomID, undefined);

      const request = addRoomIDHeader(
        new Request(url, {
          method: 'POST',
          body: JSON.stringify({
            lastMutationID: 0,
          }),
        }),
        roomID,
      );
      const response = await roomDO.fetch(request);

      expect(response.status).toBe(400);
    });
  }
});
