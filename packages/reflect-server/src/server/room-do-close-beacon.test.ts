import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {resetAllConfig, setConfig} from 'reflect-shared/src/config.js';
import type {ClientID} from 'reflect-shared/src/mod.js';
import {CLOSE_BEACON_PATH} from 'reflect-shared/src/paths.js';
import type {MutatorDefs} from 'reflect-shared/src/types.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {newCreateRoomRequest} from '../client/room.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  ClientRecord,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
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

async function makeBaseRoomDO(state?: DurableObjectState) {
  const testLogSink = new TestLogSink();
  return new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
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

  const cases: {
    name: string;
    enabled?: boolean;
    expectedStatus: number;
    storedLastMutationID: number;
    body: unknown;
    expectedClientIDs: Iterable<ClientID>;
    entries?: Record<string, ReadonlyJSONValue>;
    expectedEntries?: Record<string, UserValue>;
  }[] = [
    {
      name: 'Config not enabled',
      enabled: false,
      expectedStatus: 404,
      storedLastMutationID: 0,
      body: {lastMutationID: 0},
      expectedClientIDs: [clientID],
    },
    {
      name: 'Same lmid sent',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      expectedClientIDs: [],
    },
    {
      name: 'Same lmid sent remove old presence keys',
      expectedStatus: 200,
      storedLastMutationID: 10,
      body: {lastMutationID: 10},
      expectedClientIDs: [],
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
        'user/-/p/testClientID': {
          deleted: true,
          value: 1,
          version: 101,
        },
        'user/-/p/testClientID/2': {
          deleted: true,
          value: 2,
          version: 101,
        },
      },
    },
    {
      name: 'sent lmid is less than stored lmid',
      expectedStatus: 500,
      storedLastMutationID: 2,
      body: {lastMutationID: 1},
      expectedClientIDs: [clientID],
    },
    {
      name: 'sent lmid is greater than stored lmid',
      expectedStatus: 200,
      storedLastMutationID: 3,
      body: {lastMutationID: 4},
      expectedClientIDs: [clientID],
    },
    {
      name: 'bad body',
      expectedStatus: 400,
      storedLastMutationID: 2,
      body: {invalidBody: true},
      expectedClientIDs: [clientID],
    },
  ];
  for (const c of cases) {
    test(`c.name`, async () => {
      const {
        enabled = true,
        body,
        expectedClientIDs = [],
        entries = {},
        expectedEntries = entries,
      } = c;
      const version = 100;
      setConfig('closeBeacon', enabled);

      const state = await createTestDurableObjectState('test-do-id');
      const storage = new DurableStorage(state.storage);

      const testLogSink = new TestLogSink();
      const roomDO = new BaseRoomDO({
        mutators: {},
        roomStartHandler: () => Promise.resolve(),
        disconnectHandler: () => Promise.resolve(),
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

      const clientRecord: ClientRecord = {
        clientGroupID: 'testClientGroupID',
        baseCookie: null,
        lastMutationID: c.storedLastMutationID,
        lastMutationIDVersion: null,
        lastSeen: 0,
      };
      await putClientRecord(clientID, clientRecord, storage);

      const url = `http://test.roci.dev${CLOSE_BEACON_PATH}?roomID=${roomID}&clientID=${clientID}&userID=${userID}`;
      const request = addRoomIDHeader(
        new Request(url, {method: 'POST', body: JSON.stringify(body)}),
        roomID,
      );
      const response = await roomDO.fetch(request);

      expect(response.status).toBe(c.expectedStatus);

      expect(new Set((await listClientRecords(storage)).keys())).toEqual(
        new Set(expectedClientIDs),
      );
      expect(
        await storage.list({prefix: userValueKey('-/p/')}, userValueSchema),
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
