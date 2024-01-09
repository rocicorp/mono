/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import type {TailErrorMessage} from 'reflect-protocol/src/tail.js';
import {resetAllConfig, setConfig} from 'reflect-shared/src/config.js';
import {API_KEY_HEADER_NAME, createAPIHeaders} from 'shared/src/api/headers.js';
import type {APIErrorInfo} from 'shared/src/api/responses.js';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {must} from 'shared/src/must.js';
import {
  newCloseRoomRequest,
  newCreateRoomRequest,
  newDeleteRoomRequest,
  newGetRoomRequest,
} from '../client/room.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {encodeHeaderValue} from '../util/headers.js';
import {sleep} from '../util/sleep.js';
import {Mocket, TestLogSink, mockWebSocketPair} from '../util/test-utils.js';
import {TestAuthDO} from './auth-do-test-util.js';
import {
  ALARM_INTERVAL,
  AUTH_HANDLER_TIMEOUT_MS,
  BaseAuthDO,
  recordConnection,
} from './auth-do.js';
import type {AuthHandler} from './auth.js';
import {
  TestDurableObjectId,
  TestDurableObjectState,
  TestDurableObjectStub,
  TestExecutionContext,
  createTestDurableObjectNamespace,
} from './do-test-utils.js';
import {upgradeWebsocketResponse} from './http-util.js';
import {
  AUTH_DATA_HEADER_NAME,
  ROOM_ID_HEADER_NAME,
} from './internal-headers.js';
import {
  AUTH_CONNECTIONS_PATH,
  CLOSE_ROOM_PATH,
  CREATE_ROOM_PATH,
  DELETE_ROOM_PATH,
  GET_ROOM_PATH,
  INVALIDATE_ALL_CONNECTIONS_PATH,
  INVALIDATE_ROOM_CONNECTIONS_PATH,
  INVALIDATE_USER_CONNECTIONS_PATH,
  LIST_ROOMS_PATH,
  TAIL_URL_PATH,
  fmtPath,
} from './paths.js';
import {
  RoomStatus,
  roomRecordByObjectIDForTest as getRoomRecordByObjectIDOriginal,
  roomRecordByRoomID as getRoomRecordOriginal,
  type RoomRecord,
} from './rooms.js';
import {createWorker} from './worker.js';

const TEST_API_KEY = 'TEST_REFLECT_API_KEY_TEST';
const {authDO} = getMiniflareBindings();
const authDOID = authDO.idFromName('auth');
let storage: DurableObjectStorage;
let state: TestDurableObjectState;

beforeEach(async () => {
  storage = await getMiniflareDurableObjectStorage(authDOID);
  await storage.deleteAll();
  state = new TestDurableObjectState(authDOID, storage);
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
  resetAllConfig();
});

function isInvalidateRequest(request: Request) {
  return request.url.indexOf('/api/v1/connections') !== -1;
}

function isAuthRequest(request: Request) {
  return request.url.indexOf('/api/auth/') !== -1;
}

async function recordConnectionHelper(
  userID: string,
  roomID: string,
  clientID: string,
) {
  await recordConnection(
    {
      userID,
      roomID,
      clientID,
    },
    new DurableStorage(storage, false),
    {
      connectTimestamp: 1000,
    },
  );
}

async function storeTestConnectionState() {
  await recordConnectionHelper('testUserID1', 'testRoomID1', 'testClientID1');
  await recordConnectionHelper('testUserID1', 'testRoomID1', 'testClientID2');
  await recordConnectionHelper('testUserID1', 'testRoomID2', 'testClientID3');
  await recordConnectionHelper('testUserID2', 'testRoomID1', 'testClientID4');
  await recordConnectionHelper('testUserID2', 'testRoomID3', 'testClientID5');
  await recordConnectionHelper('testUserID3', 'testRoomID3', 'testClientID6');
}

async function expectSuccessfulAPIResponse(
  response: Response,
  result: ReadonlyJSONValue = {},
) {
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    result,
    error: null,
  });
  expect(response.headers.get('content-type')).toBe('application/json');
}

async function expectAPIErrorResponse(response: Response, error: APIErrorInfo) {
  expect(response.status).toBe(error.code);
  expect(await response.json()).toEqual({
    result: null,
    error,
  });
  expect(response.headers.get('content-type')).toBe('application/json');
}

function createCreateRoomTestFixture({
  testRoomID = 'testRoomID1',
}: {testRoomID?: string | undefined} = {}) {
  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );

  const roomDOcreateRoomCounts = new Map<
    string, // objectIDString
    number
  >();
  let roomNum = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: () => {
      throw 'should not be called';
    },
    newUniqueId: () => new TestDurableObjectId('unique-room-do-' + roomNum++),
    get: (id: DurableObjectId) => {
      const objectIDString = id.toString();

      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(
          encodeHeaderValue(testRoomID),
        );
        if (new URLPattern({pathname: CREATE_ROOM_PATH}).test(request.url)) {
          const count = roomDOcreateRoomCounts.get(objectIDString) || 0;
          roomDOcreateRoomCounts.set(objectIDString, count + 1);
          return new Response();
        }
        return new Response('', {status: 200});
      });
    },
  };

  return {
    testRoomID,
    testRequest,
    testRoomDO,
    state,
    roomDOcreateRoomCounts,
  };
}

test("createRoom creates a room and doesn't allow it to be re-created", async () => {
  const {testRoomID, testRequest, testRoomDO, state, roomDOcreateRoomCounts} =
    createCreateRoomTestFixture();
  const testRequest2 = testRequest.clone();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  // Create the room for the first time.
  const response = await authDO.fetch(testRequest);

  expect(roomDOcreateRoomCounts.size).toEqual(1);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr).not.toBeUndefined();
  const roomRecord = rr as RoomRecord;
  expect(roomRecord.objectIDString).toEqual('unique-room-do-0');
  expect(roomDOcreateRoomCounts.get(roomRecord.objectIDString)).toEqual(1);
  await expectSuccessfulAPIResponse(response);

  // Attempt to create the room again.
  const response2 = await authDO.fetch(testRequest2);
  await expectAPIErrorResponse(response2, {
    code: 409,
    resource: 'rooms',
    message: 'Room "testRoomID1" already exists',
  });
  expect(roomDOcreateRoomCounts.size).toEqual(1);
});

test('createRoom allows slashes in roomIDs', async () => {
  const testRoomID = '/';
  const {testRoomDO, state} = createCreateRoomTestFixture({
    testRoomID,
  });

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  let response = await authDO.fetch(testRequest);
  await expectSuccessfulAPIResponse(response);

  response = await authDO.fetch(newRoomRecordsRequest());
  expect(await response.json()).toEqual({
    error: null,
    result: {
      results: [
        {
          jurisdiction: '',
          roomID: testRoomID,
          status: 'open',
        },
      ],
      numResults: 1,
      more: false,
    },
  });

  response = await authDO.fetch(
    newGetRoomRequest('https://teset.roci.dev/', TEST_API_KEY, '/'),
  );
  expect(await response.json()).toEqual({
    error: null,
    result: {
      roomID: '/',
      jurisdiction: '',
      status: 'open',
    },
  });
});

test('createRoom requires roomIDs to not contain weird characters', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const roomIDs = ['', ' ', testRoomID + '!', '$', ' foo ', '🤷'];
  for (const roomID of roomIDs) {
    const testRequest = newCreateRoomRequest(
      'https://test.roci.dev',
      TEST_API_KEY,
      roomID,
    );
    const response = await authDO.fetch(testRequest);
    if (roomID.length === 0) {
      await expectAPIErrorResponse(response, {
        code: 404,
        resource: 'request',
        message: `Unknown or invalid URL`,
      });
    } else {
      await expectAPIErrorResponse(response, {
        code: 400,
        resource: 'rooms',
        message: `Invalid roomID "${roomID}" (must match /^[A-Za-z0-9_\\-/]+$/)`,
      });
    }
  }
});

// Tiny wrappers that hide the conversion from raw DO storage to DurableStorage.
function getRoomRecord(storage: DurableObjectStorage, roomID: string) {
  return getRoomRecordOriginal(new DurableStorage(storage, false), roomID);
}

function getRoomRecordByObjectID(
  storage: DurableObjectStorage,
  objectID: DurableObjectId,
) {
  return getRoomRecordByObjectIDOriginal(
    new DurableStorage(storage, false),
    objectID,
  );
}

test('createRoom returns 500 if roomDO createRoom fails', async () => {
  const {testRoomID, testRequest, testRoomDO, state} =
    createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  // Override the roomDO to return a 500.
  testRoomDO.get = (id: DurableObjectId) =>
    // eslint-disable-next-line require-await
    new TestDurableObjectStub(id, async () => new Response('', {status: 500}));

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(500);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr).toBeUndefined();
});

test('createRoom sets jurisdiction if requested', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
    'eu',
  );

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  let gotJurisdiction = false;
  testRoomDO.newUniqueId = (
    options: DurableObjectNamespaceNewUniqueIdOptions,
  ) => {
    if (options?.jurisdiction === 'eu') {
      gotJurisdiction = true;
    }
    return new TestDurableObjectId('unique-room-do-0');
  };

  const response = await authDO.fetch(testRequest);
  await expectSuccessfulAPIResponse(response);
  expect(gotJurisdiction).toEqual(true);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr?.jurisdiction).toEqual('eu');
});

test('400 bad body requests', async () => {
  const {testRoomDO, state} = createCreateRoomTestFixture();
  const badCreateRoomRequest = createBadBodyRequest(
    fmtPath(CREATE_ROOM_PATH, {roomID: 'foo'}),
    JSON.stringify({badJurisdiction: 'foo'}),
  );

  const requests = [badCreateRoomRequest];

  for (const request of requests) {
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: () => Promise.reject('should not be called'),
      logSink: new TestLogSink(),
      logLevel: 'debug',
      env: {foo: 'bar'},
    });
    const response = await authDO.fetch(request);
    expect(response.status).toEqual(400);
  }
});

function createBadBodyRequest(path: string, body: BodyInit | null): Request {
  const url = new URL(path, 'https://roci.dev');
  return new Request(url.toString(), {
    method: 'post',
    headers: createAPIHeaders(TEST_API_KEY),
    body,
  });
}

test('closeRoom closes an open room', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  await expectSuccessfulAPIResponse(closeRoomResponse);

  const getRoomRequest = newGetRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const getRoomResponse = await authDO.fetch(getRoomRequest);
  await expectSuccessfulAPIResponse(getRoomResponse, {
    roomID: testRoomID,
    jurisdiction: '',
    status: RoomStatus.Closed,
  });
});

test('closeRoom 404s on non-existent room', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  // Note: no createRoom.

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  await expectAPIErrorResponse(closeRoomResponse, {
    code: 404,
    resource: 'rooms',
    message: 'Room "testRoomID1" not found',
  });
});

test('calling closeRoom on closed room is ok', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  await expectSuccessfulAPIResponse(closeRoomResponse);

  const closeRoomResponse2 = await authDO.fetch(closeRoomRequest);
  await expectSuccessfulAPIResponse(closeRoomResponse2);
});

test('deleteRoom calls into roomDO and marks room deleted', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const deleteRoomPathWithRoomID = fmtPath(DELETE_ROOM_PATH, {
    roomID: testRoomID,
  });

  let gotDeleteForObjectIDString;
  testRoomDO.get = (id: DurableObjectId) =>
    // eslint-disable-next-line require-await
    new TestDurableObjectStub(id, async (request: Request) => {
      const url = new URL(request.url);
      if (url.pathname === deleteRoomPathWithRoomID) {
        gotDeleteForObjectIDString = id.toString();
        return new Response();
      }
      return new Response('', {status: 200});
    });

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  await expectSuccessfulAPIResponse(closeRoomResponse);

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const deleteRoomResponse = await authDO.fetch(deleteRoomRequest);
  await expectSuccessfulAPIResponse(deleteRoomResponse);
  expect(gotDeleteForObjectIDString).not.toBeUndefined();
  expect(gotDeleteForObjectIDString).toEqual('unique-room-do-0');

  const getRoomRequest = newGetRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const getRoomResponse = await authDO.fetch(getRoomRequest);
  await expectSuccessfulAPIResponse(getRoomResponse, {
    roomID: testRoomID,
    jurisdiction: '',
    status: RoomStatus.Deleted,
  });
});

test('deleteRoom requires room to be closed', async () => {
  const {testRoomID, testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const deleteRoomResponse = await authDO.fetch(deleteRoomRequest);
  await expectAPIErrorResponse(deleteRoomResponse, {
    code: 409,
    resource: 'rooms',
    message: 'Room "testRoomID1" must first be closed',
  });

  const getRoomRequest = newGetRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const getRoomResponse = await authDO.fetch(getRoomRequest);
  await expectSuccessfulAPIResponse(getRoomResponse, {
    roomID: testRoomID,
    jurisdiction: '',
    status: RoomStatus.Open,
  });
});

test('get room that exists', async () => {
  const {testRoomID, testRequest, testRoomDO, state} =
    createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const response = await authDO.fetch(testRequest);
  await expectSuccessfulAPIResponse(response);

  const getRoomRequest = newGetRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const getRoomResponse = await authDO.fetch(getRoomRequest);
  await expectSuccessfulAPIResponse(getRoomResponse, {
    roomID: testRoomID,
    jurisdiction: '',
    status: RoomStatus.Open,
  });
});

test('get room that does not exist', async () => {
  const {testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const getRoomRequest = newGetRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    'no-such-room',
  );
  const getRoomResponse = await authDO.fetch(getRoomRequest);
  await expectAPIErrorResponse(getRoomResponse, {
    code: 404,
    resource: 'rooms',
    message: 'Room "no-such-room" not found',
  });
});

function newRoomRecordsRequest(queryParams?: Record<string, string>) {
  const query = queryParams
    ? `?${new URLSearchParams(queryParams).toString()}`
    : '';
  return new Request(
    `https://test.roci.dev${fmtPath(LIST_ROOMS_PATH)}${query}`,
    {
      method: 'get',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
}

test('roomRecords returns HTTP error for malformed request', async () => {
  const {testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const roomRecordsRequest = newRoomRecordsRequest({
    startKey: 'foo',
    startAfterKey: 'foo',
  });
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  await expectAPIErrorResponse(roomRecordsResponse, {
    code: 400,
    resource: 'request',
    message:
      'Query string error. Cannot specify both startKey and startAfterKey. Got object',
  });
});

test('roomRecords returns empty array if no rooms exist', async () => {
  const {testRoomDO, state} = createCreateRoomTestFixture();

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const roomRecordsRequest = newRoomRecordsRequest();
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  await expectSuccessfulAPIResponse(roomRecordsResponse, {
    results: [],
    numResults: 0,
    more: false,
  });
});

test('roomRecords returns rooms that exists', async () => {
  let roomNum = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: () => {
      throw 'should not be called';
    },
    newUniqueId: () => new TestDurableObjectId('unique-room-do-' + roomNum++),
    get: (id: DurableObjectId) =>
      // eslint-disable-next-line require-await
      new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.has(ROOM_ID_HEADER_NAME)).toBeTruthy();
        return new Response('', {status: 200});
      }),
  };

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  // Specifically test roomID="-" to verify a fence-post condition:
  // Because "-" is lexicographically before "/", the storage key "room/-/"
  // is lexicographically before "room//". An easy mistake to make would be
  // to assume that the lexicographically earliest room key is "room//",
  // but that would be AFTER "room/-.*/".
  await createRoom(authDO, '-');
  await createRoom(authDO, '1');
  await createRoom(authDO, '2');
  await createRoom(authDO, '3');

  const roomRecordsRequest = newRoomRecordsRequest();
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  await expectSuccessfulAPIResponse(roomRecordsResponse, {
    results: [
      {roomID: '-', jurisdiction: '', status: 'open'},
      {roomID: '1', jurisdiction: '', status: 'open'},
      {roomID: '2', jurisdiction: '', status: 'open'},
      {roomID: '3', jurisdiction: '', status: 'open'},
    ],
    numResults: 4,
    more: false,
  });

  let limitedRequest = newRoomRecordsRequest({maxResults: '3', startKey: ''});
  let limitedResponse = await authDO.fetch(limitedRequest);
  await expectSuccessfulAPIResponse(limitedResponse, {
    results: [
      {roomID: '-', jurisdiction: '', status: 'open'},
      {roomID: '1', jurisdiction: '', status: 'open'},
      {roomID: '2', jurisdiction: '', status: 'open'},
    ],
    numResults: 3,
    more: true,
  });

  limitedRequest = newRoomRecordsRequest({maxResults: '3', startAfterKey: '2'});
  limitedResponse = await authDO.fetch(limitedRequest);
  await expectSuccessfulAPIResponse(limitedResponse, {
    results: [{roomID: '3', jurisdiction: '', status: 'open'}],
    numResults: 1,
    more: false,
  });
});

function createConnectTestFixture(
  options: {
    testUserID?: string;
    testRoomID?: string;
    testClientID?: string;
    jurisdiction?: string | undefined;
    encodedTestAuth?: string | undefined;
    testAuth?: string | undefined;
    connectedClients?: {clientID: string; userID: string}[] | undefined;
  } = {},
) {
  const optionsWithDefault = {
    testUserID: 'testUserID1',
    testRoomID: 'testRoomID1',
    testClientID: 'testClientID1',
    encodedTestAuth: 'test%20auth%20token%20value%20%25%20encoded',
    testAuth: 'test auth token value % encoded',
    ...options,
  };
  const {
    testUserID,
    testRoomID,
    testClientID,
    jurisdiction,
    encodedTestAuth,
    testAuth,
    connectedClients,
  } = optionsWithDefault;

  const headers = new Headers();
  if (encodedTestAuth !== undefined) {
    headers.set('Sec-WebSocket-Protocol', encodedTestAuth);
  }
  headers.set('Upgrade', 'websocket');
  let url = `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}&userID=${testUserID}`;
  if (jurisdiction) {
    url += `&jurisdiction=${jurisdiction}`;
  }
  const testRequest = new Request(url, {
    headers,
  });

  const mocket = new Mocket();

  let numRooms = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName() {
      throw 'should not be called';
    },
    newUniqueId(options) {
      if (jurisdiction) {
        assert(options);
        expect(options.jurisdiction).toEqual(jurisdiction);
      }
      return new TestDurableObjectId('room-do-' + numRooms++, undefined);
    },
    get(id: DurableObjectId) {
      expect(id.toString()).toEqual('room-do-0');
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
        const url = new URL(request.url);
        if (new URLPattern({pathname: CREATE_ROOM_PATH}).test(request.url)) {
          return new Response();
        }

        if (url.pathname === AUTH_CONNECTIONS_PATH) {
          return new Response(
            JSON.stringify(
              connectedClients ?? [
                {userID: testUserID, clientID: testClientID},
              ],
            ),
          );
        }

        expect(request.url).toEqual(testRequest.url);
        expect(request.headers.get(AUTH_DATA_HEADER_NAME)).toEqual(
          encodeHeaderValue(JSON.stringify({userID: testUserID})),
        );
        if (encodedTestAuth !== undefined) {
          expect(request.headers.get('Sec-WebSocket-Protocol')).toEqual(
            encodedTestAuth,
          );
        }

        return upgradeWebsocketResponse(mocket, request.headers);
      });
    },
  };

  return {
    testAuth,
    testUserID,
    testRoomID,
    testClientID,
    testRequest,
    testRoomDO,
    mocket,
    encodedTestAuth,
  };
}

function createRoomDOThatThrowsIfFetchIsCalled(): DurableObjectNamespace {
  return {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      // eslint-disable-next-line require-await
      new TestDurableObjectStub(id, async (request: Request) => {
        throw new Error('Unexpected call to Room DO fetch ' + request.url);
      }),
  };
}

describe("connect will implicitly create a room that doesn't exist", () => {
  const t = (jurisdiction: string | undefined) => {
    test(`jurisdiction=${jurisdiction}:`, async () => {
      const {
        testAuth,
        testUserID,
        testRoomID,
        testRequest,
        testRoomDO,
        mocket,
        encodedTestAuth,
      } = createConnectTestFixture({jurisdiction});
      const logSink = new TestLogSink();
      const authDO = new TestAuthDO({
        roomDO: testRoomDO,
        state,
        // eslint-disable-next-line require-await
        authHandler: async (auth, roomID, env) => {
          expect(auth).toEqual(testAuth);
          expect(roomID).toEqual(testRoomID);
          expect(env).toEqual({foo: 'bar'});
          return {userID: testUserID};
        },
        logSink,
        logLevel: 'debug',
        env: {foo: 'bar'},
      });

      await connectAndTestThatRoomGotCreated(
        authDO,
        testRequest,
        mocket,
        encodedTestAuth,
        testUserID,
        storage,
        jurisdiction,
      );
    });
  };

  t(undefined);
  t('eu');
  t('invalid');
});

test('connect calls authHandler and sends resolved AuthData in header to Room DO', async () => {
  const {
    testAuth,
    testUserID,
    testRoomID,
    testRequest,
    testRoomDO,
    mocket,
    encodedTestAuth,
  } = createConnectTestFixture();
  const logSink = new TestLogSink();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({bar: 'foo'});
      return {userID: testUserID};
    },
    logSink,
    logLevel: 'debug',
    env: {bar: 'foo'},
  });

  await createRoom(authDO, testRoomID);

  await connectAndTestThatRoomGotCreated(
    authDO,
    testRequest,
    mocket,
    encodedTestAuth,
    testUserID,
    storage,
    undefined,
  );
});

describe('connect with undefined authHandler sends AuthData with url param userID to roomDO', () => {
  const t = (
    tTestAuth: string | undefined,
    tEncodedTestAuth: string | undefined,
  ) =>
    test(`${tTestAuth} - ${tEncodedTestAuth}`, async () => {
      const {
        testRoomID,
        testRequest,
        testRoomDO,
        mocket,
        encodedTestAuth,
        testUserID,
      } = createConnectTestFixture({
        testAuth: tTestAuth,
        encodedTestAuth: tEncodedTestAuth,
      });
      const logSink = new TestLogSink();
      const authDO = new TestAuthDO({
        roomDO: testRoomDO,
        state,
        authHandler: undefined,
        logSink,
        logLevel: 'debug',
        env: {foo: 'bar'},
      });

      await createRoom(authDO, testRoomID);

      await connectAndTestThatRoomGotCreated(
        authDO,
        testRequest,
        mocket,
        encodedTestAuth,
        testUserID,
        storage,
        undefined,
      );
    });

  t(
    'test auth token value % encoded',
    'test%20auth%20token%20value%20%25%20encoded',
  );
  t('', '');
  t(undefined, undefined);
});

test('connect wont connect to a room that is closed', async () => {
  jest.useRealTimers();
  const {testUserID, testRoomID, testRequest, testRoomDO} =
    createConnectTestFixture();
  const logSink = new TestLogSink();
  const [, serverWS] = mockWebSocketPair();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async () => ({userID: testUserID}),
    logSink,
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  await expectSuccessfulAPIResponse(closeRoomResponse);

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(serverWS.log).toEqual([
    ['send', JSON.stringify(['error', 'RoomClosed', 'testRoomID1'])],
    ['close'],
  ]);
});

test('connect percent escapes components of the connection key', async () => {
  const {
    testAuth,
    testUserID,
    testRoomID,
    testRequest,
    testRoomDO,
    mocket,
    encodedTestAuth,
  } = createConnectTestFixture({
    testUserID: '/testUserID/?',
    testRoomID: 'testRoomID',
    testClientID: '/testClientID/&',
  });

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({boo: 'far'});
      return {userID: testUserID};
    },
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {boo: 'far'},
  });
  await createRoom(authDO, testRoomID);

  const testTime = 1010101;
  jest.setSystemTime(testTime);
  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.webSocket).toBe(mocket);
  expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(
    encodedTestAuth,
  );
  expect(await storage.list({prefix: 'conn/'})).toEqual(
    new Map([
      [
        'conn/%2FtestUserID%2F%3F/testRoomID/%2FtestClientID%2F/',
        {connectTimestamp: testTime},
      ],
    ]),
  );
  expect(await storage.list({prefix: 'conns_by_room/'})).toEqual(
    new Map([
      [
        'conns_by_room/testRoomID/conn/%2FtestUserID%2F%3F/testRoomID/%2FtestClientID%2F/',
        {},
      ],
    ]),
  );
});

describe('connect pipes 401 over ws without calling Room DO if', () => {
  const testRoomID = 'testRoomID1';
  const testClientID = 'testClientID1';
  const testAuth = 'testAuthTokenValue';
  const testUserID = 'testUserID1';

  const t = (name: string, authHandler: AuthHandler, errorMessage: string) => {
    test(name, async () => {
      const headers = new Headers();
      headers.set('Sec-WebSocket-Protocol', testAuth);
      headers.set('Upgrade', 'websocket');

      const testRequest = new Request(
        `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}&userID=${testUserID}`,
        {
          headers,
        },
      );
      const [clientWS, serverWS] = mockWebSocketPair();

      const authDO = new TestAuthDO({
        roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
        state,
        authHandler,
        logSink: new TestLogSink(),
        logLevel: 'debug',
        env: {food: 'bard'},
      });

      const responseP = authDO.fetch(testRequest);

      // Have to wait a bit for the authHandler to get to the point of waiting on timers.
      for (let i = 0; i < 100; i++) {
        await Promise.resolve();
      }

      // This is arbitrary just has to be higher than authHandler timeout.
      jest.advanceTimersByTime(AUTH_HANDLER_TIMEOUT_MS + 1);

      const response = await responseP;
      expect(response.status).toEqual(101);
      expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(testAuth);
      expect(response.webSocket).toBe(clientWS);
      expect(serverWS.log).toEqual([
        ['send', JSON.stringify(['error', 'Unauthorized', errorMessage])],
        ['close'],
      ]);
    });
  };

  t(
    'authHandler throws',
    (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({food: 'bard'});
      throw new Error('Test authHandler reject');
    },
    'authHandler rejected: Error: Test authHandler reject',
  );

  t(
    'authHandler rejects',
    (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({food: 'bard'});
      return Promise.reject(new Error('Test authHandler reject'));
    },
    'authHandler rejected: Error: Test authHandler reject',
  );

  t(
    'authHandler returns null',
    (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({food: 'bard'});
      return null;
    },
    'no authData',
  );

  t(
    'authHandler returns Promise<null>',
    (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({food: 'bard'});
      return Promise.resolve(null);
    },
    'no authData',
  );

  t(
    'authHandler takes a very long time',
    async (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({food: 'bard'});
      await sleep(30000, setTimeout);
      return {userID: 'bonk'};
    },
    'authHandler rejected: Error: authHandler timed out',
  );
});

describe('connect sends InvalidConnectionRequest over ws without calling Room DO if Sec-WebSocket-Protocol header is missing', () => {
  const t = (headers: Headers) => {
    test(`headers: ${JSON.stringify(headers)}`, async () => {
      const testRoomID = 'testRoomID1';
      const testClientID = 'testClientID1';
      const [clientWS, serverWS] = mockWebSocketPair();

      const testRequest = new Request(
        `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}`,
        {
          headers,
        },
      );

      const authDO = new TestAuthDO({
        roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
        state,
        // eslint-disable-next-line require-await
        authHandler: () =>
          Promise.reject(new Error('Unexpected call to authHandler')),
        logSink: new TestLogSink(),
        logLevel: 'debug',
        env: {foo: 'bar'},
      });

      const response = await authDO.fetch(testRequest);

      expect(response.status).toEqual(101);
      expect(response.webSocket).toBe(clientWS);
      expect(serverWS.log).toEqual([
        [
          'send',
          JSON.stringify([
            'error',
            'InvalidConnectionRequest',
            'auth required',
          ]),
        ],
        ['close'],
      ]);
    });
  };

  t(new Headers([['Upgrade', 'websocket']]));
  t(
    new Headers([
      ['Upgrade', 'websocket'],
      ['Sec-WebSocket-Protocol', ''],
    ]),
  );
});

test('connect sends over InvalidConnectionRequest over ws without calling Room DO if userID is not present', async () => {
  const testRoomID = 'testRoomID1';
  const testClientID = 'testClientID1';
  const testAuth = 'testAuthTokenValue';

  const headers = new Headers();
  headers.set('Sec-WebSocket-Protocol', testAuth);
  headers.set('Upgrade', 'websocket');

  const testRequest = new Request(
    `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}`,
    {
      headers,
    },
  );
  const [clientWS, serverWS] = mockWebSocketPair();

  const authDO = new TestAuthDO({
    roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID, env) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      expect(env).toEqual({foo: 'bar'});
      return {userID: ''};
    },
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(testAuth);
  expect(response.webSocket).toBe(clientWS);
  expect(serverWS.log).toEqual([
    [
      'send',
      JSON.stringify([
        'error',
        'InvalidConnectionRequest',
        'userID parameter required',
      ]),
    ],
    ['close'],
  ]);
});

describe('connect sends VersionNotSupported error over ws if path is for unsupported version', () => {
  const t = (path: string) =>
    test('path: ' + path, async () => {
      const testRoomID = 'testRoomID1';
      const testClientID = 'testClientID1';

      const testAuth = 'testAuthTokenValue';

      const headers = new Headers();
      headers.set('Sec-WebSocket-Protocol', testAuth);
      headers.set('Upgrade', 'websocket');
      const testRequest = new Request(
        `ws://test.roci.dev${path}?roomID=${testRoomID}&clientID=${testClientID}`,
        {
          headers,
        },
      );
      const [clientWS, serverWS] = mockWebSocketPair();

      const authDO = new TestAuthDO({
        roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
        state,
        // eslint-disable-next-line require-await
        authHandler: () =>
          Promise.reject(new Error('Unexpected call to authHandler')),
        logSink: new TestLogSink(),
        logLevel: 'debug',
        env: {foo: 'bar'},
      });

      const response = await authDO.fetch(testRequest);

      expect(response.status).toEqual(101);
      expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(testAuth);
      expect(response.webSocket).toBe(clientWS);
      expect(serverWS.log).toEqual([
        [
          'send',
          JSON.stringify([
            'error',
            'VersionNotSupported',
            'unsupported version',
          ]),
        ],
        ['close'],
      ]);
    });
  t('/api/sync/v0/connect');
  t('/api/sync/v2/connect');
});

test('authInvalidateForUser when requests to roomDOs are successful', async () => {
  const testUserID = 'testUserID1';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_USER_CONNECTIONS_PATH, {
      userID: testUserID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  const testRequestClone = testRequest.clone();

  await storeTestConnectionState();
  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests. Plus, we can't get the RoomRecord
        // during the /createRoom call because it hasn't been written yet when /createRoom
        // is called!
        if (isInvalidateRequest(request)) {
          const roomRecord = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          const {roomID} = roomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const logSink = new TestLogSink();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink,
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(undefined);
  await expectSuccessfulAPIResponse(response);
});

test('authInvalidateForUser when connection ids have chars that need to be percent escaped', async () => {
  const testUserID = '/testUserID/?';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_USER_CONNECTIONS_PATH, {
      userID: testUserID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  const testRequestClone = testRequest.clone();

  await recordConnectionHelper(
    '/testUserID/?',
    'testRoomID1',
    '/testClientID1/&',
  );
  await recordConnectionHelper(
    '/testUserID/?',
    'testRoomID1',
    '/testClientID2/&',
  );
  await recordConnectionHelper(
    '/testUserID/?',
    'testRoomID2',
    '/testClientID3/&',
  );
  await recordConnectionHelper('testUserID2', 'testRoomID1', 'testClientID1');

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests.
        if (isInvalidateRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const logSink = new TestLogSink();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink,
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  await expectSuccessfulAPIResponse(response);
});

test('authInvalidateForUser when any request to roomDOs returns error response', async () => {
  const testUserID = 'testUserID1';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_USER_CONNECTIONS_PATH, {
      userID: testUserID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  const testRequestClone = testRequest.clone();

  await storeTestConnectionState();
  await recordConnectionHelper('testUserID1', 'testRoomID3', 'testClientID6');

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests.
        if (isInvalidateRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
          return roomID === 'testRoomID2'
            ? new Response(
                'Test authInvalidateForUser Internal Server Error Msg',
                {status: 500},
              )
            : new Response('Test Success', {status: 200});
        }
        return new Response('ok', {status: 200});
      }),
  };

  const logSink = new TestLogSink();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink,
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(3);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);
  expect(response.status).toEqual(500);
  expect(await response.text()).toEqual(
    'Test authInvalidateForUser Internal Server Error Msg',
  );
});

test('authInvalidateForRoom when request to roomDO is successful', async () => {
  const testRoomID = 'testRoomID1';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_ROOM_CONNECTIONS_PATH, {
      roomID: testRoomID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  const testRequestClone = testRequest.clone();
  await storeTestConnectionState();

  let roomDORequestCount = 0;
  let gotObjectId: DurableObjectId | undefined;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) => {
      gotObjectId = id;
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
        if (isInvalidateRequest(request)) {
          roomDORequestCount++;
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
        }
        return new Response('Test Success', {status: 200});
      });
    },
  };
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const response = await authDO.fetch(testRequest);

  const {roomID} = (await getRoomRecordByObjectID(
    storage,
    gotObjectId!,
  )) as RoomRecord;
  expect(roomID).toEqual(testRoomID);
  expect(roomDORequestCount).toEqual(1);
  await expectSuccessfulAPIResponse(response);
});

test('authInvalidateForRoom when roomID has no open connections no invalidate request is made to roomDO', async () => {
  const testRoomID = 'testRoomIDNoConnections';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_ROOM_CONNECTIONS_PATH, {
      roomID: testRoomID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  await storeTestConnectionState();

  let roomDORequestCount = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      // eslint-disable-next-line require-await
      new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
        if (isInvalidateRequest(request)) {
          roomDORequestCount++;
        }
        return new Response('Test Success', {status: 200});
      }),
  };
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCount).toEqual(0);
  await expectSuccessfulAPIResponse(response);
});

async function connectAndTestThatRoomGotCreated(
  authDO: BaseAuthDO,
  testRequest: Request,
  mocket: Mocket,
  encodedTestAuth: string | undefined,
  testUserID: string,
  storage: DurableObjectStorage,
  jurisdiction: string | undefined,
) {
  const testTime = 1010101;
  jest.setSystemTime(testTime);
  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  if (encodedTestAuth) {
    expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(
      encodedTestAuth,
    );
  }

  if (jurisdiction !== 'invalid') {
    expect(response.webSocket).toBe(mocket);
    expect((await storage.list({prefix: 'conn/'})).size).toEqual(1);
    const connectionRecord = (await storage.get(
      `conn/${testUserID}/testRoomID1/testClientID1/`,
    )) as Record<string, unknown> | undefined;
    assert(connectionRecord);
    expect(connectionRecord.connectTimestamp).toEqual(testTime);
    expect(await storage.list({prefix: 'conns_by_room/'})).toEqual(
      new Map([
        [
          `conns_by_room/testRoomID1/conn/${testUserID}/testRoomID1/testClientID1/`,
          {},
        ],
      ]),
    );
  } else {
    expect((await storage.list({prefix: 'conn/'})).size).toEqual(0);
    const connectionRecord = await storage.get(
      `conn/${testUserID}/testRoomID1/testClientID1/`,
    );
    expect(connectionRecord).toBeUndefined();
    expect((await storage.list({prefix: 'conns_by_room/'})).size).toEqual(0);
  }
}

async function createRoom(
  authDO: BaseAuthDO,
  roomID: string,
  authApiKey = TEST_API_KEY,
) {
  const createRoomRequest = newCreateRoomRequest(
    'https://test.roci.dev/',
    authApiKey,
    roomID,
  );
  const resp = await authDO.fetch(createRoomRequest);
  await expectSuccessfulAPIResponse(resp);
}

test('authInvalidateForRoom when request to roomDO returns error response', async () => {
  const testRoomID = 'testRoomID1';
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_ROOM_CONNECTIONS_PATH, {
      roomID: testRoomID,
    })}`,
    {
      method: 'post',
      headers: createAPIHeaders(TEST_API_KEY),
    },
  );
  const testRequestClone = testRequest.clone();
  await storeTestConnectionState();

  let roomDORequestCount = 0;
  let gotObjectId: DurableObjectId | undefined;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) => {
      gotObjectId = id;
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
        if (isInvalidateRequest(request)) {
          roomDORequestCount++;
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
          return new Response(
            'Test authInvalidateForRoom Internal Server Error Msg',
            {status: 500},
          );
        }
        return new Response('ok', {status: 200});
      });
    },
  };

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, testRoomID);

  const response = await authDO.fetch(testRequest);

  const {roomID} = (await getRoomRecordByObjectID(
    storage,
    gotObjectId!,
  )) as RoomRecord;
  expect(roomID).toEqual(testRoomID);
  expect(roomDORequestCount).toEqual(1);
  expect(response.status).toEqual(500);
  expect(await response.text()).toEqual(
    'Test authInvalidateForRoom Internal Server Error Msg',
  );
});

test('authInvalidateAll when requests to roomDOs are successful', async () => {
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_ALL_CONNECTIONS_PATH)}`,
    {
      headers: createAPIHeaders(TEST_API_KEY),
      method: 'post',
      body: '',
    },
  );
  const testRequestClone = testRequest.clone();

  await storeTestConnectionState();

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        if (isInvalidateRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);
  await expectSuccessfulAPIResponse(response);

  expect(roomDORequestCountsByRoomID.size).toEqual(3);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);
});

function expectForwardedAuthInvalidateRequest(
  forwardedRequest: Request,
  originalRequest: Request,
) {
  expect(forwardedRequest.url).toEqual(originalRequest.url);
  expect(forwardedRequest.method).toEqual(originalRequest.method);
  expect(forwardedRequest.headers.get(API_KEY_HEADER_NAME)).toEqual(
    originalRequest.headers.get(API_KEY_HEADER_NAME),
  );
}

test('authInvalidateAll when any request to roomDOs returns error response', async () => {
  const testRequest = new Request(
    `https://test.roci.dev${fmtPath(INVALIDATE_ALL_CONNECTIONS_PATH)}`,
    {
      headers: createAPIHeaders(TEST_API_KEY),
      method: 'post',
      body: '',
    },
  );
  const testRequestClone = testRequest.clone();

  await storeTestConnectionState();

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        if (isInvalidateRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expectForwardedAuthInvalidateRequest(request, testRequestClone);
          return roomID === 'testRoomID2'
            ? new Response('Test authInvalidateAll Internal Server Error Msg', {
                status: 500,
              })
            : new Response('Test Success', {status: 200});
        }
        return new Response('ok', {status: 200});
      }),
  };

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(500);

  expect(roomDORequestCountsByRoomID.size).toEqual(3);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);
  expect(await response.text()).toEqual(
    'Test authInvalidateAll Internal Server Error Msg',
  );
});

describe('test unexpected query params or body rejected', () => {
  const roomID = 'testRoomID1';
  const URL_PREFIX = `https://roci.dev`;
  let authDO: TestAuthDO;

  beforeEach(async () => {
    storage = await getMiniflareDurableObjectStorage(authDOID);
    await storage.deleteAll();
    state = new TestDurableObjectState(authDOID, storage);

    const {testRoomID, testRoomDO} = createCreateRoomTestFixture({
      testRoomID: roomID,
    });

    authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: () => Promise.reject('should not be called'),
      logSink: new TestLogSink(),
      logLevel: 'debug',
      env: {foo: 'bar'},
    });
    await createRoom(authDO, testRoomID);
  });

  type Case = {
    method: 'GET' | 'POST';
    url: string;
    acceptsQueryString?: boolean;
    expectedSuccessStatus?: number;
  };
  const cases: Case[] = [
    {
      method: 'GET',
      url: `${URL_PREFIX}${fmtPath(LIST_ROOMS_PATH)}`,
      acceptsQueryString: true,
    },
    {method: 'GET', url: `${URL_PREFIX}${fmtPath(GET_ROOM_PATH, {roomID})}`},
    {
      method: 'POST',
      url: `${URL_PREFIX}${fmtPath(INVALIDATE_ROOM_CONNECTIONS_PATH, {
        roomID,
      })}`,
    },
    {
      method: 'POST',
      url: `${URL_PREFIX}${fmtPath(INVALIDATE_USER_CONNECTIONS_PATH, {
        userID: 'foo',
      })}`,
    },
    {
      method: 'POST',
      url: `${URL_PREFIX}${fmtPath(INVALIDATE_ALL_CONNECTIONS_PATH)}`,
    },
    {method: 'POST', url: `${URL_PREFIX}${fmtPath(CLOSE_ROOM_PATH, {roomID})}`},
    {
      method: 'POST',
      url: `${URL_PREFIX}${fmtPath(DELETE_ROOM_PATH, {roomID})}`,
      expectedSuccessStatus: 409, // Can't delete without close
    },
  ];

  for (const c of cases) {
    test(c.url, async () => {
      const unexpectedQuery = new Request(`${c.url}?foobar=bonk`, {
        method: c.method,
        headers: createAPIHeaders(TEST_API_KEY),
      });

      const resp1 = await authDO.fetch(unexpectedQuery);
      await expectAPIErrorResponse(resp1, {
        code: 400,
        resource: 'request',
        message: c.acceptsQueryString
          ? 'Query string error. Unexpected property foobar'
          : 'Unexpected query parameters',
      });

      if (c.method !== 'GET') {
        const unexpectedBody = new Request(c.url, {
          method: c.method,
          headers: createAPIHeaders(TEST_API_KEY),
          body: '{"foo":"bar"}',
        });
        const resp2 = await authDO.fetch(unexpectedBody);
        await expectAPIErrorResponse(resp2, {
          code: 400,
          resource: 'request',
          message: 'Unexpected request body.',
        });
      }

      // Finally, sanity check that the request itself is accepted.
      const validRequest = new Request(c.url, {
        method: c.method,
        headers: createAPIHeaders(TEST_API_KEY),
      });
      const resp3 = await authDO.fetch(validRequest);
      expect(resp3.status).toBe(c.expectedSuccessStatus ?? 200);
    });
  }
});

async function createRevalidateConnectionsTestFixture({
  roomDOIDWithErrorResponse,
}: {roomDOIDWithErrorResponse?: string} = {}) {
  await storeTestConnectionState();

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        if (isAuthRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(roomID);
          expect(request.url).toEqual(
            'https://unused-reflect-room-do.dev/api/auth/v0/connections',
          );
          if (roomDOIDWithErrorResponse === roomID) {
            return new Response(
              'Test revalidateConnections Internal Server Error Msg',
              {
                status: 500,
              },
            );
          }
          switch (roomID) {
            case 'testRoomID1':
              return new Response(
                JSON.stringify([
                  {userID: 'testUserID1', clientID: 'testClientID1'},
                  {userID: 'testUserID2', clientID: 'testClientID4'},
                ]),
              );
            case 'testRoomID2':
              return new Response(
                JSON.stringify([
                  {userID: 'testUserID1', clientID: 'testClientID3'},
                ]),
              );
            case 'testRoomID3':
              return new Response(JSON.stringify([]));
            default:
              throw new Error(`Unexpected roomID ${roomID}`);
          }
        }
        return new Response('ok', {status: 200});
      }),
  };

  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    logSink: new TestLogSink(),
    logLevel: 'debug',
    env: {foo: 'bar'},
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');
  return {authDO, roomDORequestCountsByRoomID, storage};
}

test('revalidateConnections', async () => {
  const {authDO, roomDORequestCountsByRoomID, storage} =
    await createRevalidateConnectionsTestFixture();

  await authDO.runRevalidateConnectionsTaskForTest();

  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'conn/'})).keys()]).toEqual([
    'conn/testUserID1/testRoomID1/testClientID1/',
    'conn/testUserID1/testRoomID2/testClientID3/',
    'conn/testUserID2/testRoomID1/testClientID4/',
  ]);
  expect([...(await storage.list({prefix: 'conns_by_room/'})).keys()]).toEqual([
    'conns_by_room/testRoomID1/conn/testUserID1/testRoomID1/testClientID1/',
    'conns_by_room/testRoomID1/conn/testUserID2/testRoomID1/testClientID4/',
    'conns_by_room/testRoomID2/conn/testUserID1/testRoomID2/testClientID3/',
  ]);
});

test('revalidateConnections continues if one storage delete throws an error', async () => {
  const {authDO, roomDORequestCountsByRoomID, storage} =
    await createRevalidateConnectionsTestFixture();

  jest.spyOn(storage, 'delete').mockImplementationOnce(() => {
    throw new Error('test delete error');
  });

  await authDO.runRevalidateConnectionsTaskForTest();

  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'conn/'})).keys()]).toEqual([
    'conn/testUserID1/testRoomID1/testClientID1/',
    'conn/testUserID1/testRoomID1/testClientID2/',
    'conn/testUserID1/testRoomID2/testClientID3/',
    'conn/testUserID2/testRoomID1/testClientID4/',
  ]);
  expect([...(await storage.list({prefix: 'conns_by_room/'})).keys()]).toEqual([
    'conns_by_room/testRoomID1/conn/testUserID1/testRoomID1/testClientID1/',
    'conns_by_room/testRoomID1/conn/testUserID1/testRoomID1/testClientID2/',
    'conns_by_room/testRoomID1/conn/testUserID2/testRoomID1/testClientID4/',
    'conns_by_room/testRoomID2/conn/testUserID1/testRoomID2/testClientID3/',
  ]);
});

test('revalidateConnections continues if one roomDO returns an error', async () => {
  const {authDO, roomDORequestCountsByRoomID, storage} =
    await createRevalidateConnectionsTestFixture({
      roomDOIDWithErrorResponse: 'testRoomID1',
    });

  await authDO.runRevalidateConnectionsTaskForTest();

  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'conn/'})).keys()]).toEqual([
    'conn/testUserID1/testRoomID1/testClientID1/',
    'conn/testUserID1/testRoomID1/testClientID2/',
    'conn/testUserID1/testRoomID2/testClientID3/',
    'conn/testUserID2/testRoomID1/testClientID4/',
  ]);
  expect([...(await storage.list({prefix: 'conns_by_room/'})).keys()]).toEqual([
    'conns_by_room/testRoomID1/conn/testUserID1/testRoomID1/testClientID1/',
    'conns_by_room/testRoomID1/conn/testUserID1/testRoomID1/testClientID2/',
    'conns_by_room/testRoomID1/conn/testUserID2/testRoomID1/testClientID4/',
    'conns_by_room/testRoomID2/conn/testUserID1/testRoomID2/testClientID3/',
  ]);
});

function createTailTestFixture(
  options: {
    testRoomID?: string | null;
    testApiToken?: string | null;
  } = {},
) {
  const {testRoomID = null, testApiToken = null} = options;

  const headers = new Headers();
  if (testApiToken !== null) {
    headers.set('Sec-WebSocket-Protocol', testApiToken);
  }
  headers.set('Upgrade', 'websocket');
  const tailURL = new URL(TAIL_URL_PATH, 'ws://test.roci.dev');
  if (testRoomID !== null) {
    tailURL.searchParams.set('roomID', testRoomID);
  }

  const testRequest = new Request(tailURL.toString(), {
    headers,
  });

  const socketFromRoomDO = new Mocket();

  let numRooms = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName() {
      throw 'should not be called';
    },
    newUniqueId() {
      return new TestDurableObjectId('room-do-' + numRooms++, undefined);
    },
    get(id: DurableObjectId) {
      expect(id.toString()).toEqual('room-do-0');
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        if (request.url !== tailURL.toString()) {
          return new Response();
        }
        expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
        expect(request.url).toEqual(testRequest.url);
        expect(request.headers.has(AUTH_DATA_HEADER_NAME)).toBe(false);
        if (testApiToken !== undefined) {
          expect(request.headers.get('Sec-WebSocket-Protocol')).toEqual(
            testApiToken,
          );
        } else {
          expect(request.headers.has('Sec-WebSocket-Protocol')).toBe(false);
        }

        return upgradeWebsocketResponse(socketFromRoomDO, request.headers);
      });
    },
  };

  return {
    testRoomID,
    testRequest,
    testRoomDO,
    socketFromRoomDO,
    testApiToken,
  };
}

describe('tail', () => {
  const t = (
    name: string,
    options: {
      testApiToken: string | null;
      authApiKey?: string;
      testRoomID: string | null;
      expectedError?: TailErrorMessage;
    },
  ) => {
    test(name, async () => {
      const [, server] = mockWebSocketPair();
      const {testRoomID, testRequest, testRoomDO, socketFromRoomDO} =
        createTailTestFixture(options);
      const {testApiToken, expectedError, authApiKey = TEST_API_KEY} = options;
      const logSink = new TestLogSink();
      const authDO = new TestAuthDO({
        roomDO: testRoomDO,
        state,
        logSink,
        logLevel: 'debug',
        env: {foo: 'bar'},
      });

      if (testRoomID) {
        await createRoom(authDO, testRoomID, authApiKey);
      }

      // Go through the full Worker -> AuthDO -> RoomDO stack to capture both auth and business logic.
      const worker = createWorker(() => ({logSink, logLevel: 'debug'}));
      const response = must(
        await worker.fetch?.(
          testRequest,
          {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            REFLECT_API_KEY: authApiKey,
            authDO: {
              ...createTestDurableObjectNamespace(),
              get: (id: DurableObjectId) =>
                new TestDurableObjectStub(id, (request: Request) =>
                  authDO.fetch(request),
                ),
            },
          },
          new TestExecutionContext(),
        ),
      );

      expect(response.status).toEqual(101);
      expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(
        testApiToken,
      );

      if (expectedError) {
        // The server sends an error followed by a close message
        expect(server.log).toEqual([
          ['send', JSON.stringify(expectedError)],
          ['close'],
        ]);
      } else {
        expect(server.log).toEqual([]);
        expect(response.webSocket).toBe(socketFromRoomDO);
      }
    });
  };

  t('basic', {testApiToken: TEST_API_KEY, testRoomID: 'testRoomID1'});
  t('without api token', {
    testApiToken: null,
    testRoomID: 'testRoomID1',
    expectedError: {
      type: 'error',
      kind: 'Unauthorized',
      message: 'auth required',
    },
  });
  t('wrong api token', {
    testApiToken: 'wrong',
    testRoomID: 'testRoomID1',
    expectedError: {
      type: 'error',
      kind: 'Unauthorized',
      message: 'auth required',
    },
  });
  t('without api token but with roomID', {
    testApiToken: null,
    testRoomID: 'hello',
    expectedError: {
      type: 'error',
      kind: 'Unauthorized',
      message: 'auth required',
    },
  });
  t('missing room id', {
    testApiToken: TEST_API_KEY,
    testRoomID: null,
    expectedError: {
      type: 'error',
      kind: 'InvalidConnectionRequest',
      message: 'roomID parameter required',
    },
  });
  t('api token with spaces', {
    testApiToken: 'a b c',
    authApiKey: 'a b c',
    testRoomID: 'testRoomID1',
  });
});

test('tail not a websocket', async () => {
  const testRoomID = 'testRoomID1';
  const {testRequest, testRoomDO} = createTailTestFixture({testRoomID});
  testRequest.headers.delete('Upgrade');

  const logSink = new TestLogSink();
  const authDO = new TestAuthDO({
    roomDO: testRoomDO,
    state,
    logSink,
    logLevel: 'debug',
    env: {foo: 'bar'},
  });

  await createRoom(authDO, testRoomID);

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(400);
});

describe('Alarms', () => {
  async function connect(
    testAuth: string,
    connectedClients?: {clientID: string; userID: string}[],
  ) {
    const jurisdiction = undefined;
    const {testUserID, testRequest, testRoomDO, mocket, encodedTestAuth} =
      createConnectTestFixture({
        testAuth,
        encodedTestAuth: testAuth,
        connectedClients,
      });
    const logSink = new TestLogSink();
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      logSink,
      logLevel: 'debug',
      env: {foo: 'bar'},
    });

    await connectAndTestThatRoomGotCreated(
      authDO,
      testRequest,
      mocket,
      encodedTestAuth,
      testUserID,
      storage,
      jurisdiction,
    );

    return {authDO, logSink};
  }

  test('When the alarm is triggered we should revalidate the connections', async () => {
    const {logSink} = await connect('abc');
    const alarm = await state.storage.getAlarm();
    // In tests the time doesn't change unless we manually increment it so the
    // alarm should be set to the current time + the interval. In a non test
    // environment the alarm will be dependent on the time of the call to
    // setAlarm.
    expect(alarm).toBe(Date.now() + ALARM_INTERVAL);

    logSink.messages.length = 0;
    await jest.advanceTimersByTimeAsync(ALARM_INTERVAL);

    // What happens during reauthentication is a black box except for the logs...
    expect(logSink.messages.flatMap(msg => msg[2])).toMatchInlineSnapshot(`
      [
        "Firing 1 timeout(s)",
        "Revalidating connections waiting for lock.",
        "Revalidating connections acquired lock.",
        "Revalidating 1 connections for room testRoomID1.",
        "waiting for authLock.",
        "authLock acquired.",
        "Sending request https://unused-reflect-room-do.dev/api/auth/v0/connections to roomDO with roomID testRoomID1",
        "Starting RoomDO fetch for revalidate connections",
        "Finished RoomDO fetch for revalidate connections in 0ms",
        "received DO response",
        200,
        "",
        "Revalidated 1 connections for room testRoomID1, deleted 0 connections.",
        "Revalidated 1 connections, deleted 0 connections.  Failed to revalidate 0 connections.",
        "Ensuring revalidate connections task is scheduled.",
        "Scheduling revalidate connections task.",
        "Scheduled immediate Alarm to flush items from this Alarm",
      ]
    `);

    // Fire the flush Alarm.
    logSink.messages.length = 0;
    await jest.advanceTimersByTimeAsync(1);
    expect(logSink.messages.flatMap(msg => msg[2])).toMatchInlineSnapshot(`
    [
      "Fired empty Alarm to flush events to Tail Log",
      "Next Alarm fires in 299999 ms",
    ]
    `);
  });

  test('When the alarm is triggered we should revalidate the connections (delete one)', async () => {
    const {logSink} = await connect('abc', []);
    logSink.messages.length = 0;
    await jest.advanceTimersByTimeAsync(ALARM_INTERVAL);

    // What happens during reauthentication is a black box except for the logs...
    expect(logSink.messages.flatMap(msg => msg[2])).toMatchInlineSnapshot(`
      [
        "Firing 1 timeout(s)",
        "Revalidating connections waiting for lock.",
        "Revalidating connections acquired lock.",
        "Revalidating 1 connections for room testRoomID1.",
        "waiting for authLock.",
        "authLock acquired.",
        "Sending request https://unused-reflect-room-do.dev/api/auth/v0/connections to roomDO with roomID testRoomID1",
        "Starting RoomDO fetch for revalidate connections",
        "Finished RoomDO fetch for revalidate connections in 0ms",
        "received DO response",
        200,
        "",
        "Revalidated 1 connections for room testRoomID1, deleted 1 connections.",
        "Revalidated 1 connections, deleted 1 connections.  Failed to revalidate 0 connections.",
        "Scheduled immediate Alarm to flush items from this Alarm",
      ]
    `);

    // Fire the flush Alarm.
    logSink.messages.length = 0;
    await jest.advanceTimersByTimeAsync(1);
    expect(logSink.messages.flatMap(msg => msg[2])).toMatchInlineSnapshot(`
    [
      "Fired empty Alarm to flush events to Tail Log",
      "No more timeouts scheduled",
    ]
    `);
  });
});

describe('client disconnect', () => {
  function createDisconnectTestFixture(
    options: {
      testUserID?: string;
      testRoomID?: string;
      testClientID?: string;
      encodedTestAuth?: string | undefined;
      testAuth?: string | undefined;
      connectedClients?: {clientID: string; userID: string}[] | undefined;
    } = {},
  ) {
    const optionsWithDefault = {
      testUserID: 'testUserID1',
      testRoomID: 'testRoomID1',
      testClientID: 'testClientID1',
      encodedTestAuth: 'test%20auth%20token%20value%20%25%20encoded',
      testAuth: 'test auth token value % encoded',
      ...options,
    };
    const {
      testUserID,
      testRoomID,
      testClientID,
      encodedTestAuth,
      testAuth,
      connectedClients,
    } = optionsWithDefault;

    const headers = new Headers();
    if (encodedTestAuth !== undefined) {
      headers.set('Authorization', 'Bearer ' + encodedTestAuth);
    }
    const url = `http://test.roci.dev/api/sync/v1/disconnect?roomID=${testRoomID}&clientID=${testClientID}&userID=${testUserID}`;

    const testRequest = new Request(url, {
      method: 'POST',
      headers,
    });

    let numRooms = 0;
    const testRoomDO: DurableObjectNamespace = {
      ...createTestDurableObjectNamespace(),
      idFromName() {
        throw 'should not be called';
      },
      newUniqueId() {
        return new TestDurableObjectId('room-do-' + numRooms++, undefined);
      },
      get(id: DurableObjectId) {
        expect(id.toString()).toEqual('room-do-0');
        // eslint-disable-next-line require-await
        return new TestDurableObjectStub(id, async (request: Request) => {
          expect(request.headers.get(ROOM_ID_HEADER_NAME)).toEqual(testRoomID);
          const url = new URL(request.url);
          if (new URLPattern({pathname: CREATE_ROOM_PATH}).test(request.url)) {
            return new Response();
          }

          if (url.pathname === AUTH_CONNECTIONS_PATH) {
            return new Response(
              JSON.stringify(
                connectedClients ?? [
                  {userID: testUserID, clientID: testClientID},
                ],
              ),
            );
          }

          expect(request.url).toEqual(testRequest.url);
          expect(request.headers.get(AUTH_DATA_HEADER_NAME)).toEqual(
            encodeHeaderValue(JSON.stringify({userID: testUserID})),
          );
          if (encodedTestAuth !== undefined) {
            expect(request.headers.get('Authorization')).toEqual(
              'Bearer ' + encodedTestAuth,
            );
          }

          return new Response('test ok', {status: 200});
        });
      },
    };

    return {
      testAuth,
      testUserID,
      testRoomID,
      testClientID,
      testRequest,
      testRoomDO,
      encodedTestAuth,
    };
  }

  beforeEach(() => {
    setConfig('disconnectBeacon', true);
  });

  afterEach(() => {
    resetAllConfig();
  });

  test("disconnect will not create a room that doesn't exist", async () => {
    const {testAuth, testUserID, testRoomID, testRequest, testRoomDO} =
      createDisconnectTestFixture({});
    const logSink = new TestLogSink();
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      // eslint-disable-next-line require-await
      authHandler: async (auth, roomID, env) => {
        expect(auth).toEqual(testAuth);
        expect(roomID).toEqual(testRoomID);
        expect(env).toEqual({foo: 'bar'});
        return {userID: testUserID};
      },
      logSink,
      logLevel: 'debug',
      env: {foo: 'bar'},
    });

    const testTime = 1010101;
    jest.setSystemTime(testTime);
    const response = await authDO.fetch(testRequest);

    expect(response.status).toEqual(404);

    expect((await storage.list({prefix: 'conn/'})).size).toEqual(0);
  });

  test('disconnect will get forwarded to room do if room exists and authenticated', async () => {
    const {testAuth, testUserID, testRoomID, testRequest, testRoomDO} =
      createDisconnectTestFixture({});
    const logSink = new TestLogSink();
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: (auth, roomID, env) => {
        expect(auth).toEqual(testAuth);
        expect(roomID).toEqual(testRoomID);
        expect(env).toEqual({foo: 'bar'});
        return {userID: testUserID};
      },
      logSink,
      logLevel: 'debug',
      env: {foo: 'bar'},
    });

    await createRoom(authDO, testRoomID);

    const testTime = 1010101;
    jest.setSystemTime(testTime);
    const response = await authDO.fetch(testRequest);

    expect(response.status).toEqual(200);
    expect(await response.text()).toEqual('test ok');
  });

  test('disconnect will get forwarded to room do if room exists and no authHandler', async () => {
    const {testRoomID, testRequest, testRoomDO} = createDisconnectTestFixture(
      {},
    );
    const logSink = new TestLogSink();
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      logSink,
      logLevel: 'debug',
      env: {foo: 'bar'},
    });

    await createRoom(authDO, testRoomID);

    const testTime = 1010101;
    jest.setSystemTime(testTime);
    const response = await authDO.fetch(testRequest);

    expect(response.status).toEqual(200);
    expect(await response.text()).toEqual('test ok');
  });

  test('disconnect authentication fail', async () => {
    const {testRoomID, testRequest, testRoomDO} = createDisconnectTestFixture({
      encodedTestAuth: 'abc',
    });
    const logSink = new TestLogSink();
    const authDO = new TestAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: () => null,
      logSink,
      logLevel: 'debug',
      env: {foo: 'bar'},
    });

    await createRoom(authDO, testRoomID);

    const testTime = 1010101;
    jest.setSystemTime(testTime);
    const response = await authDO.fetch(testRequest);

    expect(response.status).toEqual(403);
    expect(await response.text()).toEqual('no authData');
  });
});
