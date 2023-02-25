/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
  jest,
  afterEach,
  beforeEach,
  test,
  expect,
  describe,
} from '@jest/globals';
import {encodeHeaderValue} from '../util/headers.js';
import {Mocket, mockWebSocketPair, TestLogSink} from '../util/test-utils.js';
import {USER_DATA_HEADER_NAME} from './auth.js';
import {
  createTestDurableObjectNamespace,
  TestDurableObjectId,
  TestDurableObjectState,
  TestDurableObjectStub,
} from './do-test-utils.js';
import {BaseAuthDO, AUTH_ROUTES, ConnectionRecord} from './auth-do.js';
import {
  AUTH_API_KEY_HEADER_NAME,
  createAuthAPIHeaders,
} from './auth-api-headers.js';
import {
  type RoomRecord,
  roomRecordByRoomID as getRoomRecordOriginal,
  roomRecordByObjectIDForTest as getRoomRecordByObjectIDOriginal,
  RoomStatus,
} from './rooms.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {
  newCloseRoomRequest,
  newCreateRoomRequest,
  newDeleteRoomRequest,
  newForgetRoomRequest,
  newMigrateRoomRequest,
  newRoomStatusRequest,
} from '../client/room.js';
import {ErrorKind} from 'reflect-protocol';
import {newInvalidateAllAuthRequest} from '../client/auth.js';
import {newAuthRevalidateConnections} from '../util/auth-test-util.js';

const TEST_AUTH_API_KEY = 'TEST_REFLECT_AUTH_API_KEY_TEST';
const {authDO} = getMiniflareBindings();
const authDOID = authDO.idFromName('auth');

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
});

function isAuthRequest(request: Request) {
  return request.url.indexOf('/api/auth/') !== -1;
}

async function createCreateRoomTestFixture() {
  const testRoomID = 'testRoomID1';

  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);

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
        const url = new URL(request.url);
        if (url.pathname === '/createRoom') {
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
    await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  // Create the room for the first time.
  const response = await authDO.fetch(testRequest);

  expect(roomDOcreateRoomCounts.size).toEqual(1);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr).not.toBeUndefined();
  const roomRecord = rr as RoomRecord;
  expect(roomRecord.objectIDString).toEqual('unique-room-do-0');
  expect(roomDOcreateRoomCounts.get(roomRecord.objectIDString)).toEqual(1);
  expect(response.status).toEqual(200);

  // Attempt to create the room again.
  const response2 = await authDO.fetch(testRequest);
  expect(response2.status).toEqual(409);
  expect(roomDOcreateRoomCounts.size).toEqual(1);
});

test('createRoom allows slashes in roomIDs', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    '/',
  );
  let response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);

  response = await authDO.fetch(newRoomRecordsRequest());
  expect(await response.json()).toEqual([
    {
      jurisdiction: '',
      objectIDString: 'unique-room-do-0',
      roomID: '/',
      status: 'open',
    },
  ]);

  response = await authDO.fetch(
    newRoomStatusRequest('https://teset.roci.dev/', TEST_AUTH_API_KEY, '/'),
  );
  expect(await response.json()).toEqual({status: 'open'});
});

test('createRoom requires roomIDs to not contain weird characters', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const roomIDs = ['', ' ', testRoomID + '!', '$', ' foo ', '🤷'];
  for (const roomID of roomIDs) {
    const testRequest = newCreateRoomRequest(
      'https://test.roci.dev',
      TEST_AUTH_API_KEY,
      roomID,
    );
    const response = await authDO.fetch(testRequest);
    expect(response.status).toEqual(400);
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

test('createRoom returns 401 if authApiKey is wrong', async () => {
  const {testRoomID, testRequest, testRoomDO, state, roomDOcreateRoomCounts} =
    await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: 'SOME OTHER API KEY',
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(401);
  expect(roomDOcreateRoomCounts.size).toEqual(0);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr).toBeUndefined();
});

test('createRoom returns 500 if roomDO createRoom fails', async () => {
  const {testRoomID, testRequest, testRoomDO, state} =
    await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
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
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const testRequest = newCreateRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
    'eu',
  );

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
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
  expect(response.status).toEqual(200);
  expect(gotJurisdiction).toEqual(true);
  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr?.jurisdiction).toEqual('eu');
});

test('migrate room creates a room record', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  testRoomDO.idFromName = (name: string) =>
    new TestDurableObjectId(`id-${name}`);
  const expectedObjectIDString = testRoomDO.idFromName(testRoomID).toString();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const migrateRoomRequest = newMigrateRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const response = await authDO.fetch(migrateRoomRequest);
  expect(response.status).toEqual(200);

  const rr = await getRoomRecord(state.storage, testRoomID);
  expect(rr).not.toBeUndefined();
  const roomRecord = rr as RoomRecord;
  expect(roomRecord.objectIDString).toEqual(expectedObjectIDString);
});

test('migrate room enforces roomID format', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const migrateRoomRequest = newMigrateRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    'not allowed! ',
  );
  const response = await authDO.fetch(migrateRoomRequest);
  expect(response.status).toEqual(400);
});

test('401s if wrong auth api key', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();
  const wrongApiKey = 'WRONG KEY';
  const migrateRoomRequest = newMigrateRoomRequest(
    'https://test.roci.dev',
    wrongApiKey,
    testRoomID,
  );

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    wrongApiKey,
    testRoomID,
  );

  const forgetRoomRequest = newForgetRoomRequest(
    'https://test.roci.dev',
    wrongApiKey,
    testRoomID,
  );

  const invalidateAllRequest = newInvalidateAllAuthRequest(
    'https://test.roci.dev',
    wrongApiKey,
  );

  const authRevalidateConnections = newAuthRevalidateConnections(
    'https://test.roci.dev',
    wrongApiKey,
  );

  const requests = [
    migrateRoomRequest,
    deleteRoomRequest,
    forgetRoomRequest,
    invalidateAllRequest,
    authRevalidateConnections,
  ];

  for (const request of requests) {
    const authDO = new BaseAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: () => Promise.reject('should not be called'),
      authApiKey: TEST_AUTH_API_KEY,
      logSink: new TestLogSink(),
      logLevel: 'debug',
    });
    const response = await authDO.fetch(request);
    expect(response.status).toEqual(401);
  }
});

test('400 bad body requests', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();
  const undefinedInvalidateForUserRequest = createBadBodyRequest(
    AUTH_ROUTES.authInvalidateForUser,
    null,
  );
  const badInvalidateForUserRequest = createBadBodyRequest(
    AUTH_ROUTES.authInvalidateForUser,
    JSON.stringify({badUserID: 'foo'}),
  );

  const undefinedInvalidateForRoomRequest = createBadBodyRequest(
    AUTH_ROUTES.authInvalidateForRoom,
    null,
  );
  const badInvalidateForRoomRequest = createBadBodyRequest(
    AUTH_ROUTES.authInvalidateForRoom,
    JSON.stringify({badRoomId: 'foo'}),
  );

  const badCreateRoomRequest = createBadBodyRequest(
    AUTH_ROUTES.createRoom,
    JSON.stringify({badRoomId: 'foo'}),
  );

  const badLegacyCreateRoomRequest = createBadBodyRequest(
    AUTH_ROUTES.legacyCreateRoom,
    JSON.stringify({badRoomId: 'legacyfoo'}),
  );

  const requests = [
    undefinedInvalidateForUserRequest,
    badInvalidateForUserRequest,
    undefinedInvalidateForRoomRequest,
    badInvalidateForRoomRequest,
    badCreateRoomRequest,
    badLegacyCreateRoomRequest,
  ];

  for (const request of requests) {
    const authDO = new BaseAuthDO({
      roomDO: testRoomDO,
      state,
      authHandler: () => Promise.reject('should not be called'),
      authApiKey: TEST_AUTH_API_KEY,
      logSink: new TestLogSink(),
      logLevel: 'debug',
    });
    const response = await authDO.fetch(request);
    expect(response.status).toEqual(400);
  }
});

function createBadBodyRequest(path: string, body: BodyInit | null): Request {
  const url = new URL(path, 'https://roci.dev');
  return new Request(url.toString(), {
    method: 'post',
    headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
    body,
  });
}

test('closeRoom closes an open room', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  expect(closeRoomResponse.status).toEqual(200);

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Closed,
  });
});

test('closeRoom 404s on non-existent room', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  // Note: no createRoom.

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  expect(closeRoomResponse.status).toEqual(404);
});

test('calling closeRoom on closed room is ok', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  expect(closeRoomResponse.status).toEqual(200);

  const closeRoomRequest2 = await authDO.fetch(closeRoomRequest);
  expect(closeRoomRequest2.status).toEqual(200);
});

test('deleteRoom calls into roomDO and marks room deleted', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const deleteRoomPathWithRoomID = AUTH_ROUTES.deleteRoom.replace(
    ':roomID',
    testRoomID,
  );

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

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  expect(closeRoomResponse.status).toEqual(200);

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const deleteRoomResponse = await authDO.fetch(deleteRoomRequest);
  expect(deleteRoomResponse.status).toEqual(200);
  expect(gotDeleteForObjectIDString).not.toBeUndefined();
  expect(gotDeleteForObjectIDString).toEqual('unique-room-do-0');

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Deleted,
  });
});

test('deleteRoom requires room to be closed', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const deleteRoomResponse = await authDO.fetch(deleteRoomRequest);
  expect(deleteRoomResponse.status).toEqual(409);

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Open,
  });
});

test('deleteRoom does not delete if auth api key is incorrect', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const deleteRoomRequest = newDeleteRoomRequest(
    'https://test.roci.dev',
    'SOME OTHER AUTH KEY',
    testRoomID,
  );

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const deleteRoomResponse = await authDO.fetch(deleteRoomRequest);
  expect(deleteRoomResponse.status).toEqual(401);

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Open,
  });
});

test('forget room forgets an existing room', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const forgetRoomRequest = newForgetRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const forgetRoomResponse = await authDO.fetch(forgetRoomRequest);
  expect(forgetRoomResponse.status).toEqual(200);

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Unknown,
  });
});

test('foget room 404s on non-existent room', async () => {
  const {testRoomID, testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  // Note: no createRoom.

  const forgetRoomRequest = newForgetRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const forgetRoomResponse = await authDO.fetch(forgetRoomRequest);
  expect(forgetRoomResponse.status).toEqual(404);
});

test('roomStatusByRoomID returns status for a room that exists', async () => {
  const {testRoomID, testRequest, testRoomDO, state} =
    await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Open,
  });
});

test('roomStatusByRoomID returns unknown for a room that does not exist', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const statusRequest = newRoomStatusRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    'no-such-room',
  );
  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(200);
  expect(await statusResponse.json()).toMatchObject({
    status: RoomStatus.Unknown,
  });
});

test('roomStatusByRoomID requires authApiKey', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const path = AUTH_ROUTES.roomStatusByRoomID.replace(':roomID', 'abc123');
  const statusRequest = new Request(`https://test.roci.dev${path}`, {
    method: 'get',
    // No auth header.
  });

  const statusResponse = await authDO.fetch(statusRequest);
  expect(statusResponse.status).toEqual(401);
});

function newRoomRecordsRequest() {
  return new Request(`https://test.roci.dev${AUTH_ROUTES.roomRecords}`, {
    method: 'get',
    headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
  });
}

test('roomRecords returns empty array if no rooms exist', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const roomRecordsRequest = newRoomRecordsRequest();
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  expect(roomRecordsResponse.status).toEqual(200);
  const gotRecords = await roomRecordsResponse.json();
  expect(gotRecords).toEqual([]);
});

test('roomRecords returns rooms that exists', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, '1');
  await createRoom(authDO, '2');
  await createRoom(authDO, '3');

  const roomRecordsRequest = newRoomRecordsRequest();
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  expect(roomRecordsResponse.status).toEqual(200);
  const gotRecords = await roomRecordsResponse.json();
  expect(gotRecords).toMatchObject([
    {roomID: '1'},
    {roomID: '2'},
    {roomID: '3'},
  ]);
});

test('roomRecords requires authApiKey', async () => {
  const {testRoomDO, state} = await createCreateRoomTestFixture();

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () => Promise.reject('should not be called'),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, '1');

  const roomRecordsRequest = new Request(
    `https://test.roci.dev${AUTH_ROUTES.roomRecords}`,
    {
      method: 'get',
      // No auth header.
    },
  );
  const roomRecordsResponse = await authDO.fetch(roomRecordsRequest);
  expect(roomRecordsResponse.status).toEqual(401);
});

function createConnectTestFixture(
  options: {
    testUserID?: string;
    testRoomID?: string;
    testClientID?: string;
  } = {},
) {
  const {
    testUserID = 'testUserID1',
    testRoomID = 'testRoomID1',
    testClientID = 'testClientID1',
  } = options;
  const encodedTestAuth = 'test%20auth%20token%20value%20%25%20encoded';
  const testAuth = 'test auth token value % encoded';

  const headers = new Headers();
  headers.set('Sec-WebSocket-Protocol', encodedTestAuth);
  headers.set('Upgrade', 'websocket');
  const url = `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}`;
  const testRequest = new Request(url, {
    headers,
  });

  const mocket = new Mocket();

  let numRooms = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: () => {
      throw 'should not be called';
    },
    newUniqueId: () => new TestDurableObjectId('room-do-' + numRooms++),
    get: (id: DurableObjectId) => {
      expect(id.toString()).toEqual('room-do-0');
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        const url = new URL(request.url);
        if (url.pathname === '/createRoom') {
          return new Response();
        }
        expect(request.url).toEqual(testRequest.url);
        expect(request.headers.get(USER_DATA_HEADER_NAME)).toEqual(
          encodeHeaderValue(JSON.stringify({userID: testUserID})),
        );
        expect(request.headers.get('Sec-WebSocket-Protocol')).toEqual(
          encodedTestAuth,
        );
        return new Response(null, {status: 101, webSocket: mocket});
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

test("connect won't connect to a room that doesn't exist", async () => {
  jest.useRealTimers();
  const {testAuth, testUserID, testRoomID, testRequest, testRoomDO} =
    createConnectTestFixture();

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  const logSink = new TestLogSink();
  const [, serverWS] = mockWebSocketPair();
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      return {userID: testUserID};
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
  });
  // Note: no room created.

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(serverWS.log).toEqual([
    ['send', JSON.stringify(['error', ErrorKind.RoomNotFound, 'testRoomID1'])],
    ['close'],
  ]);
  expect((await storage.list({prefix: 'connection/'})).size).toEqual(0);
});

test('connect calls authHandler and sends resolved UserData in header to Room DO', async () => {
  const {
    testAuth,
    testUserID,
    testRoomID,
    testRequest,
    testRoomDO,
    mocket,
    encodedTestAuth,
  } = createConnectTestFixture();

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  const logSink = new TestLogSink();
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      return {userID: testUserID};
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
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
  expect((await storage.list({prefix: 'connection/'})).size).toEqual(1);
  const connectionRecord = (await storage.get(
    'connection/testUserID1/testRoomID1/testClientID1/',
  )) as ConnectionRecord;
  expect(connectionRecord).toBeDefined();
  expect(connectionRecord.connectTimestamp).toEqual(testTime);
});

test('connect wont connect to a room that is closed', async () => {
  jest.useRealTimers();
  const {testUserID, testRoomID, testRequest, testRoomDO} =
    createConnectTestFixture();

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  const logSink = new TestLogSink();
  const [, serverWS] = mockWebSocketPair();
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async () => ({userID: testUserID}),
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const closeRoomRequest = newCloseRoomRequest(
    'https://test.roci.dev',
    TEST_AUTH_API_KEY,
    testRoomID,
  );
  const closeRoomResponse = await authDO.fetch(closeRoomRequest);
  expect(closeRoomResponse.status).toEqual(200);

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(serverWS.log).toEqual([
    ['send', JSON.stringify(['error', ErrorKind.RoomClosed, 'testRoomID1'])],
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

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      return {userID: testUserID};
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
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
  expect((await storage.list({prefix: 'connection/'})).size).toEqual(1);
  const connectionRecord = (await storage.get(
    'connection/%2FtestUserID%2F%3F/testRoomID/%2FtestClientID%2F/',
  )) as ConnectionRecord;
  expect(connectionRecord).toBeDefined();
  expect(connectionRecord.connectTimestamp).toEqual(testTime);
});

test('connect pipes 401 over ws without calling Room DO if authHandler rejects', async () => {
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
  const authDO = new BaseAuthDO({
    roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
    state: {id: authDOID} as DurableObjectState,
    // eslint-disable-next-line require-await
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      throw new Error('Test authHandler reject');
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.headers.get('Sec-WebSocket-Protocol')).toEqual(testAuth);
  expect(response.webSocket).toBe(clientWS);
  expect(serverWS.log).toEqual([
    [
      'send',
      JSON.stringify(['error', ErrorKind.Unauthorized, 'authHandler rejected']),
    ],
    ['close'],
  ]);
});

test('connect pipes 401 over ws without calling Room DO if Sec-WebSocket-Protocol header is not present', async () => {
  const testRoomID = 'testRoomID1';
  const testClientID = 'testClientID1';

  const headers = new Headers();
  headers.set('Upgrade', 'websocket');
  const testRequest = new Request(
    `ws://test.roci.dev/api/sync/v1/connect?roomID=${testRoomID}&clientID=${testClientID}`,
    {
      headers,
    },
  );
  const authDO = new BaseAuthDO({
    roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
    state: {id: authDOID} as DurableObjectState,
    // eslint-disable-next-line require-await
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(401);
  expect(response.webSocket).toBeUndefined();
});

describe('connect sends VersionNotSupported error over ws if path is for unsupported version', () => {
  const t = (path: string) =>
    -test('path: ' + path, async () => {
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
      const authDO = new BaseAuthDO({
        roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
        state: {id: authDOID} as DurableObjectState,
        // eslint-disable-next-line require-await
        authHandler: () =>
          Promise.reject(new Error('Unexpected call to authHandler')),
        authApiKey: TEST_AUTH_API_KEY,
        logSink: new TestLogSink(),
        logLevel: 'debug',
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
            ErrorKind.VersionNotSupported,
            'unsupported version',
          ]),
        ],
        ['close'],
      ]);
    });
  t('/connect');
  t('/api/sync/v0/connect');
  t('/api/sync/v2/connect');
});

test('authInvalidateForUser when requests to roomDOs are successful', async () => {
  const testUserID = 'testUserID1';
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: 'post',
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID1/testClientID2/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID3/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID4/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID3/testClientID5/', {
    connectTimestamp: 1000,
  });
  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests. Plus, we can't get the RoomRecord
        // during the /createRoom call because it hasn't been written yet when /createRoom
        // is called!
        if (isAuthRequest(request)) {
          const roomRecord = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          const {roomID} = roomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          await expectForwardedAuthInvalidateRequest(request, testRequest);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const logSink = new TestLogSink();
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(undefined);
  expect(response.status).toEqual(200);
});

test('authInvalidateForUser when connection ids have chars that need to be percent escaped', async () => {
  const testUserID = '/testUserID/?';
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: 'post',
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put(
    'connection/%2FtestUserID%2F%3F/testRoomID1/%2FtestClientID%2F/',
    {
      connectTimestamp: 1000,
    },
  );
  await storage.put(
    'connection/%2FtestUserID%2F%3F/testRoomID1/%2FtestClientID2%2F/',
    {
      connectTimestamp: 1000,
    },
  );
  await storage.put(
    'connection/%2FtestUserID%2F%3F/testRoomID2/%2FtestClientID%2F/',
    {
      connectTimestamp: 1000,
    },
  );
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests.
        if (isAuthRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          await expectForwardedAuthInvalidateRequest(request, testRequest);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const logSink = new TestLogSink();
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    // eslint-disable-next-line require-await
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(response.status).toEqual(200);
});

test('authInvalidateForUser when any request to roomDOs returns error response', async () => {
  const testUserID = 'testUserID1';
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: 'post',
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) =>
      new TestDurableObjectStub(id, async (request: Request) => {
        // We are only interested in auth requests.
        if (isAuthRequest(request)) {
          const {roomID} = (await getRoomRecordByObjectID(
            storage,
            id,
          )) as RoomRecord;
          roomDORequestCountsByRoomID.set(
            roomID,
            (roomDORequestCountsByRoomID.get(roomID) || 0) + 1,
          );
          await expectForwardedAuthInvalidateRequest(request, testRequest);
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
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink,
    logLevel: 'debug',
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
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: 'post',
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);

  let roomDORequestCount = 0;
  let gotObjectId: DurableObjectId | undefined;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) => {
      gotObjectId = id;
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        if (isAuthRequest(request)) {
          roomDORequestCount++;
          expect(request).toBe(testRequest);
        }
        return new Response('Test Success', {status: 200});
      });
    },
  };
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, testRoomID);

  const response = await authDO.fetch(testRequest);

  const {roomID} = (await getRoomRecordByObjectID(
    storage,
    gotObjectId!,
  )) as RoomRecord;
  expect(roomID).toEqual(testRoomID);
  expect(roomDORequestCount).toEqual(1);
  expect(response.status).toEqual(200);
});

async function createRoom(authDO: BaseAuthDO, roomID: string) {
  const createRoomRequest = newCreateRoomRequest(
    'https://test.roci.dev/',
    TEST_AUTH_API_KEY,
    roomID,
  );
  const resp = await authDO.fetch(createRoomRequest);
  expect(resp.status).toEqual(200);
}

test('authInvalidateForRoom when request to roomDO returns error response', async () => {
  const testRoomID = 'testRoomID1';
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: 'post',
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);

  let roomDORequestCount = 0;
  let gotObjectId: DurableObjectId | undefined;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    get: (id: DurableObjectId) => {
      gotObjectId = id;
      // eslint-disable-next-line require-await
      return new TestDurableObjectStub(id, async (request: Request) => {
        if (isAuthRequest(request)) {
          roomDORequestCount++;
          expect(request).toBe(testRequest);
          return new Response(
            'Test authInvalidateForRoom Internal Server Error Msg',
            {status: 500},
          );
        }
        return new Response('ok', {status: 200});
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
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
    `https://test.roci.dev/api/auth/v0/invalidateAll`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: 'post',
      body: '',
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID1/testClientID2/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put(
    'connection/%2FtestUserID%2F%3F/testRoomID/%2FtestClientID%2F/',
    {
      connectTimestamp: 1000,
    },
  );

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
          await expectForwardedAuthInvalidateRequest(request, testRequest);
        }
        return new Response('Test Success', {status: 200});
      }),
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');
  await createRoom(authDO, 'testRoomID');

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);

  expect(roomDORequestCountsByRoomID.size).toEqual(4);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID')).toEqual(1);
});

async function expectForwardedAuthInvalidateRequest(
  forwardedRequest: Request,
  originalRequest: Request,
) {
  expect(forwardedRequest.url).toEqual(originalRequest.url);
  expect(forwardedRequest.method).toEqual(originalRequest.method);
  expect(forwardedRequest.headers.get(AUTH_API_KEY_HEADER_NAME)).toEqual(
    originalRequest.headers.get(AUTH_API_KEY_HEADER_NAME),
  );
  expect(await forwardedRequest.text()).toEqual(
    await originalRequest.clone().text(),
  );
  expect(forwardedRequest.bodyUsed).toBeTruthy();
}

test('authInvalidateAll when any request to roomDOs returns error response', async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateAll`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: 'post',
      body: '',
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID1/testClientID2/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put(
    'connection/%2FtestUserID%2F%3F/testRoomID/%2FtestClientID%2F/',
    {
      connectTimestamp: 1000,
    },
  );

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
          await expectForwardedAuthInvalidateRequest(request, testRequest);
          return roomID === 'testRoomID2'
            ? new Response('Test authInvalidateAll Internal Server Error Msg', {
                status: 500,
              })
            : new Response('Test Success', {status: 200});
        }
        return new Response('ok', {status: 200});
      }),
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');
  await createRoom(authDO, 'testRoomID');

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(500);

  expect(roomDORequestCountsByRoomID.size).toEqual(4);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID')).toEqual(1);
  expect(await response.text()).toEqual(
    'Test authInvalidateAll Internal Server Error Msg',
  );
});

async function createRevalidateConnectionsTestFixture() {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/revalidateConnections`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: 'post',
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID1/testClientID2/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID3/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });

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
          expect(request.url).toEqual(
            'https://unused-reflect-room-do.dev/api/auth/v0/connections',
          );
          switch (roomID) {
            case 'testRoomID1':
              return new Response(
                JSON.stringify([
                  {userID: 'testUserID1', clientID: 'testClientID1'},
                  {userID: 'testUserID2', clientID: 'testClientID1'},
                ]),
              );
            case 'testRoomID2':
              return new Response(
                JSON.stringify([
                  {userID: 'testUserID1', clientID: 'testClientID1'},
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

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');
  return {authDO, testRequest, roomDORequestCountsByRoomID, storage};
}

test('revalidateConnections', async () => {
  const {authDO, testRequest, roomDORequestCountsByRoomID, storage} =
    await createRevalidateConnectionsTestFixture();

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'connection/'})).keys()]).toEqual([
    'connection/testUserID1/testRoomID1/testClientID1/',
    'connection/testUserID1/testRoomID2/testClientID1/',
    'connection/testUserID2/testRoomID1/testClientID1/',
  ]);
});

test('revalidateConnections continues if one storage delete throws an error', async () => {
  const {authDO, testRequest, roomDORequestCountsByRoomID, storage} =
    await createRevalidateConnectionsTestFixture();

  jest.spyOn(storage, 'delete').mockImplementationOnce(() => {
    throw new Error('test delete error');
  });

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'connection/'})).keys()]).toEqual([
    'connection/testUserID1/testRoomID1/testClientID1/',
    'connection/testUserID1/testRoomID1/testClientID2/',
    'connection/testUserID1/testRoomID2/testClientID1/',
    'connection/testUserID2/testRoomID1/testClientID1/',
  ]);
});

test('revalidateConnections continues if one roomDO returns an error', async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/revalidateConnections`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: 'post',
    },
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put('connection/testUserID1/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID1/testClientID2/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID1/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID1/testRoomID2/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID2/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });
  await storage.put('connection/testUserID3/testRoomID3/testClientID1/', {
    connectTimestamp: 1000,
  });

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
          expect(request.url).toEqual(
            'https://unused-reflect-room-do.dev/api/auth/v0/connections',
          );
          switch (roomID) {
            case 'testRoomID1':
              return new Response(
                'Test revalidateConnections Internal Server Error Msg',
                {
                  status: 500,
                },
              );
            case 'testRoomID2':
              return new Response(
                JSON.stringify([
                  {userID: 'testUserID1', clientID: 'testClientID1'},
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

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: () =>
      Promise.reject(new Error('Unexpected call to authHandler')),
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: 'debug',
  });
  await createRoom(authDO, 'testRoomID1');
  await createRoom(authDO, 'testRoomID2');
  await createRoom(authDO, 'testRoomID3');

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get('testRoomID1')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID2')).toEqual(1);
  expect(roomDORequestCountsByRoomID.get('testRoomID3')).toEqual(1);

  expect([...(await storage.list({prefix: 'connection/'})).keys()]).toEqual([
    'connection/testUserID1/testRoomID1/testClientID1/',
    'connection/testUserID1/testRoomID1/testClientID2/',
    'connection/testUserID1/testRoomID2/testClientID1/',
    'connection/testUserID2/testRoomID1/testClientID1/',
  ]);
});
