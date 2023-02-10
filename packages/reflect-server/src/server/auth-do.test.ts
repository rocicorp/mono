import { jest, afterEach, beforeEach, test, expect } from "@jest/globals";
import { encodeHeaderValue } from "../util/headers.js";
import { Mocket, TestLogSink } from "../util/test-utils.js";
import { USER_DATA_HEADER_NAME } from "./auth.js";
import {
  createTestDurableObjectNamespace,
  TestDurableObjectId,
  TestDurableObjectState,
  TestDurableObjectStub,
} from "./do-test-utils.js";
import { BaseAuthDO, ConnectionRecord } from "./auth-do.js";
import { createAuthAPIHeaders } from "./auth-api-headers.js";

const TEST_AUTH_API_KEY = "TEST_REFLECT_AUTH_API_KEY_TEST";
const { authDO } = getMiniflareBindings();
const authDOID = authDO.idFromName("auth");

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.restoreAllMocks();
});

function createConnectTestFixture(
  options: {
    testUserID?: string;
    testRoomID?: string;
    testClientID?: string;
  } = {}
) {
  const {
    testUserID = "testUserID1",
    testRoomID = "testRoomID1",
    testClientID = "testClientID1",
  } = options;
  const encodedTestAuth = "test%20auth%20token%20value%20%25%20encoded";
  const testAuth = "test auth token value % encoded";

  const headers = new Headers();
  headers.set("Sec-WebSocket-Protocol", encodedTestAuth);
  headers.set("Upgrade", "websocket");
  const testRequest = new Request(
    `ws://test.roci.dev/connect?roomID=${testRoomID}&clientID=${testClientID}`,
    {
      headers,
    }
  );

  const mocket = new Mocket();

  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      expect(name).toEqual(testRoomID);
      return new TestDurableObjectId("room-do-" + name);
    },
    get: (id: DurableObjectId) => {
      expect(id.name).toEqual("room-do-" + testRoomID);
      return new TestDurableObjectStub(id, async (request: Request) => {
        expect(request.url).toEqual(testRequest.url);
        expect(request.headers.get(USER_DATA_HEADER_NAME)).toEqual(
          encodeHeaderValue(JSON.stringify({ userID: testUserID }))
        );
        expect(request.headers.get("Sec-WebSocket-Protocol")).toEqual(
          encodedTestAuth
        );
        return new Response(null, { status: 101, webSocket: mocket });
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
    idFromName: (name: string) => {
      return new TestDurableObjectId("room-do-" + name);
    },
    get: (id: DurableObjectId) => {
      return new TestDurableObjectStub(id, async (request: Request) => {
        throw new Error("Unexpected call to Room DO fetch " + request.url);
      });
    },
  };
}

test("connect calls authHandler and sends resolved UserData in header to Room DO", async () => {
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
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      return { userID: testUserID };
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const testTime = 1010101;
  jest.setSystemTime(testTime);
  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.webSocket).toBe(mocket);
  expect(response.headers.get("Sec-WebSocket-Protocol")).toEqual(
    encodedTestAuth
  );
  expect((await storage.list()).size).toEqual(1);
  const connectionRecord = (await storage.get(
    "connection/testUserID1/testRoomID1/testClientID1/"
  )) as ConnectionRecord;
  expect(connectionRecord).toBeDefined();
  expect(connectionRecord.connectTimestamp).toEqual(testTime);
});

test("connect percent escapes components of the connection key", async () => {
  const {
    testAuth,
    testUserID,
    testRoomID,
    testRequest,
    testRoomDO,
    mocket,
    encodedTestAuth,
  } = createConnectTestFixture({
    testUserID: "/testUserID/?",
    testRoomID: "/testRoomID/=",
    testClientID: "/testClientID/&",
  });

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      return { userID: testUserID };
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const testTime = 1010101;
  jest.setSystemTime(testTime);
  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.webSocket).toBe(mocket);
  expect(response.headers.get("Sec-WebSocket-Protocol")).toEqual(
    encodedTestAuth
  );
  expect((await storage.list()).size).toEqual(1);
  const connectionRecord = (await storage.get(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID%2F%3D/%2FtestClientID%2F/"
  )) as ConnectionRecord;
  expect(connectionRecord).toBeDefined();
  expect(connectionRecord.connectTimestamp).toEqual(testTime);
});

test("connect pipes 401 over ws without calling Room DO if authHandler rejects", async () => {
  const testRoomID = "testRoomID1";
  const testClientID = "testClientID1";
  const testAuth = "testAuthTokenValue";

  const headers = new Headers();
  headers.set("Sec-WebSocket-Protocol", testAuth);
  headers.set("Upgrade", "websocket");
  const testRequest = new Request(
    `ws://test.roci.dev/connect?roomID=${testRoomID}&clientID=${testClientID}`,
    {
      headers,
    }
  );
  const clientWs = new Mocket();
  const serverWs = new Mocket();
  const authDO = new BaseAuthDO({
    roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
    state: { id: authDOID } as DurableObjectState,
    authHandler: async (auth, roomID) => {
      expect(auth).toEqual(testAuth);
      expect(roomID).toEqual(testRoomID);
      throw new Error("Test authHandler reject");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
    newWebSocketPair: () => {
      return [clientWs, serverWs];
    },
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(101);
  expect(response.headers.get("Sec-WebSocket-Protocol")).toEqual(testAuth);
  expect(response.webSocket).toBe(clientWs);
  expect(serverWs.log).toEqual([
    ["send", '["error","401: authHandler rejected"]'],
    ["close"],
  ]);
});

test("connect pipes 401 over ws without calling Room DO if Sec-WebSocket-Protocol header is not present", async () => {
  const testRoomID = "testRoomID1";
  const testClientID = "testClientID1";

  const headers = new Headers();
  headers.set("Upgrade", "websocket");
  const testRequest = new Request(
    `ws://test.roci.dev/connect?roomID=${testRoomID}&clientID=${testClientID}`,
    {
      headers,
    }
  );
  const authDO = new BaseAuthDO({
    roomDO: createRoomDOThatThrowsIfFetchIsCalled(),
    state: { id: authDOID } as DurableObjectState,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(response.status).toEqual(401);
  expect(response.webSocket).toBeUndefined();
});

test("authInvalidateForUser when requests to roomDOs are successful", async () => {
  const testUserID = "testUserID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID1/testClientID2/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request).toBe(testRequest);
        return new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(response.status).toEqual(200);
});

test("authInvalidateForUser when connection ids have chars that need to be percent escaped", async () => {
  const testUserID = "/testUserID/?";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID1%2F%3D/%2FtestClientID%2F/",
    {
      connectTimestamp: 1000,
    }
  );
  await storage.put(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID1%2F%3D/%2FtestClientID2%2F/",
    {
      connectTimestamp: 1000,
    }
  );
  await storage.put(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID2%2F%3D/%2FtestClientID%2F/",
    {
      connectTimestamp: 1000,
    }
  );
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request).toBe(testRequest);
        return new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(2);
  expect(roomDORequestCountsByRoomID.get("/testRoomID1/=")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("/testRoomID2/=")).toEqual(1);
  expect(response.status).toEqual(200);
});

test("authInvalidateForUser when any request to roomDOs returns error response", async () => {
  const testUserID = "testUserID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        userID: testUserID,
      }),
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request).toBe(testRequest);
        return roomID === "testRoomID2"
          ? new Response(
              "Test authInvalidateForUser Internal Server Error Msg",
              { status: 500 }
            )
          : new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(3);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);
  expect(response.status).toEqual(500);
  expect(await response.text()).toEqual(
    "Test authInvalidateForUser Internal Server Error Msg"
  );
});

test("authInvalidateForRoom when request to roomDO is successful", async () => {
  const testRoomID = "testRoomID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    }
  );

  let roomDORequestCount = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      expect(name).toEqual(testRoomID);
      return new TestDurableObjectId("room-do-" + name);
    },
    get: (id: DurableObjectId) => {
      expect(id.name).toEqual("room-do-" + testRoomID);
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCount++;
        expect(request).toBe(testRequest);
        return new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state: { id: authDOID } as DurableObjectState,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCount).toEqual(1);
  expect(response.status).toEqual(200);
});

test("authInvalidateForRoom when request to roomDO returns error response", async () => {
  const testRoomID = "testRoomID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    }
  );

  let roomDORequestCount = 0;
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      expect(name).toEqual(testRoomID);
      return new TestDurableObjectId("room-do-" + name);
    },
    get: (id: DurableObjectId) => {
      expect(id.name).toEqual("room-do-" + testRoomID);
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCount++;
        expect(request).toBe(testRequest);
        return new Response(
          "Test authInvalidateForRoom Internal Server Error Msg",
          { status: 500 }
        );
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state: { id: authDOID } as DurableObjectState,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCount).toEqual(1);
  expect(response.status).toEqual(500);
  expect(await response.text()).toEqual(
    "Test authInvalidateForRoom Internal Server Error Msg"
  );
});

test("authInvalidateAll when requests to roomDOs are successful", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateAll`,
    { headers: createAuthAPIHeaders(TEST_AUTH_API_KEY), method: "post" }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID1/testClientID2/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID%2F%3D/%2FtestClientID%2F/",
    {
      connectTimestamp: 1000,
    }
  );

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request).toBe(testRequest);
        return new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(4);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("/testRoomID/=")).toEqual(1);
  expect(response.status).toEqual(200);
});

test("authInvalidateAll when any request to roomDOs returns error response", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateAll`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: "post",
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID1/testClientID2/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put(
    "connection/%2FtestUserID%2F%3F/%2FtestRoomID%2F%3D/%2FtestClientID%2F/",
    {
      connectTimestamp: 1000,
    }
  );

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request).toBe(testRequest);
        return roomID === "testRoomID2"
          ? new Response("Test authInvalidateAll Internal Server Error Msg", {
              status: 500,
            })
          : new Response("Test Success", { status: 200 });
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);

  expect(roomDORequestCountsByRoomID.size).toEqual(4);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("/testRoomID/=")).toEqual(1);
  expect(response.status).toEqual(500);
  expect(await response.text()).toEqual(
    "Test authInvalidateAll Internal Server Error Msg"
  );
});

async function createRevalidateConnectionsTestFixture() {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/revalidateConnections`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: "post",
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID1/testClientID2/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID3/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request.url).toEqual(
          "https://unused-reflect-room-do.dev/api/auth/v0/connections"
        );
        switch (roomID) {
          case "testRoomID1":
            return new Response(
              JSON.stringify([
                { userID: "testUserID1", clientID: "testClientID1" },
                { userID: "testUserID2", clientID: "testClientID1" },
              ])
            );
          case "testRoomID2":
            return new Response(
              JSON.stringify([
                { userID: "testUserID1", clientID: "testClientID1" },
              ])
            );
          case "testRoomID3":
            return new Response(JSON.stringify([]));
          default:
            throw new Error(`Unexpected roomID ${roomID}`);
        }
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });
  return { authDO, testRequest, roomDORequestCountsByRoomID, storage };
}

test("revalidateConnections", async () => {
  const { authDO, testRequest, roomDORequestCountsByRoomID, storage } =
    await createRevalidateConnectionsTestFixture();

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);

  expect([...(await storage.list({ prefix: "connection/" })).keys()]).toEqual([
    "connection/testUserID1/testRoomID1/testClientID1/",
    "connection/testUserID1/testRoomID2/testClientID1/",
    "connection/testUserID2/testRoomID1/testClientID1/",
  ]);
});

test("revalidateConnections continues if one storage delete throws an error", async () => {
  const { authDO, testRequest, roomDORequestCountsByRoomID, storage } =
    await createRevalidateConnectionsTestFixture();

  jest.spyOn(storage, "delete").mockImplementationOnce(() => {
    throw new Error("test delete error");
  });

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);

  expect([...(await storage.list({ prefix: "connection/" })).keys()]).toEqual([
    "connection/testUserID1/testRoomID1/testClientID1/",
    "connection/testUserID1/testRoomID1/testClientID2/",
    "connection/testUserID1/testRoomID2/testClientID1/",
    "connection/testUserID2/testRoomID1/testClientID1/",
  ]);
});

test("revalidateConnections continues if one roomDO returns an error", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/revalidateConnections`,
    {
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      method: "post",
    }
  );

  const storage = await getMiniflareDurableObjectStorage(authDOID);
  const state = new TestDurableObjectState(authDOID, storage);
  await storage.put("connection/testUserID1/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID1/testClientID2/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID1/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID1/testRoomID2/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID2/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });
  await storage.put("connection/testUserID3/testRoomID3/testClientID1/", {
    connectTimestamp: 1000,
  });

  const roomDORequestCountsByRoomID = new Map();
  const testRoomDO: DurableObjectNamespace = {
    ...createTestDurableObjectNamespace(),
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    get: (id: DurableObjectId) => {
      const { name: roomID } = id;
      return new TestDurableObjectStub(id, async (request: Request) => {
        roomDORequestCountsByRoomID.set(
          roomID,
          (roomDORequestCountsByRoomID.get(roomID) || 0) + 1
        );
        expect(request.url).toEqual(
          "https://unused-reflect-room-do.dev/api/auth/v0/connections"
        );
        switch (roomID) {
          case "testRoomID1":
            return new Response(
              "Test revalidateConnections Internal Server Error Msg",
              {
                status: 500,
              }
            );
          case "testRoomID2":
            return new Response(
              JSON.stringify([
                { userID: "testUserID1", clientID: "testClientID1" },
              ])
            );
          case "testRoomID3":
            return new Response(JSON.stringify([]));
          default:
            throw new Error(`Unexpected roomID ${roomID}`);
        }
      });
    },
  };

  const authDO = new BaseAuthDO({
    roomDO: testRoomDO,
    state,
    authHandler: async (_auth, _roomID) => {
      throw new Error("Unexpected call to authHandler");
    },
    authApiKey: TEST_AUTH_API_KEY,
    logSink: new TestLogSink(),
    logLevel: "debug",
  });

  const response = await authDO.fetch(testRequest);
  expect(response.status).toEqual(200);
  expect(roomDORequestCountsByRoomID.get("testRoomID1")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID2")).toEqual(1);
  expect(roomDORequestCountsByRoomID.get("testRoomID3")).toEqual(1);

  expect([...(await storage.list({ prefix: "connection/" })).keys()]).toEqual([
    "connection/testUserID1/testRoomID1/testClientID1/",
    "connection/testUserID1/testRoomID1/testClientID2/",
    "connection/testUserID1/testRoomID2/testClientID1/",
    "connection/testUserID2/testRoomID1/testClientID1/",
  ]);
});
