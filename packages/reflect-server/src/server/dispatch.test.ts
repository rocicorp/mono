import { test, expect } from "@jest/globals";
import type {
  InvalidateForRoomRequest,
  InvalidateForUserRequest,
} from "../protocol/api/auth";
import type { LogContext } from "@rocicorp/logger";
import { createAuthAPIHeaders } from "./auth-api-headers";
import { dispatch, Handlers } from "./dispatch";
import { createSilentLogContext } from "../util/test-utils";

const testAuthApiKey = "TEST_REFLECT_AUTH_API_KEY_TEST";

function createThrowingHandlers() {
  return {
    connect: () => {
      throw new Error("unexpect call to connect handler");
    },
    authInvalidateForUser: () => {
      throw new Error("unexpect call to authInvalidateForUser handler");
    },
    authInvalidateForRoom: () => {
      throw new Error("unexpect call to authInvalidateForRoom handler");
    },
    authInvalidateAll: () => {
      throw new Error("unexpect call to authInvalidateAll handler");
    },
    authConnections: () => {
      throw new Error("unexpect call to authInvalidateAll handler");
    },
    authRevalidateConnections: () => {
      throw new Error("unexpect call to authInvalidateAll handler");
    },
  };
}

async function testMethodNotAllowedValidationError(
  testRequestBadMethod: Request,
  allowedMethod: string
) {
  const responseForBadMethod = await dispatch(
    testRequestBadMethod,
    createSilentLogContext(),
    testAuthApiKey,
    createThrowingHandlers()
  );
  expect(responseForBadMethod.status).toEqual(405);
  expect(await responseForBadMethod.text()).toEqual(
    `Method not allowed. Use "${allowedMethod}".`
  );
}

async function testApiKeyValidationErrors(baseRequest: Request) {
  const testRequestMissingAuthApiKey = baseRequest.clone();
  const responseForMissingAuthApiKey = await dispatch(
    testRequestMissingAuthApiKey,
    createSilentLogContext(),
    testAuthApiKey,
    createThrowingHandlers()
  );
  expect(responseForMissingAuthApiKey.status).toEqual(401);
  expect(await responseForMissingAuthApiKey.text()).toEqual("Unauthorized");

  const testRequestWrongAuthApiKey = new Request(baseRequest, {
    headers: createAuthAPIHeaders("WRONG_API_KEY"),
  });
  const responseForWrongAuthApiKey = await dispatch(
    testRequestWrongAuthApiKey,
    createSilentLogContext(),
    testAuthApiKey,
    createThrowingHandlers()
  );
  expect(responseForWrongAuthApiKey.status).toEqual(401);
  expect(await responseForWrongAuthApiKey.text()).toEqual("Unauthorized");
}

async function testUnsupportedPathValidationError(
  requestWUnsupportedPath: Request,
  handlers: Handlers
) {
  const response = await dispatch(
    requestWUnsupportedPath,
    createSilentLogContext(),
    undefined,
    handlers
  );
  expect(response.status).toEqual(400);
  expect(await response.text()).toEqual("Unsupported path.");
}

test("unsupported path", async () => {
  await testUnsupportedPathValidationError(
    new Request("https://test.roci.dev/bad_path"),
    createThrowingHandlers()
  );
});

test("unsupported path for optional handlers", async () => {
  const handlers: Handlers = createThrowingHandlers();
  delete handlers.authRevalidateConnections;
  delete handlers.authConnections;
  await testUnsupportedPathValidationError(
    new Request("https://test.roci.dev/api/auth/v0/reavalidateConnections"),
    handlers
  );
  await testUnsupportedPathValidationError(
    new Request("https://test.roci.dev/api/auth/v0/connections"),
    handlers
  );
});

test("connect good request", async () => {
  const testRequest = new Request("ws://test.roci.dev/connect");
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    undefined,
    {
      ...createThrowingHandlers(),
      connect: (_lc: LogContext, request: Request, body: undefined) => {
        expect(request).toBe(testRequest);
        expect(body).toBeUndefined();
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("connect request with validation errors", async () => {
  await testMethodNotAllowedValidationError(
    new Request("ws://test.roci.dev/connect", {
      method: "post",
    }),
    "get"
  );
});

test("authInvalidateForUser good request", async () => {
  const testUserID = "testUserID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        userID: testUserID,
      }),
    }
  );
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    testAuthApiKey,
    {
      ...createThrowingHandlers(),
      authInvalidateForUser: (
        _lc: LogContext,
        request: Request,
        body: InvalidateForUserRequest
      ) => {
        expect(request).toBe(testRequest);
        expect(body).toEqual({
          userID: testUserID,
        });
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("authInvalidateForUser request with validation errors", async () => {
  const testUserID = "testUserID1";
  await testMethodNotAllowedValidationError(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForUser`, {
      method: "put",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        userID: testUserID,
      }),
    }),
    "post"
  );

  await testApiKeyValidationErrors(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForUser`, {
      method: "post",
      body: JSON.stringify({
        userID: testUserID,
      }),
    })
  );

  const testRequestBadBody = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForUser`,
    {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        roomID: testUserID,
      }),
    }
  );
  const responseForBadBody = await dispatch(
    testRequestBadBody,
    createSilentLogContext(),
    testAuthApiKey,
    createThrowingHandlers()
  );
  expect(responseForBadBody.status).toEqual(400);
  expect(await responseForBadBody.text()).toEqual(
    "Body schema error. At path: userID -- Expected a string, but received: undefined"
  );
});

test("authInvalidateForRoom good request", async () => {
  const testRoomID = "testRoomID1";
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    }
  );
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    testAuthApiKey,
    {
      ...createThrowingHandlers(),
      authInvalidateForRoom: (
        _lc: LogContext,
        request: Request,
        body: InvalidateForRoomRequest
      ) => {
        expect(request).toBe(testRequest);
        expect(body).toEqual({
          roomID: testRoomID,
        });
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("authInvalidateForRoom request with validation errors", async () => {
  const testRoomID = "testRoomID1";
  await testMethodNotAllowedValidationError(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForRoom`, {
      method: "put",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    }),
    "post"
  );

  const testRequestBadBody = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateForRoom`,
    {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        userID: testRoomID,
      }),
    }
  );
  const responseForBadBody = await dispatch(
    testRequestBadBody,
    createSilentLogContext(),
    testAuthApiKey,
    createThrowingHandlers()
  );
  expect(responseForBadBody.status).toEqual(400);
  expect(await responseForBadBody.text()).toEqual(
    "Body schema error. At path: roomID -- Expected a string, but received: undefined"
  );

  await testApiKeyValidationErrors(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForRoom`, {
      method: "post",
      body: JSON.stringify({
        roomID: testRoomID,
      }),
    })
  );
});

test("authInvalidateAll good request", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/invalidateAll`,
    {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "post",
    }
  );
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    testAuthApiKey,
    {
      ...createThrowingHandlers(),
      authInvalidateAll: (
        _lc: LogContext,
        request: Request,
        body: undefined
      ) => {
        expect(request).toBe(testRequest);
        expect(body).toBeUndefined();
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("authInvalidateAll request with validation errors", async () => {
  await testMethodNotAllowedValidationError(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateAll`, {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "put",
    }),
    "post"
  );

  await testApiKeyValidationErrors(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateAll`, {
      method: "post",
    })
  );
});

test("authRevalidateConnections good request", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/revalidateConnections`,
    {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "post",
    }
  );
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    testAuthApiKey,
    {
      ...createThrowingHandlers(),
      authRevalidateConnections: (
        _lc: LogContext,
        request: Request,
        body: undefined
      ) => {
        expect(request).toBe(testRequest);
        expect(body).toBeUndefined();
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("authRevalidateConnections request with validation errors", async () => {
  await testMethodNotAllowedValidationError(
    new Request(`https://test.roci.dev/api/auth/v0/revalidateConnections`, {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "put",
    }),
    "post"
  );

  await testApiKeyValidationErrors(
    new Request(`https://test.roci.dev/api/auth/v0/revalidateConnections`, {
      method: "post",
    })
  );
});

test("authConnections good request", async () => {
  const testRequest = new Request(
    `https://test.roci.dev/api/auth/v0/connections`,
    {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "get",
    }
  );
  const testResponse = new Response("");
  const response = await dispatch(
    testRequest,
    createSilentLogContext(),
    testAuthApiKey,
    {
      ...createThrowingHandlers(),
      authConnections: (_lc: LogContext, request: Request, body: undefined) => {
        expect(request).toBe(testRequest);
        expect(body).toBeUndefined();
        return Promise.resolve(testResponse);
      },
    }
  );
  expect(response).toBe(testResponse);
});

test("authConnections request with validation errors", async () => {
  await testMethodNotAllowedValidationError(
    new Request(`https://test.roci.dev/api/auth/v0/connections`, {
      headers: createAuthAPIHeaders(testAuthApiKey),
      method: "post",
    }),
    "get"
  );
  await testApiKeyValidationErrors(
    new Request(`https://test.roci.dev/api/auth/v0/connections`, {
      method: "get",
    })
  );
});

test("auth api returns 401 for all requests when authApiKey is undefined", async () => {
  async function testUnauthorizedWhenAuthApiKeyIsUndefined(request: Request) {
    const response = await dispatch(
      request,
      createSilentLogContext(),
      undefined,
      createThrowingHandlers()
    );
    expect(response.status).toEqual(401);
    expect(await response.text()).toEqual("Unauthorized");
  }
  await testUnauthorizedWhenAuthApiKeyIsUndefined(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForUser`, {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        userID: "testUserID1",
      }),
    })
  );
  await testUnauthorizedWhenAuthApiKeyIsUndefined(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateForRoom`, {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
      body: JSON.stringify({
        roomID: "testRoomID1",
      }),
    })
  );
  await testUnauthorizedWhenAuthApiKeyIsUndefined(
    new Request(`https://test.roci.dev/api/auth/v0/invalidateAll`, {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
    })
  );
  await testUnauthorizedWhenAuthApiKeyIsUndefined(
    new Request(`https://test.roci.dev/api/auth/v0/revalidateConnections`, {
      method: "post",
      headers: createAuthAPIHeaders(testAuthApiKey),
    })
  );
  await testUnauthorizedWhenAuthApiKeyIsUndefined(
    new Request(`https://test.roci.dev/api/auth/v0/connections`, {
      method: "get",
      headers: createAuthAPIHeaders(testAuthApiKey),
    })
  );
});
