import {test, expect} from '@jest/globals';
import type {LogLevel} from '@rocicorp/logger';
import {Mocket, TestLogSink} from '../util/test-utils.js';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {AUTH_ROUTES} from './auth-do.js';
import {
  createTestDurableObjectNamespace,
  TestDurableObjectId,
  TestDurableObjectStub,
} from './do-test-utils.js';
import {BaseWorkerEnv, createWorker} from './worker.js';

const TEST_AUTH_API_KEY = 'TEST_REFLECT_AUTH_API_KEY_TEST';

class TestExecutionContext implements ExecutionContext {
  waitUntil(_promise: Promise<unknown>): void {
    return;
  }
  passThroughOnException(): void {
    return;
  }
}

function createTestFixture(
  createTestResponse: (req: Request) => Response = () =>
    new Response('success', {status: 200}),
  authApiKeyDefined = true,
) {
  const authDORequests: {req: Request; resp: Response}[] = [];

  const testEnv: BaseWorkerEnv = {
    authDO: {
      ...createTestDurableObjectNamespace(),
      idFromName: (name: string) => {
        expect(name).toEqual('auth');
        return new TestDurableObjectId('test-auth-do-id', 'test-auth-do-id');
      },
      get: (id: DurableObjectId) => {
        expect(id.name).toEqual('test-auth-do-id');
        // eslint-disable-next-line require-await
        return new TestDurableObjectStub(id, async (request: Request) => {
          const testResponse = createTestResponse(request);
          authDORequests.push({req: request, resp: testResponse});
          return testResponse;
        });
      },
    },
    // eslint-disable-next-line @typescript-eslint/naming-convention
    REFLECT_AUTH_API_KEY: authApiKeyDefined ? TEST_AUTH_API_KEY : undefined,
  };

  return {
    testEnv,
    authDORequests,
  };
}

async function testForwardedToAuthDO(
  testRequest: Request,
  testResponse = new Response('success', {status: 200}),
  expectAuthDOCalled = true,
) {
  // Don't clone response if it has a websocket, otherwise CloudFlare's Response
  // class will throw
  // "TypeError: Cannot clone a response to a WebSocket handshake."
  const testResponseClone = testResponse.webSocket
    ? undefined
    : testResponse.clone();
  const {testEnv, authDORequests} = createTestFixture(() => testResponse);
  const worker = createWorker({
    getLogSink: _env => new TestLogSink(),
    getLogLevel: _env => 'error',
  });
  if (!worker.fetch) {
    throw new Error('Expect fetch to be defined');
  }
  const response = await worker.fetch(
    testRequest,
    testEnv,
    new TestExecutionContext(),
  );
  if (expectAuthDOCalled) {
    expect(authDORequests.length).toEqual(1);
    expect(authDORequests[0].req).toBe(testRequest);
    expect(authDORequests[0].resp).toBe(testResponse);
    expect(response.status).toEqual(testResponse.status);
    if (testResponseClone) {
      expect(await response.text()).toEqual(await testResponseClone.text());
    }
    const responseHeaders = [...response.headers.entries()];
    const expectedResponseHeaders = [
      ...testResponse.headers.entries(),
      ['access-control-allow-origin', '*'],
    ];
    expect(responseHeaders.length).toEqual(expectedResponseHeaders.length);
    expect(responseHeaders).toEqual(
      expect.arrayContaining(expectedResponseHeaders),
    );
    expect(response.webSocket).toBe(testResponse.webSocket);
  } else {
    expect(authDORequests.length).toEqual(0);
  }
  expect(response.headers.get('Access-Control-Allow-Origin')).toEqual('*');
}

test('worker forwards connect requests to authDO', async () => {
  await testForwardedToAuthDO(
    new Request('ws://test.roci.dev/connect'),
    new Response(null, {
      status: 101,
      webSocket: new Mocket(),
    }),
  );
});

test('worker forwards authDO api requests to authDO', async () => {
  const roomStatusByRoomIDPathWithRoomID =
    AUTH_ROUTES.roomStatusByRoomID.replace(':roomID', 'ae4565');
  type TestCase = {
    path: string;
    method: string;
    body: undefined | Record<string, unknown>;
  };
  const closeRoomPathWithRoomID = AUTH_ROUTES.closeRoom.replace(
    ':roomID',
    'ae4565',
  );
  const deleteRoomPathWithRoomID = AUTH_ROUTES.roomRecords.replace(
    ':roomID',
    'ae4565',
  );
  const forgetRoomPathWithRoomID = AUTH_ROUTES.forgetRoom.replace(
    ':roomID',
    'ae4565',
  );
  const migrateRoomPathWithRoomID = AUTH_ROUTES.migrateRoom.replace(
    ':roomID',
    'ae4565',
  );
  const testCases: TestCase[] = [
    // Auth API calls.
    {
      path: 'https://test.roci.dev/api/auth/v0/invalidateForUser',
      method: 'post',
      body: {userID: 'userID1'},
    },
    {
      path: 'https://test.roci.dev/api/auth/v0/invalidateForRoom',
      method: 'post',
      body: {roomID: 'roomID1'},
    },
    {
      path: 'https://test.roci.dev/api/auth/v0/invalidateAll',
      method: 'post',
      body: undefined,
    },

    // Room API calls.
    {
      path: `https://test.roci.dev${roomStatusByRoomIDPathWithRoomID}`,
      method: 'get',
      body: undefined,
    },
    {
      path: `https://test.roci.dev${AUTH_ROUTES.roomRecords}`,
      method: 'get',
      body: undefined,
    },
    {
      path: `https://test.roci.dev${closeRoomPathWithRoomID}`,
      method: 'post',
      body: undefined,
    },
    {
      path: `https://test.roci.dev${deleteRoomPathWithRoomID}`,
      method: 'post',
      body: undefined,
    },
    {
      path: `https://test.roci.dev${forgetRoomPathWithRoomID}`,
      method: 'post',
      body: undefined,
    },
    {
      path: `https://test.roci.dev${migrateRoomPathWithRoomID}`,
      method: 'post',
      body: undefined,
    },
  ];
  for (const tc of testCases) {
    await testForwarding(tc);
  }

  async function testForwarding(tc: TestCase) {
    await testForwardedToAuthDO(
      new Request(tc.path, {
        method: tc.method,
        headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
        body: tc.body ? JSON.stringify(tc.body) : undefined,
      }),
    );
    await testForwardedToAuthDO(
      new Request(tc.path, {
        method: tc.path,
        // Note: no auth header.
        body: tc.body ? JSON.stringify(tc.body) : undefined,
      }),
      new Response(null, {
        status: 200,
      }),
      false, // Expect authDO not called.
    );
  }
});

test('on scheduled event sends api/auth/v0/revalidateConnections to AuthDO when REFLECT_AUTH_API_KEY is defined', async () => {
  const worker = createWorker({
    getLogSink: _env => new TestLogSink(),
    getLogLevel: _env => 'error',
  });

  const {testEnv, authDORequests} = createTestFixture();

  if (!worker.scheduled) {
    throw new Error('Expect scheduled to be defined');
  }
  await worker.scheduled(
    {scheduledTime: 100, cron: '', noRetry: () => undefined},
    testEnv,
    new TestExecutionContext(),
  );
  expect(authDORequests.length).toEqual(1);
  const {req} = authDORequests[0];
  expect(req.method).toEqual('POST');
  expect(req.url).toEqual(
    'https://unused-reflect-auth-do.dev/api/auth/v0/revalidateConnections',
  );
  expect(req.headers.get('x-reflect-auth-api-key')).toEqual(TEST_AUTH_API_KEY);
});

test('on scheduled event does not send api/auth/v0/revalidateConnections to AuthDO when REFLECT_AUTH_API_KEY is undefined', async () => {
  const worker = createWorker({
    getLogSink: _env => new TestLogSink(),
    getLogLevel: _env => 'error',
  });

  const {testEnv, authDORequests} = createTestFixture(undefined, false);

  if (!worker.scheduled) {
    throw new Error('Expect scheduled to be defined');
  }
  await worker.scheduled(
    {scheduledTime: 100, cron: '', noRetry: () => undefined},
    testEnv,
    new TestExecutionContext(),
  );
  expect(authDORequests.length).toEqual(0);
});

async function testLogging(
  fn: (
    worker: ExportedHandler<BaseWorkerEnv>,
    testEnv: BaseWorkerEnv,
    testExecutionContext: ExecutionContext,
  ) => Promise<unknown>,
) {
  const {testEnv} = createTestFixture();

  const waitUntilCalls: Promise<unknown>[] = [];
  const testExecutionContext = {
    waitUntil: (promise: Promise<unknown>): void => {
      waitUntilCalls.push(promise);
      return;
    },
    passThroughOnException: () => undefined,
  };

  let getLogSinkCallCount = 0;
  let getLogLevelCallCount = 0;
  let logCallCount = 0;
  const logFlushPromise = Promise.resolve();
  const worker = createWorker({
    getLogSink: env => {
      getLogSinkCallCount++;
      expect(env).toBe(testEnv);
      return {
        log: (_level: LogLevel, ..._args: unknown[]): void => {
          logCallCount++;
        },
        flush: (): Promise<void> => logFlushPromise,
      };
    },
    getLogLevel: env => {
      getLogLevelCallCount++;
      expect(env).toBe(testEnv);
      return 'debug';
    },
  });

  expect(getLogSinkCallCount).toEqual(0);
  expect(getLogLevelCallCount).toEqual(0);
  expect(logCallCount).toEqual(0);

  await fn(worker, testEnv, testExecutionContext);

  expect(getLogSinkCallCount).toEqual(1);
  expect(getLogLevelCallCount).toEqual(1);
  const logCallCountAfterFirstFetch = logCallCount;
  expect(logCallCountAfterFirstFetch).toBeGreaterThan(0);
  expect(waitUntilCalls.length).toBe(1);
  expect(waitUntilCalls[0]).toBe(logFlushPromise);

  await fn(worker, testEnv, testExecutionContext);

  expect(getLogSinkCallCount).toEqual(2);
  expect(getLogLevelCallCount).toEqual(2);
  expect(logCallCount).toBeGreaterThan(logCallCountAfterFirstFetch);
  expect(waitUntilCalls.length).toBe(2);
  expect(waitUntilCalls[1]).toBe(logFlushPromise);
}

test('fetch logging', async () => {
  // eslint-disable-next-line require-await
  await testLogging(async (worker, testEnv, testExecutionContext) => {
    const testRequest = new Request('ws://test.roci.dev/connect');
    if (!worker.fetch) {
      throw new Error('Expected fetch to be defined');
    }
    return worker.fetch(testRequest, testEnv, testExecutionContext);
  });
});

test('scheduled logging', async () => {
  // eslint-disable-next-line require-await
  await testLogging(async (worker, testEnv, testExecutionContext) => {
    if (!worker.scheduled) {
      throw new Error('Expected scheduled to be defined');
    }
    return worker.scheduled(
      {scheduledTime: 100, cron: '', noRetry: () => undefined},
      testEnv,
      testExecutionContext,
    );
  });
});

test('preflight request handling allows all origins, paths, methods and headers', async () => {
  await testPreflightRequest({
    origin: 'http://example.com',
    url: 'https://worker.com/pull',
    accessControlRequestHeaders: '',
    accessControlRequestMethod: 'POST',
  });

  await testPreflightRequest({
    origin: 'http://example.com',
    url: 'https://worker.com/pull',
    accessControlRequestHeaders: '',
    accessControlRequestMethod: 'GET',
  });

  await testPreflightRequest({
    origin: 'http://example.com',
    url: 'https://worker.com/pull',
    accessControlRequestHeaders: 'x-request-id, x-auth, other-header',
    accessControlRequestMethod: 'POST',
  });

  await testPreflightRequest({
    origin: 'http://example.com',
    url: 'https://worker.com/connect',
    accessControlRequestHeaders: 'Upgrade, Sec-WebSocket-Protocol',
    accessControlRequestMethod: 'POST',
  });

  await testPreflightRequest({
    origin: 'https://google.com',
    url: 'https://worker.com/anything',
    accessControlRequestHeaders: '',
    accessControlRequestMethod: 'GET',
  });

  await testPreflightRequest({
    origin: 'https://google.com',
    url: 'https://worker.com/anything',
    accessControlRequestHeaders: '',
    accessControlRequestMethod: 'HEAD',
  });
});

async function testPreflightRequest({
  origin,
  url,
  accessControlRequestHeaders,
  accessControlRequestMethod,
}: {
  origin: string;
  url: string;
  accessControlRequestHeaders: string;
  accessControlRequestMethod: string;
}) {
  const {testEnv, authDORequests} = createTestFixture();
  const worker = createWorker({
    getLogSink: _env => new TestLogSink(),
    getLogLevel: _env => 'error',
  });
  if (!worker.fetch) {
    throw new Error('Expect fetch to be defined');
  }
  const headers = new Headers();
  headers.set('Origin', origin);
  headers.set('Access-Control-Request-Method', accessControlRequestMethod);
  headers.set('Access-Control-Request-Headers', accessControlRequestHeaders);
  const response = await worker.fetch(
    new Request(url, {
      method: 'OPTIONS',
      headers,
    }),
    testEnv,
    new TestExecutionContext(),
  );
  expect(authDORequests.length).toEqual(0);
  expect(response.status).toEqual(200);
  expect(response.headers.get('Access-Control-Allow-Origin')).toEqual('*');
  expect(response.headers.get('Access-Control-Allow-Methods')).toEqual(
    'GET,HEAD,POST,OPTIONS',
  );
  expect(response.headers.get('Access-Control-Max-Age')).toEqual('86400');
  expect(response.headers.get('Access-Control-Allow-Headers')).toEqual(
    accessControlRequestHeaders,
  );
}
