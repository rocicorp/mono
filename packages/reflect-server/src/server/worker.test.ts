import {describe, expect, jest, test} from '@jest/globals';
import type {LogLevel} from '@rocicorp/logger';
import {version} from 'reflect-shared';
import {assertString} from 'shared/src/asserts.js';
import type {Series} from '../types/report-metrics.js';
import {Mocket, TestLogSink, fail} from '../util/test-utils.js';
import {createAPIHeaders} from './api-headers.js';
import {AUTH_ROUTES} from './auth-do.js';
import {
  TestDurableObjectId,
  TestDurableObjectStub,
  TestExecutionContext,
  createTestDurableObjectNamespace,
} from './do-test-utils.js';
import {LOG_LOGS_PATH, REPORT_METRICS_PATH} from './paths.js';
import {BaseWorkerEnv, createWorker} from './worker.js';

const TEST_API_KEY = 'TEST_REFLECT_API_KEY_TEST';

function createTestFixture(
  options: {
    createTestResponse?: (req: Request) => Response;
    authApiKeyDefined?: boolean;
    disable?: string | undefined;
  } = {},
) {
  const {
    createTestResponse = () => new Response('success', {status: 200}),
    authApiKeyDefined = true,
    disable,
  } = options;
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
  };
  if (authApiKeyDefined) {
    testEnv.REFLECT_API_KEY = TEST_API_KEY;
  }
  if (disable !== undefined) {
    testEnv.DISABLE = disable;
  }

  return {
    testEnv,
    authDORequests,
  };
}

function createWorkerWithTestLogSink() {
  return createWorker(() => ({
    logSink: new TestLogSink(),
    logLevel: 'error',
  }));
}

function testDisabled(testRequest: Request) {
  return testNotForwardedToAuthDo(
    testRequest,
    new Response('Disabled', {status: 503}),
    'true',
  );
}

async function testNotForwardedToAuthDo(
  testRequest: Request,
  expectedResponse: Response,
  disable?: string | undefined,
) {
  const {testEnv, authDORequests} = createTestFixture({
    createTestResponse: () => {
      throw new Error('Unexpected call to auth DO');
    },
    disable,
  });
  const worker = createWorkerWithTestLogSink();
  if (!worker.fetch) {
    throw new Error('Expect fetch to be defined');
  }
  if (expectedResponse.webSocket) {
    throw new Error('Expected response should not be a websocket');
  }
  const expectedResponseClone = expectedResponse.clone();
  const response = await worker.fetch(
    testRequest,
    testEnv,
    new TestExecutionContext(),
  );

  expect(authDORequests.length).toEqual(0);
  expect(response.status).toEqual(expectedResponse.status);
  expect(await response.text()).toEqual(await expectedResponseClone.text());

  const responseHeaders = [...response.headers.entries()];
  const expectedResponseHeaders = [
    ...expectedResponse.headers.entries(),
    ['access-control-allow-origin', '*'],
  ];
  expect(responseHeaders.length).toEqual(expectedResponseHeaders.length);
  expect(responseHeaders).toEqual(
    expect.arrayContaining(expectedResponseHeaders),
  );
}

async function testForwardedToAuthDO(
  testRequest: Request,
  authDoResponse = new Response('success', {status: 200}),
) {
  // Don't clone response if it has a websocket, otherwise CloudFlare's Response
  // class will throw
  // "TypeError: Cannot clone a response to a WebSocket handshake."
  const testResponseClone = authDoResponse.webSocket
    ? undefined
    : authDoResponse.clone();
  const {testEnv, authDORequests} = createTestFixture({
    createTestResponse: () => authDoResponse,
  });
  const worker = createWorkerWithTestLogSink();
  if (!worker.fetch) {
    throw new Error('Expect fetch to be defined');
  }
  const response = await worker.fetch(
    testRequest,
    testEnv,
    new TestExecutionContext(),
  );

  expect(authDORequests.length).toEqual(1);
  expect(authDORequests[0].req).toBe(testRequest);
  expect(authDORequests[0].resp).toBe(authDoResponse);
  expect(response.status).toEqual(authDoResponse.status);
  if (testResponseClone) {
    expect(await response.text()).toEqual(await testResponseClone.text());
  }
  const responseHeaders = [...response.headers.entries()];
  const expectedResponseHeaders = [
    ...authDoResponse.headers.entries(),
    ['access-control-allow-origin', '*'],
  ];
  expect(responseHeaders.length).toEqual(expectedResponseHeaders.length);
  expect(responseHeaders).toEqual(
    expect.arrayContaining(expectedResponseHeaders),
  );
  expect(response.webSocket).toBe(authDoResponse.webSocket);

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

test('worker does not forward connect requests to authDO when DISABLE is true', async () => {
  await testDisabled(new Request('ws://test.roci.dev/connect'));
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
        headers: createAPIHeaders(TEST_API_KEY),
        body: tc.body ? JSON.stringify(tc.body) : null,
      }),
    );
    await testDisabled(
      new Request(tc.path, {
        method: tc.method,
        headers: createAPIHeaders(TEST_API_KEY),
        body: tc.body ? JSON.stringify(tc.body) : null,
      }),
    );
    await testNotForwardedToAuthDo(
      new Request(tc.path, {
        method: tc.path,
        // Note: no auth header.
        body: tc.body ? JSON.stringify(tc.body) : null,
      }),
      new Response('Unauthorized', {
        status: 401,
      }),
    );
  }
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
  const worker = createWorker(env => {
    expect(env).toBe(testEnv);
    const logSink = {
      log: (_level: LogLevel, ..._args: unknown[]): void => {
        logCallCount++;
      },
      flush: (): Promise<void> => logFlushPromise,
    };
    return {
      get logSink() {
        getLogSinkCallCount++;
        return logSink;
      },
      get logLevel(): LogLevel {
        getLogLevelCallCount++;
        return 'debug';
      },
    };
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

test('preflight request handling allows all origins, paths, methods and headers', async () => {
  await testPreflightRequest({
    origin: 'http://example.com',
    url: 'https://worker.com/connect',
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
  const worker = createWorkerWithTestLogSink();
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

describe('reportMetrics', () => {
  const reportMetricsURL = new URL(
    REPORT_METRICS_PATH,
    'https://test.roci.dev/',
  );
  type TestCase = {
    name: string;
    method: string;
    path?: string;
    body: undefined | Record<string, unknown>;
    expectedStatus: number;
    expectFetch: boolean;
  };

  const series: Series = {
    metric: 'metric1',
    points: [[1, [2]]],
  };
  const goodBody = {series: [series]};
  const testCases: TestCase[] = [
    {
      name: 'legacy distribution request',
      method: 'post',
      path: '/api/v1/distribution_points',
      body: goodBody,
      expectedStatus: 200,
      expectFetch: true,
    },
    {
      name: 'explicit distribution request',
      method: 'post',
      path: '/api/v1/distribution_points',
      body: {
        series: [
          {
            metric: 'metric1',
            points: [[1, [2]]],
            type: 'distribution',
          },
        ],
      },
      expectedStatus: 200,
      expectFetch: true,
    },
    {
      name: 'count request',
      method: 'post',
      path: '/api/v1/series',
      body: {
        series: [
          {
            metric: 'metric1',
            points: [[1, 2]],
            type: 'count',
          },
        ],
      },
      expectedStatus: 200,
      expectFetch: true,
    },
    {
      name: 'good request: empty series',
      method: 'post',
      body: {series: []},
      expectedStatus: 200,
      expectFetch: false,
    },
    {
      name: 'bad method',
      method: 'get',
      body: goodBody,
      expectedStatus: 405,
      expectFetch: false,
    },
    {
      name: 'malformed body: no body',
      method: 'post',
      body: undefined,
      expectedStatus: 400,
      expectFetch: false,
    },
    {
      name: 'malformed body: empty body',
      method: 'post',
      body: {},
      expectedStatus: 400,
      expectFetch: false,
    },
    {
      name: 'malformed body: no series',
      method: 'post',
      body: {foo: 'bar'},
      expectedStatus: 400,
      expectFetch: false,
    },
  ];
  for (const tc of testCases) {
    test(tc.name, () => testReportMetrics(tc));
  }

  async function testReportMetrics(tc: TestCase) {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockReturnValue(Promise.resolve(new Response('{}')));

    const testEnv: BaseWorkerEnv = {
      authDO: {
        ...createTestDurableObjectNamespace(),
      },
    };

    const worker = createWorker(() => ({
      logSink: new TestLogSink(),
      logLevel: 'error',
      datadogMetricsOptions: {
        apiKey: 'test-dd-key',
        service: 'test-service',
        tags: {script: 'test-script'},
      },
    }));

    const testRequest = new Request(reportMetricsURL.toString(), {
      method: tc.method,
      body: tc.method === 'post' && tc.body ? JSON.stringify(tc.body) : null,
    });
    if (worker.fetch === undefined) {
      throw new Error('Expect fetch to be defined');
    }
    const response = await worker.fetch(
      testRequest,
      testEnv,
      new TestExecutionContext(),
    );
    if (response.status !== tc.expectedStatus) {
      fail(
        `Expected status ${tc.expectedStatus} but got ${response.status} ` +
          `Response body: ${await response.text()}`,
      );
    }

    if (tc.expectFetch) {
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const args = fetchSpy.mock.calls[0];
      assertString(args[0]);
      const gotURL = new URL(args[0]);
      expect(gotURL.origin).toEqual('https://api.datadoghq.com');
      expect(gotURL.pathname).toEqual(tc.path);
      const gotOptions = args[1];
      expect(gotOptions).toEqual({
        body: tc.body
          ? JSON.stringify({
              ...tc.body,
              series: (tc.body.series as Series[]).map(s => ({
                ...s,
                tags: [
                  ...(s.tags ?? []),
                  'script:test-script',
                  'service:test-service',
                ],
                type: s.type,
              })),
            })
          : undefined,
        headers: {
          'DD-API-KEY': 'test-dd-key',
        },
        method: 'POST',
      });
    } else {
      expect(fetchSpy).not.toHaveBeenCalled();
    }
  }
});

describe('log logs', () => {
  async function testLogLogs(
    fetchSpy: jest.SpiedFunction<typeof fetch>,
    expectedResponseStatusCode: number,
    ddLogsApiKeyInEnv = true,
  ) {
    const logLogsURL = new URL(LOG_LOGS_PATH, 'https://test.roci.dev/');
    logLogsURL.searchParams.set('service', 'test-service');
    logLogsURL.searchParams.set('ddtags', 'version:0.35.0');
    logLogsURL.searchParams.set('host', 'test.host.com');
    logLogsURL.searchParams.set('foo', 'bar');

    const testEnv: BaseWorkerEnv = {
      authDO: {
        ...createTestDurableObjectNamespace(),
      },
    };
    if (ddLogsApiKeyInEnv) {
      testEnv['DATADOG_LOGS_API_KEY'] = 'test-dd-logs-api-key';
    }

    const worker = createWorker(() => ({
      logSink: new TestLogSink(),
      logLevel: 'error',
      datadogMetricsOptions: {
        apiKey: 'test-dd-key',
        service: 'test-service',
        tags: {script: 'test-script'},
      },
    }));

    const testBody = 'test-body';

    const testRequest = new Request(logLogsURL.toString(), {
      method: 'POST',
      body: testBody,
    });
    if (worker.fetch === undefined) {
      throw new Error('Expect fetch to be defined');
    }
    const response = await worker.fetch(
      testRequest,
      testEnv,
      new TestExecutionContext(),
    );

    expect(response.status).toEqual(expectedResponseStatusCode);

    if (!ddLogsApiKeyInEnv) {
      expect(fetchSpy).toHaveBeenCalledTimes(0);
      return;
    }
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const fetchedRequest = fetchSpy.mock.calls[0][0];
    if (typeof fetchedRequest === 'string') {
      throw new Error('Expected request not string fetched');
    }
    expect(fetchedRequest.url).toEqual(
      'https://http-intake.logs.datadoghq.com/api/v2/logs?service=test-service&ddtags=version%3A0.35.0&host=test.host.com&foo=bar&dd-api-key=test-dd-logs-api-key&ddsource=client',
    );
    expect(fetchedRequest.headers.get('content-type')).toEqual(
      'text/plain;charset=UTF-8',
    );
    expect(await fetchedRequest.text()).toEqual(testBody);
  }

  test('success', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockReturnValue(Promise.resolve(new Response('{}')));
    await testLogLogs(fetchSpy, 200);
  });

  test('error response', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockReturnValue(Promise.resolve(new Response('failed', {status: 403})));
    await testLogLogs(fetchSpy, 403);
  });

  test('error', async () => {
    const fetchSpy = jest.spyOn(globalThis, 'fetch').mockImplementation(() => {
      throw new Error('test error');
    });
    await testLogLogs(fetchSpy, 500);
  });

  test('no api key in env', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockReturnValue(Promise.resolve(new Response('{}')));
    await testLogLogs(fetchSpy, 200, false);
  });
});

test('hello', async () => {
  const testEnv: BaseWorkerEnv = {
    authDO: {
      ...createTestDurableObjectNamespace(),
    },
  };

  const worker = createWorker(() => ({
    logSink: new TestLogSink(),
    logLevel: 'error',
    datadogMetricsOptions: {
      apiKey: 'test-dd-key',
      service: 'test-service',
    },
  }));

  const testRequest = new Request('https://test.roci.dev/'.toString());
  if (worker.fetch === undefined) {
    throw new Error('Expect fetch to be defined');
  }
  const response = await worker.fetch(
    testRequest,
    testEnv,
    new TestExecutionContext(),
  );
  expect(response.status).toEqual(200);
  expect(await response.json()).toEqual({reflectServerVersion: version});
});
