import { test, expect } from "@jest/globals";
import type { LogLevel } from "@rocicorp/logger";
import { Mocket, TestLogSink } from "../util/test-utils.js";
import { createAuthAPIHeaders } from "./auth-api-headers.js";
import {
  createTestDurableObjectNamespace,
  TestDurableObjectId,
  TestDurableObjectStub,
} from "./do-test-utils.js";
import { BaseWorkerEnv, createWorker } from "./worker";

const TEST_AUTH_API_KEY = "TEST_REFLECT_AUTH_API_KEY_TEST";

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
    new Response("success", { status: 200 }),
  authApiKeyDefined = true
) {
  const authDORequests: { req: Request; resp: Response }[] = [];

  const testEnv: BaseWorkerEnv = {
    authDO: {
      ...createTestDurableObjectNamespace(),
      idFromName: (name: string) => {
        expect(name).toEqual("auth");
        return new TestDurableObjectId("test-auth-do-id");
      },
      get: (id: DurableObjectId) => {
        expect(id.name).toEqual("test-auth-do-id");
        return new TestDurableObjectStub(id, async (request: Request) => {
          const testResponse = createTestResponse(request);
          authDORequests.push({ req: request, resp: testResponse });
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
  testResponse = new Response("success", { status: 200 })
) {
  const { testEnv, authDORequests } = createTestFixture(() => testResponse);
  const worker = createWorker({
    getLogSink: (_env) => new TestLogSink(),
    getLogLevel: (_env) => "error",
  });
  if (!worker.fetch) {
    throw new Error("Expect fetch to be defined");
  }
  const response = await worker.fetch(
    testRequest,
    testEnv,
    new TestExecutionContext()
  );
  expect(authDORequests.length).toEqual(1);
  expect(authDORequests[0].req).toBe(testRequest);
  expect(authDORequests[0].resp).toBe(response);
}

test("worker forwards connect requests to authDO", async () => {
  await testForwardedToAuthDO(
    new Request("ws://test.roci.dev/connect"),
    new Response(null, {
      status: 101,
      webSocket: new Mocket(),
    })
  );
});

test("worker forwards auth api requests to authDO", async () => {
  await testForwardedToAuthDO(
    new Request("https://test.roci.dev/api/auth/v0/invalidateForUser", {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({ userID: "userID1" }),
    })
  );
  await testForwardedToAuthDO(
    new Request("https://test.roci.dev/api/auth/v0/invalidateForRoom", {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
      body: JSON.stringify({ roomID: "roomID1" }),
    })
  );
  await testForwardedToAuthDO(
    new Request("https://test.roci.dev/api/auth/v0/invalidateAll", {
      method: "post",
      headers: createAuthAPIHeaders(TEST_AUTH_API_KEY),
    })
  );
});

test("on scheduled event sends api/auth/v0/revalidateConnections to AuthDO when REFLECT_AUTH_API_KEY is defined", async () => {
  const worker = createWorker({
    getLogSink: (_env) => new TestLogSink(),
    getLogLevel: (_env) => "error",
  });

  const { testEnv, authDORequests } = createTestFixture();

  if (!worker.scheduled) {
    throw new Error("Expect scheduled to be defined");
  }
  await worker.scheduled(
    { scheduledTime: 100, cron: "", noRetry: () => undefined },
    testEnv,
    new TestExecutionContext()
  );
  expect(authDORequests.length).toEqual(1);
  const { req } = authDORequests[0];
  expect(req.method).toEqual("POST");
  expect(req.url).toEqual(
    "https://unused-reflect-auth-do.dev/api/auth/v0/revalidateConnections"
  );
  expect(req.headers.get("x-reflect-auth-api-key")).toEqual(TEST_AUTH_API_KEY);
});

test("on scheduled event does not send api/auth/v0/revalidateConnections to AuthDO when REFLECT_AUTH_API_KEY is undefined", async () => {
  const worker = createWorker({
    getLogSink: (_env) => new TestLogSink(),
    getLogLevel: (_env) => "error",
  });

  const { testEnv, authDORequests } = createTestFixture(undefined, false);

  if (!worker.scheduled) {
    throw new Error("Expect scheduled to be defined");
  }
  await worker.scheduled(
    { scheduledTime: 100, cron: "", noRetry: () => undefined },
    testEnv,
    new TestExecutionContext()
  );
  expect(authDORequests.length).toEqual(0);
});

async function testLogging(
  fn: (
    worker: ExportedHandler<BaseWorkerEnv>,
    testEnv: BaseWorkerEnv,
    testExecutionContext: ExecutionContext
  ) => Promise<unknown>
) {
  const { testEnv } = createTestFixture();

  const waitUntilCalls: Promise<unknown>[] = [];
  const testExecutionContext = {
    waitUntil: (promise: Promise<unknown>): void => {
      waitUntilCalls.push(promise);
      return;
    },
    passThroughOnException: (): void => {
      return;
    },
  };

  let getLogSinkCallCount = 0;
  let getLogLevelCallCount = 0;
  let logCallCount = 0;
  const logFlushPromise = Promise.resolve();
  const worker = createWorker({
    getLogSink: (env) => {
      getLogSinkCallCount++;
      expect(env).toBe(testEnv);
      return {
        log: (_level: LogLevel, ..._args: unknown[]): void => {
          logCallCount++;
        },
        flush: (): Promise<void> => {
          return logFlushPromise;
        },
      };
    },
    getLogLevel: (env) => {
      getLogLevelCallCount++;
      expect(env).toBe(testEnv);
      return "debug";
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

test("fetch logging", async () => {
  await testLogging(async (worker, testEnv, testExecutionContext) => {
    const testRequest = new Request("ws://test.roci.dev/connect");
    if (!worker.fetch) {
      throw new Error("Expected fetch to be defined");
    }
    return worker.fetch(testRequest, testEnv, testExecutionContext);
  });
});

test("scheduled logging", async () => {
  await testLogging(async (worker, testEnv, testExecutionContext) => {
    if (!worker.scheduled) {
      throw new Error("Expected scheduled to be defined");
    }
    return worker.scheduled(
      { scheduledTime: 100, cron: "", noRetry: () => undefined },
      testEnv,
      testExecutionContext
    );
  });
});
