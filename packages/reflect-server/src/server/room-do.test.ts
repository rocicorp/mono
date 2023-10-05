import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {LogContext} from '@rocicorp/logger';
import assert from 'node:assert';
import type {WriteTransaction} from 'reflect-shared';
import {version} from 'reflect-shared';
import {
  newInvalidateAllAuthRequest,
  newInvalidateForRoomAuthRequest,
  newInvalidateForUserAuthRequest,
} from '../client/auth.js';
import {newCreateRoomRequest, newDeleteRoomRequest} from '../client/room.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {getUserValue, putUserValue} from '../types/user-value.js';
import {getVersion, putVersion} from '../types/version.js';
import {newAuthConnectionsRequest} from '../util/auth-test-util.js';
import {resolver} from '../util/resolver.js';
import {TestLogSink, createSilentLogContext} from '../util/test-utils.js';
import {originalConsole} from './console.js';
import {createTestDurableObjectState} from './do-test-utils.js';
import {TAIL_URL_PATH} from './paths.js';
import {BaseRoomDO, getDefaultTurnDuration} from './room-do.js';

const testLogSink = new TestLogSink();

const START_TIME = 1000;
beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(START_TIME);
});

afterEach(() => {
  jest.restoreAllMocks();
});

test('sets roomID in createRoom', async () => {
  const roomDO = new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });
  const createRoomRequest = newCreateRoomRequest(
    'http://example.com/',
    'API KEY',
    'testRoomID',
  );
  const response = await roomDO.fetch(createRoomRequest);
  expect(response.status).toBe(200);
  const lc = createSilentLogContext();
  const roomID = await roomDO.roomID(lc);
  expect(roomID).toBe('testRoomID');
});

test('inits storage schema', async () => {
  const state = await createTestDurableObjectState('test-do-id');

  expect(await state.storage.get('storage_schema_meta')).toBeUndefined();

  new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state,
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });

  await state.concurrencyBlockingCallbacks();

  // This just asserts that the storage schema was initialized by the room constructor.
  // The actual storage schema update logic is tested in the room-schema.test
  expect(await state.storage.get('storage_schema_meta')).not.toBeUndefined();
});

test('runs roomStartHandler', async () => {
  const state = await createTestDurableObjectState('test-do-id');

  const storage = new DurableStorage(state.storage);
  const startingVersion = 23;
  await putVersion(startingVersion, storage);
  await putUserValue(
    'foo',
    {version: 1, deleted: false, value: 'bar'},
    storage,
  );

  new BaseRoomDO({
    mutators: {},
    roomStartHandler: async (tx: WriteTransaction) => {
      const value = await tx.get('foo');
      await tx.put('foo', `${value}+`);
    },
    disconnectHandler: () => Promise.resolve(),
    state,
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });

  await state.concurrencyBlockingCallbacks();

  // The roomHandler should have been run exactly once.
  expect(await getVersion(storage)).toBe(startingVersion + 1);
  expect(await getUserValue('foo', storage)).toEqual({
    version: startingVersion,
    deleted: false,
    value: 'bar+',
  });
});

test('deleteAllData deletes all data', async () => {
  const state = await createTestDurableObjectState('test-do-id');
  const someKey = 'foo';
  await state.storage.put(someKey, 'bar');
  expect(await (await state.storage.list()).size).toBeGreaterThan(0);

  const roomDO = new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state,
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });
  const createRoomRequest = newCreateRoomRequest(
    'http://example.com/',
    'API KEY',
    'testRoomID',
  );
  const createResponse = await roomDO.fetch(createRoomRequest);
  expect(createResponse.status).toBe(200);

  const deleteRequest = newDeleteRoomRequest(
    'http://example.com/',
    'API KEY',
    'testRoomID',
  );
  const response = await roomDO.fetch(deleteRequest);
  expect(response.status).toBe(200);
  const gotValue = await state.storage.get(someKey);
  expect(gotValue).toBeUndefined();
  expect(await (await state.storage.list()).size).toEqual(
    1 /* deleted record */,
  );
});

test('after deleteAllData the roomDO just 410s', async () => {
  const roomDO = new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });
  const createRoomRequest = newCreateRoomRequest(
    'http://example.com/',
    'API KEY',
    'testRoomID',
  );
  const createResponse = await roomDO.fetch(createRoomRequest);
  expect(createResponse.status).toBe(200);

  const deleteRequest = newDeleteRoomRequest(
    'http://example.com/',
    'API KEY',
    'testRoomID',
  );
  const response = await roomDO.fetch(deleteRequest);
  expect(response.status).toBe(200);

  const response2 = await roomDO.fetch(createRoomRequest);
  expect(response2.status).toBe(410);
  const response3 = await roomDO.fetch(deleteRequest);
  expect(response3.status).toBe(410);
  const response4 = await roomDO.fetch(new Request('http://example.com/'));
  expect(response4.status).toBe(410);
});

test('401s if wrong auth api key', async () => {
  const wrongApiKey = 'WRONG KEY';
  const deleteRequest = newDeleteRoomRequest(
    'http://example.com/',
    wrongApiKey,
    'testRoomID',
  );

  const invalidateAllRequest = newInvalidateAllAuthRequest(
    'http://example.com/',
    wrongApiKey,
  );

  const authConnectionsRequest = newAuthConnectionsRequest(
    'http://example.com/',
    wrongApiKey,
  );

  const invalidateForUserRequest = newInvalidateForUserAuthRequest(
    'http://example.com/',
    wrongApiKey,
    'testUserID',
  );

  const invalidateForRoomRequest = newInvalidateForRoomAuthRequest(
    'http://example.com/',
    wrongApiKey,
    'testRoomID',
  );

  const createRoomRequest = newCreateRoomRequest(
    'http://example.com/',
    wrongApiKey,
    'testRoomID',
  );

  const testRequests = [
    deleteRequest,
    invalidateAllRequest,
    invalidateForUserRequest,
    invalidateForRoomRequest,
    authConnectionsRequest,
    createRoomRequest,
  ];

  for (const testRequest of testRequests) {
    const roomDO = new BaseRoomDO({
      mutators: {},
      roomStartHandler: () => Promise.resolve(),
      disconnectHandler: () => Promise.resolve(),
      state: await createTestDurableObjectState('test-do-id'),
      authApiKey: 'API KEY',
      logSink: testLogSink,
      logLevel: 'info',
      allowUnconfirmedWrites: true,
      maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    });

    const response = await roomDO.fetch(testRequest);
    expect(response.status).toBe(401);
  }
});

test('Logs version during construction', async () => {
  const testLogSink = new TestLogSink();
  new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'foo',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });
  expect(testLogSink.messages).toEqual(
    expect.arrayContaining([
      [
        'info',
        {component: 'RoomDO', doID: 'test-do-id'},
        ['Starting RoomDO. Version:', version],
      ],
    ]),
  );
  expect(testLogSink.messages[0][2][1]).toMatch(/^\d+\.\d+\.\d+/);
});

test('Avoids queueing many intervals in the lock', async () => {
  const room = new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'foo',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });

  const {promise: canFinishCallback, resolve: finishCallback} =
    resolver<void>();
  const latches = [resolver<void>(), resolver<void>()];

  let fired = 0;
  let invoked = 0;
  const timerID = room.runInLockAtInterval(
    new LogContext('debug', {}, testLogSink),
    'fakeProcessNext',
    1, // Fire once every ms.
    async () => {
      latches[invoked++].resolve();
      await canFinishCallback; // Make the first invocation hold the lock.
    },
    1,
    () => undefined,
    () => {
      fired++;
    },
  );

  // Wait for the timer to fire at least 5 times.
  // Note: jest.useFakeTimers() doesn't quite work as expected for setInterval()
  // so we're using real timers with real sleep().
  while (fired < 5) {
    await jest.advanceTimersByTimeAsync(2);
  }
  clearTimeout(timerID);

  finishCallback();
  await latches[1].promise; // Wait for the second invocation.

  await jest.advanceTimersByTimeAsync(1); // No other invocations should happen, even with sleep.
  expect(invoked).toBe(2); // All other invocations should have been aborted.
});

test('clear interval call', async () => {
  const room = new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'foo',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });

  let fired = 0;
  let invoked = 0;
  room.runInLockAtInterval(
    new LogContext('debug', {}, testLogSink),
    'fakeProcessNext',
    1, // Fire once every ms.
    () => {
      void invoked++;
      return Promise.resolve();
    },
    3,
    () => {
      fired++;
    },
  );
  await jest.advanceTimersByTimeAsync(5);
  expect(Date.now()).toEqual(1005);
  expect(invoked).toBe(5);
  expect(fired).toBe(1);
});

test('Sets turn duration based on allowUnconfirmedWrites flag', () => {
  const cases = [
    {allowUnconfirmed: true, turnDuration: 16},
    {allowUnconfirmed: false, turnDuration: 66},
  ];
  for (const {allowUnconfirmed, turnDuration} of cases) {
    expect(getDefaultTurnDuration(allowUnconfirmed)).toBe(turnDuration);
  }
});

async function makeBaseRoomDO() {
  return new BaseRoomDO({
    mutators: {},
    roomStartHandler: () => Promise.resolve(),
    disconnectHandler: () => Promise.resolve(),
    state: await createTestDurableObjectState('test-do-id'),
    authApiKey: 'API KEY',
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
  });
}

test('good, bad, invalid connect requests', async () => {
  const goodRequest = new Request('ws://test.roci.dev/connect');
  goodRequest.headers.set('Upgrade', 'websocket');
  const goodTest = {
    request: goodRequest,
    expectedStatus: 101,
    expectedText: '',
  };

  const nonWebSocketTest = {
    request: new Request('ws://test.roci.dev/connect'),
    expectedStatus: 400,
    expectedText: 'expected websocket',
  };

  const badRequestTest = {
    request: new Request('ws://test.roci.dev/connect', {method: 'POST'}),
    expectedStatus: 405,
    expectedText: 'unsupported method',
  };

  const roomDO = await makeBaseRoomDO();
  for (const test of [goodTest, nonWebSocketTest, badRequestTest]) {
    const response = await roomDO.fetch(test.request);
    expect(await response.text()).toEqual(test.expectedText);
    expect(response.status).toBe(test.expectedStatus);
  }
});

describe('good, bad, invalid tail requests', () => {
  function makeRequest(init?: RequestInit) {
    return new Request(
      'ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID',
      init,
    );
  }

  type Case = {
    name: string;
    request: Request;
    expectedStatus: number;
    expectedText: string;
  };

  const cases: Case[] = [
    {
      name: 'good',
      request: makeRequest({headers: {['Upgrade']: 'websocket'}}),
      expectedStatus: 101,
      expectedText: '',
    },
    {
      name: 'nonWebSocket',
      request: makeRequest(),
      expectedStatus: 400,
      expectedText: 'expected websocket',
    },
    {
      name: 'badRequest',
      request: makeRequest({method: 'POST'}),
      expectedStatus: 405,
      expectedText: 'unsupported method',
    },
  ];
  for (const c of cases) {
    test(c.name, async () => {
      const state = await createTestDurableObjectState('test-do-id');
      const roomDO = new BaseRoomDO({
        mutators: {},
        roomStartHandler: () => Promise.resolve(),
        disconnectHandler: () => Promise.resolve(),
        state,
        authApiKey: 'API KEY',
        logSink: testLogSink,
        logLevel: 'info',
        allowUnconfirmedWrites: true,
        maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
      });

      const response = await roomDO.fetch(c.request);
      expect(await response.text()).toEqual(c.expectedText);
      expect(response.status).toBe(c.expectedStatus);
      if (c.expectedStatus === 101) {
        expect(response.ok).toBe(true);
        expect(response.webSocket).toBeDefined();
        response.webSocket!.accept();
        response.webSocket!.close();
      }
    });
  }
});

test('tail should replace global console', async () => {
  jest.setSystemTime(1984);
  const roomDO = await makeBaseRoomDO();

  const request = new Request(
    'ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID',
    {headers: {['Upgrade']: 'websocket'}},
  );

  const originalConsoleLogSpy = jest
    .spyOn(originalConsole, 'log')
    .mockImplementation(() => {
      // Do nothing.
    });
  const response = await roomDO.fetch(request);
  expect(response.status).toBe(101);
  const tailConsoleLogSpy = jest.spyOn(console, 'log');
  const ws = response.webSocket;
  assert(ws);
  ws.accept();

  let {promise, resolve} = resolver<void>();

  ws.addEventListener(
    'message',
    e => {
      expect(typeof e.data).toBe('string');
      expect(JSON.parse(e.data as string)).toEqual([
        'log',
        1984,
        ['hello', 'world'],
      ]);
      resolve();
    },
    {once: true},
  );

  console.log('hello', 'world');

  expect(originalConsoleLogSpy).not.toHaveBeenCalled();
  expect(tailConsoleLogSpy).toHaveBeenCalledTimes(1);
  expect(tailConsoleLogSpy).toHaveBeenCalledWith('hello', 'world');

  tailConsoleLogSpy.mockReset();

  // Wait for addEventListener to get called
  await promise;

  ({promise, resolve} = resolver<void>());
  const log: unknown[] = [];
  ws.addEventListener('message', e => {
    expect(typeof e.data).toBe('string');
    log.push(JSON.parse(e.data as string));
    if (log.length === 5) {
      resolve();
    }
  });

  console.debug('debug');
  console.error('error');
  console.info('info');
  console.log('log');
  console.warn('warn');

  // Wait to allow event listeners to get called
  await promise;

  function makeLog(s: string) {
    return [s, 1984, [s]];
  }

  expect(log).toEqual([
    makeLog('debug'),
    makeLog('error'),
    makeLog('info'),
    makeLog('log'),
    makeLog('warn'),
  ]);

  ws.close();
  // Wait for close to be dispatched
  await Promise.resolve();

  expect(tailConsoleLogSpy).toHaveBeenCalledTimes(1);
  tailConsoleLogSpy.mockReset();

  // This should be logged to the original console... which is spied on by
  // originalConsoleLogSpy.
  console.log('good', 'bye');

  expect(tailConsoleLogSpy).toHaveBeenCalledTimes(1);
  expect(tailConsoleLogSpy).toHaveBeenCalledWith('good', 'bye');

  expect(originalConsoleLogSpy).toHaveBeenCalledTimes(1);
  expect(originalConsoleLogSpy).toHaveBeenCalledWith('good', 'bye');
});

test('tail two websockets', async () => {
  jest.setSystemTime(1984);
  const roomDO = await makeBaseRoomDO();

  jest.spyOn(originalConsole, 'log').mockImplementation(() => {
    // Do nothing.
  });

  const request1 = new Request(
    'ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID',
    {headers: {['Upgrade']: 'websocket'}},
  );
  const response1 = await roomDO.fetch(request1);
  expect(response1.status).toBe(101);
  response1.webSocket!.accept();

  const request2 = new Request(
    'ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID',
    {headers: {['Upgrade']: 'websocket'}},
  );
  const response2 = await roomDO.fetch(request2);
  expect(response2.status).toBe(101);
  response2.webSocket!.accept();

  const log1: unknown[] = [];
  response1.webSocket!.addEventListener('message', e => {
    log1.push(JSON.parse(e.data as string));
  });

  const log2: unknown[] = [];
  response2.webSocket!.addEventListener('message', e => {
    log2.push(JSON.parse(e.data as string));
  });

  console.log('hello', 'world');

  function makeLog(message: unknown[]) {
    return ['log', 1984, message];
  }

  await Promise.resolve();
  expect(log1).toEqual([makeLog(['hello', 'world'])]);
  expect(log2).toEqual(log1);

  response1.webSocket!.close();

  // Wait for close to be dispatched
  await Promise.resolve();

  console.log('good', 'bye');

  await Promise.resolve();

  expect(log1).toEqual([makeLog(['hello', 'world'])]);
  expect(log2).toEqual([makeLog(['hello', 'world']), makeLog(['good', 'bye'])]);
});

test('tail log throws on json stringify', async () => {
  jest.setSystemTime(1984);
  const roomDO = await makeBaseRoomDO();

  jest.spyOn(originalConsole, 'log').mockImplementation(() => {});

  const originalConsoleErrorSpy = jest
    .spyOn(originalConsole, 'error')
    .mockImplementation(() => {});

  const request = new Request(
    'ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID',
    {headers: {['Upgrade']: 'websocket'}},
  );
  const response = await roomDO.fetch(request);
  expect(response.status).toBe(101);
  response.webSocket!.accept();

  const log: unknown[] = [];
  response.webSocket!.addEventListener('message', e => {
    log.push(JSON.parse(e.data as string));
  });

  const o = {
    a: 1,
    b: {
      toJSON() {
        throw new TypeError();
      },
    },
  };

  console.log(o);

  await Promise.resolve();
  expect(log).toEqual([]);
  expect(originalConsoleErrorSpy).toBeCalledTimes(1);
  originalConsoleErrorSpy.mockReset();

  response.webSocket!.close();

  // Wait for close to be dispatched
  await Promise.resolve();

  console.error('good', 'bye');

  await Promise.resolve();

  expect(log).toEqual([]);
  expect(originalConsoleErrorSpy).toBeCalledTimes(0);
});
