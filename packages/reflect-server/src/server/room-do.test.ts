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
import {subscribe, unsubscribe} from 'node:diagnostics_channel';
import type {LogLevel, TailMessage} from 'reflect-protocol/src/tail.js';
import type {MutatorDefs, WriteTransaction} from 'reflect-shared/src/types.js';
import {version} from 'reflect-shared/src/version.js';
import {CONNECTION_SECONDS_CHANNEL_NAME} from 'shared/src/events/connection-seconds.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {Queue} from 'shared/src/queue.js';
import {newCreateRoomRequest, newDeleteRoomRequest} from '../client/room.js';
import {REPORTING_INTERVAL_MS} from '../events/connection-seconds.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {getUserValue, putUserValue} from '../types/user-value.js';
import {getVersion, putVersion} from '../types/version.js';
import {resolver} from '../util/resolver.js';
import {sleep} from '../util/sleep.js';
import {TestLogSink} from '../util/test-utils.js';
import {originalConsole} from './console.js';
import {createTestDurableObjectState} from './do-test-utils.js';
import {AUTH_DATA_HEADER_NAME, addRoomIDHeader} from './internal-headers.js';
import {TAIL_URL_PATH} from './paths.js';
import {BaseRoomDO, getDefaultTurnDuration} from './room-do.js';

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

test('inits storage schema', async () => {
  const testLogSink = new TestLogSink();
  const state = await createTestDurableObjectState('test-do-id');

  expect(await state.storage.get('storage_schema_meta')).toBeUndefined();

  new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state,
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });

  await state.concurrencyBlockingCallbacks();

  // This just asserts that the storage schema was initialized by the room constructor.
  // The actual storage schema update logic is tested in the room-schema.test
  expect(await state.storage.get('storage_schema_meta')).not.toBeUndefined();
});

test('runs roomStartHandler on first fetch', async () => {
  const testLogSink = new TestLogSink();
  const testRoomID = 'testRoomID';
  const state = await createTestDurableObjectState(
    'test-do-id-foo-bar-baz-boom',
  );

  const storage = new DurableStorage(state.storage);
  const startingVersion = 23;
  await putVersion(startingVersion, storage);
  await putUserValue(
    'foo',
    {version: 1, deleted: false, value: 'bar'},
    storage,
  );

  let roomStartHandlerCallCount = 0;
  const roomDO = new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    roomStartHandler: async (tx: WriteTransaction, roomID: string) => {
      expect(roomID).toEqual(testRoomID);
      roomStartHandlerCallCount++;
      const value = await tx.get('foo');
      await tx.set('foo', `${value}+${roomStartHandlerCallCount}`);
    },
    state,
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });

  await state.concurrencyBlockingCallbacks();

  // The roomHandler should not have been run yet.
  expect(roomStartHandlerCallCount).toEqual(0);

  await createRoom(roomDO, testRoomID);

  // The roomHandler should have been run.
  expect(roomStartHandlerCallCount).toEqual(1);
  expect(await getVersion(storage)).toEqual(startingVersion + 1);
  expect(await getUserValue('foo', storage)).toEqual({
    version: startingVersion + 1,
    deleted: false,
    value: 'bar+1',
  });

  await createRoom(roomDO, testRoomID, undefined);

  // The roomHandler should not have been run again.
  expect(roomStartHandlerCallCount).toEqual(1);
  expect(await getVersion(storage)).toEqual(startingVersion + 1);
  expect(await getUserValue('foo', storage)).toEqual({
    version: startingVersion + 1,
    deleted: false,
    value: 'bar+1',
  });
});

test('runs roomStartHandler on next fetch if throws on first fetch', async () => {
  const testLogSink = new TestLogSink();
  const testRoomID = 'testRoomID';
  const state = await createTestDurableObjectState('test-do-id');

  const storage = new DurableStorage(state.storage);
  const startingVersion = 23;
  await putVersion(startingVersion, storage);
  await putUserValue(
    'foo',
    {version: 1, deleted: false, value: 'bar'},
    storage,
  );

  let roomStartHandlerCallCount = 0;
  const roomDO = new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    roomStartHandler: async (tx: WriteTransaction, roomID: string) => {
      expect(roomID).toEqual(testRoomID);
      roomStartHandlerCallCount++;
      if (roomStartHandlerCallCount === 1) {
        throw new Error('Test error in roomStartHandler');
      }
      const value = await tx.get('foo');
      await tx.set('foo', `${value}+${roomStartHandlerCallCount}`);
    },
    state,

    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });

  await state.concurrencyBlockingCallbacks();

  // The roomHandler should not have been run yet.
  expect(roomStartHandlerCallCount).toEqual(0);

  await createRoom(roomDO, testRoomID, 500);

  // The roomHandler should have been run, but not modified state.
  expect(roomStartHandlerCallCount).toEqual(1);
  expect(await getVersion(storage)).toEqual(startingVersion);
  expect(await getUserValue('foo', storage)).toEqual({
    version: 1,
    deleted: false,
    value: 'bar',
  });

  await createRoom(roomDO, testRoomID);

  // The roomHandler should have been run again, since the first run failed.
  expect(roomStartHandlerCallCount).toEqual(2);
  expect(await getVersion(storage)).toEqual(startingVersion + 1);
  expect(await getUserValue('foo', storage)).toEqual({
    version: startingVersion + 1,
    deleted: false,
    value: 'bar+2',
  });
});

test('deleteAllData deletes all data', async () => {
  const testLogSink = new TestLogSink();
  const state = await createTestDurableObjectState('test-do-id');
  const someKey = 'foo';
  await state.storage.put(someKey, 'bar');
  expect(await (await state.storage.list()).size).toBeGreaterThan(0);

  const roomDO = new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state,

    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });

  await createRoom(roomDO, 'testRoomID');

  const deleteRequest = addRoomIDHeader(
    newDeleteRoomRequest('http://example.com/', 'API KEY', 'testRoomID'),
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
  const testLogSink = new TestLogSink();

  const roomDO = new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state: await createTestDurableObjectState('test-do-id'),
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
  });
  await createRoom(roomDO, 'testRoomID');

  const deleteRequest = addRoomIDHeader(
    newDeleteRoomRequest('http://example.com/', 'API KEY', 'testRoomID'),
    'testRoomID',
  );
  const response = await roomDO.fetch(deleteRequest);
  expect(response.status).toBe(200);

  await createRoom(roomDO, 'testRoomID', 410);
  const response3 = await roomDO.fetch(deleteRequest);
  expect(response3.status).toBe(410);
  const response4 = await roomDO.fetch(new Request('http://example.com/'));
  expect(response4.status).toBe(410);
});

test('Logs version during construction', async () => {
  const testLogSink = new TestLogSink();
  new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state: await createTestDurableObjectState('test-do-id'),
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
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
  const testLogSink = new TestLogSink();
  const room = new BaseRoomDO({
    mutators: {},
    ...noopHandlers,
    state: await createTestDurableObjectState('test-do-id'),
    logSink: testLogSink,
    logLevel: 'info',
    allowUnconfirmedWrites: true,
    maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
    env: {foo: 'bar'},
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
    () => {
      fired++;
    },
  );

  // Wait for the timer to fire at least 5 times.
  // Note: jest.useFakeTimers() doesn't quite work as expected for setInterval()
  // so we're using real timers with real sleep().
  while (fired < 5) {
    await sleep(2);
  }
  clearTimeout(timerID);

  finishCallback();
  await latches[1].promise; // Wait for the second invocation.

  await sleep(1); // No other invocations should happen, even with sleep.
  expect(invoked).toBe(2); // All other invocations should have been aborted.
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

describe('connection seconds tracking', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('tracks', async () => {
    const START_TIME = 10000;
    jest.setSystemTime(START_TIME);

    const state = await createTestDurableObjectState('test-do-id');
    const roomDO = await makeBaseRoomDO(state);
    const reports = new Queue<unknown>();
    function onPublish(message: unknown) {
      void reports.enqueue(message);
    }
    subscribe(CONNECTION_SECONDS_CHANNEL_NAME, onPublish);

    const roomID = 'testRoomID';
    const request = addRoomIDHeader(
      new Request(
        `ws://test.roci.dev/api/sync/v1/connect?clientID=cid1&clientGroupID=cg1&ts=123&lmid=0&wsid=wsidx1&roomID=${roomID}`,
        {
          headers: {
            [AUTH_DATA_HEADER_NAME]: '{"userID":"u1","more":"data"}',
            ['Upgrade']: 'websocket',
          },
        },
      ),
      roomID,
    );
    const response = await roomDO.fetch(request);
    expect(response.status).toBe(101);

    // Let the async handleConnection() code run.
    await jest.advanceTimersByTimeAsync(1);

    const alarmTime = await state.storage.getAlarm();
    expect(alarmTime).toBe(START_TIME + REPORTING_INTERVAL_MS);

    // Fire the alarm at the scheduled time.
    jest.setSystemTime(alarmTime ?? 0);
    await roomDO.alarm();

    expect(await reports.dequeue()).toEqual({
      elapsed: REPORTING_INTERVAL_MS / 1000,
      period: REPORTING_INTERVAL_MS / 1000,
      roomID: 'testRoomID',
    });

    unsubscribe(CONNECTION_SECONDS_CHANNEL_NAME, onPublish);
  });
});

test('good, bad, invalid connect requests', async () => {
  const goodRequest = new Request('ws://test.roci.dev/api/sync/v1/connect');
  goodRequest.headers.set('Upgrade', 'websocket');
  const goodTest = {
    request: goodRequest,
    expectedStatus: 101,
    expectedText: '',
    expectedJSON: null,
  };

  const nonWebSocketTest = {
    request: new Request('ws://test.roci.dev/api/sync/v1/connect'),
    expectedStatus: 400,
    expectedText: 'expected websocket',
    expectedJSON: null,
  };

  const badRequestTest = {
    request: new Request('ws://test.roci.dev/api/sync/v1/connect', {
      method: 'POST',
    }),
    expectedStatus: 405,
    expectedText: null,
    expectedJSON: {
      error: {
        code: 405,
        resource: 'request',
        message: 'unsupported method',
      },
    },
  };

  const roomDO = await makeBaseRoomDO();
  for (const test of [goodTest, nonWebSocketTest, badRequestTest]) {
    const response = await roomDO.fetch(
      addRoomIDHeader(test.request, 'testRoomID'),
    );
    if (test.expectedText) {
      expect(await response.text()).toEqual(test.expectedText);
    } else if (test.expectedJSON) {
      expect(await response.json()).toEqual(test.expectedJSON);
    }
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
    expectedText?: string;
    expectedJSON?: ReadonlyJSONValue;
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
      expectedJSON: {
        error: {
          code: 405,
          resource: 'request',
          message: 'unsupported method',
        },
      },
    },
  ];
  for (const c of cases) {
    test(c.name, async () => {
      const testLogSink = new TestLogSink();
      const state = await createTestDurableObjectState('test-do-id');
      const roomDO = new BaseRoomDO({
        mutators: {},
        roomStartHandler: () => Promise.resolve(),
        onClientDisconnect: () => Promise.resolve(),
        onClientDelete: () => Promise.resolve(),
        state,
        logSink: testLogSink,
        logLevel: 'info',
        allowUnconfirmedWrites: true,
        maxMutationsPerTurn: Number.MAX_SAFE_INTEGER,
        env: {foo: 'bar'},
      });

      const response = await roomDO.fetch(
        addRoomIDHeader(c.request, 'testRoomID'),
      );
      if (c.expectedText) {
        expect(await response.text()).toEqual(c.expectedText);
      } else if (c.expectedJSON) {
        expect(await response.json()).toEqual(c.expectedJSON);
      }
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

describe('tail', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('tail should replace global console', async () => {
    jest.setSystemTime(1984);
    const roomDO = await makeBaseRoomDO();

    const request = addRoomIDHeader(
      new Request('ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID', {
        headers: {['Upgrade']: 'websocket'},
      }),
      'testRoomID',
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
        expect(JSON.parse(e.data as string)).toEqual({
          type: 'log',
          level: 'log',
          message: ['hello', 'world'],
        });
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

    function makeLog(s: LogLevel): TailMessage {
      return {type: 'log', level: s, message: [s]};
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

    const request1 = addRoomIDHeader(
      new Request('ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID', {
        headers: {['Upgrade']: 'websocket'},
      }),
      'testRoomID',
    );
    const response1 = await roomDO.fetch(request1);
    expect(response1.status).toBe(101);
    response1.webSocket!.accept();

    const request2 = addRoomIDHeader(
      new Request('ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID', {
        headers: {['Upgrade']: 'websocket'},
      }),
      'testRoomID',
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
      return {type: 'log', level: 'log', message};
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
    expect(log2).toEqual([
      makeLog(['hello', 'world']),
      makeLog(['good', 'bye']),
    ]);
  });

  test('tail log throws on json stringify', async () => {
    jest.setSystemTime(1984);
    const roomDO = await makeBaseRoomDO();

    jest.spyOn(originalConsole, 'log').mockImplementation(() => {});

    const originalConsoleErrorSpy = jest
      .spyOn(originalConsole, 'error')
      .mockImplementation(() => {});

    const request = addRoomIDHeader(
      new Request('ws://test.roci.dev' + TAIL_URL_PATH + '?roomID=testRoomID', {
        headers: {['Upgrade']: 'websocket'},
      }),
      'testRoomID',
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
});
