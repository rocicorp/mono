import type {Context, LogLevel} from '@rocicorp/logger';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {expect} from 'chai';
import {Mutation, NullableVersion, pushMessageSchema} from 'reflect-protocol';
import type {MutatorDefs, WriteTransaction} from 'reflect-types';
import {ExperimentalMemKVStore, PullRequestV1, PushRequestV1} from 'replicache';
import {assert} from 'shared/asserts.js';
import type {JSONValue} from 'shared/json.js';
import * as valita from 'shared/valita.js';
import * as sinon from 'sinon';
import {REPORT_INTERVAL_MS} from './metrics.js';
import {
  CONNECT_TIMEOUT_MS,
  ConnectionState,
  DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
  PING_INTERVAL_MS,
  PING_TIMEOUT_MS,
  PULL_TIMEOUT_MS,
  RUN_LOOP_INTERVAL_MS,
  createSocket,
  serverAheadReloadReason,
} from './reflect.js';
import {RELOAD_REASON_STORAGE_KEY} from './reload-error-handler.js';
import {ServerError} from './server-error.js';
import {
  MockSocket,
  TestLogSink,
  TestReflect,
  idbExists,
  reflectForTest,
  tickAFewTimes,
  waitForUpstreamMessage,
} from './test-utils.js'; // Why use fakes when we can use the real thing!

let clock: sinon.SinonFakeTimers;
const startTime = 1678829450000;

setup(() => {
  clock = sinon.useFakeTimers();
  clock.setSystemTime(startTime);
  // @ts-expect-error MockSocket is not sufficiently compatible with WebSocket
  sinon.replace(globalThis, 'WebSocket', MockSocket);
});

teardown(() => {
  sinon.restore();
});

test('onOnlineChange callback', async () => {
  let onlineCount = 0;
  let offlineCount = 0;

  const r = reflectForTest({
    onOnlineChange: online => {
      if (online) {
        onlineCount++;
      } else {
        offlineCount++;
      }
    },
  });

  {
    // Offline by default.
    await clock.tickAsync(0);
    expect(r.online).false;
  }

  {
    // First test a disconnect followed by a reconnect. This should not trigger
    // the onOnlineChange callback.
    await r.waitForConnectionState(ConnectionState.Connecting);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);
    await r.triggerConnected();
    await r.waitForConnectionState(ConnectionState.Connected);
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(0);
    await r.triggerClose();
    await r.waitForConnectionState(ConnectionState.Disconnected);
    // Still connected because we haven't yet failed to reconnect.
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(0);
    await r.triggerConnected();
    await r.waitForConnectionState(ConnectionState.Connected);
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(0);
  }

  {
    // Now testing with an error that causes the connection to close. This should
    // trigger the callback.
    onlineCount = offlineCount = 0;
    await r.triggerError('InvalidMessage', 'aaa');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(1);

    // And followed by a reconnect.
    expect(r.online).false;
    await tickAFewTimes(clock, RUN_LOOP_INTERVAL_MS);
    await r.triggerConnected();
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(1);
  }

  {
    // Now test with an auth error. This should not trigger the callback on the first error.
    onlineCount = offlineCount = 0;
    await r.triggerError('Unauthorized', 'bbb');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);

    // And followed by a reconnect.
    expect(r.online).true;
    await r.triggerConnected();
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);
  }

  {
    // Now test with two auth error. This should trigger the callback on the second error.
    onlineCount = offlineCount = 0;
    await r.triggerError('Unauthorized', 'ccc');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);

    await r.waitForConnectionState(ConnectionState.Connecting);
    await r.triggerError('Unauthorized', 'ddd');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await tickAFewTimes(clock, RUN_LOOP_INTERVAL_MS);
    await clock.tickAsync(0);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(1);

    // And followed by a reconnect.
    await r.waitForConnectionState(ConnectionState.Connecting);
    await r.triggerConnected();
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(1);
  }

  {
    // Connection timed out.
    onlineCount = offlineCount = 0;
    await clock.tickAsync(CONNECT_TIMEOUT_MS);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(1);
    await clock.tickAsync(RUN_LOOP_INTERVAL_MS);
    // and back online
    await r.triggerConnected();
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(1);
    expect(offlineCount).to.equal(1);
  }

  {
    // Now clear onOnlineChange and test that it doesn't get called.
    onlineCount = offlineCount = 0;
    r.onOnlineChange = null;
    await r.triggerError('InvalidMessage', 'eee');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);
  }
});

test('onOnlineChange reflection on Reflect class', async () => {
  const f = () => 42;
  const r = reflectForTest({
    onOnlineChange: f,
  });
  await tickAFewTimes(clock);

  expect(r.onOnlineChange).to.equal(f);
});

test('disconnects if ping fails', async () => {
  const watchdogInterval = RUN_LOOP_INTERVAL_MS;
  const pingTimeout = 5000;
  const r = reflectForTest();

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);

  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  // Wait PING_INTERVAL_MS which will trigger a ping
  // Pings timeout after PING_TIMEOUT_MS so reply before that.
  await tickAFewTimes(clock, PING_INTERVAL_MS);
  expect((await r.socket).messages).to.deep.equal(['["ping",{}]']);

  await r.triggerPong();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  await r.triggerPong();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, pingTimeout);
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('createSocket', () => {
  const nowStub = sinon.stub(performance, 'now').returns(0);

  const t = (
    socketURL: string,
    baseCookie: NullableVersion,
    clientID: string,
    roomID: string,
    userID: string,
    auth: string | undefined,
    jurisdiction: 'eu' | undefined,
    lmid: number,
    debugPerf: boolean,
    expectedURL: string,
    expectedProtocol = '',
  ) => {
    const mockSocket = createSocket(
      socketURL,
      baseCookie,
      clientID,
      'testClientGroupID',
      roomID,
      userID,
      auth,
      jurisdiction,
      lmid,
      'wsidx',
      debugPerf,
      new LogContext('error', undefined, new TestLogSink()),
    ) as unknown as MockSocket;
    expect(`${mockSocket.url}`).equal(expectedURL);
    expect(mockSocket.protocol).equal(expectedProtocol);
  };

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    '',
    undefined,
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    1234,
    'clientID',
    'roomID',
    'userID',
    '',
    undefined,
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    1234,
    'clientID',
    'a/b',
    'userID',
    '',
    undefined,
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=a%2Fb&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    '',
    undefined,
    123,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    undefined,
    undefined,
    123,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    'auth with []',
    undefined,
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
    'auth%20with%20%5B%5D',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    'auth with []',
    'eu',
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx',
    'auth%20with%20%5B%5D',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    'auth with []',
    'eu',
    0,
    true,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx&debugPerf=true',
    'auth%20with%20%5B%5D',
  );

  nowStub.returns(456);
  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'userID',
    '',
    undefined,
    0,
    false,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx',
  );
});

test('pusher sends one mutation per push message', async () => {
  const t = async (
    pushes: {
      mutations: Mutation[];
      expectedMessages: number;
      clientGroupID?: string;
      requestID?: string;
    }[],
  ) => {
    const r = reflectForTest();
    await r.triggerConnected();

    const mockSocket = await r.socket;

    for (const push of pushes) {
      const {
        mutations,
        expectedMessages,
        clientGroupID,
        requestID = 'test-request-id',
      } = push;

      const pushReq: PushRequestV1 = {
        profileID: 'p1',
        clientGroupID: clientGroupID ?? (await r.clientGroupID),
        pushVersion: 1,
        schemaVersion: '1',
        mutations,
      };

      mockSocket.messages.length = 0;

      await r.pusher(pushReq, requestID);

      expect(mockSocket.messages).to.have.lengthOf(expectedMessages);

      for (const raw of mockSocket.messages) {
        const msg = valita.parse(JSON.parse(raw), pushMessageSchema);
        expect(msg[1].clientGroupID).to.equal(
          clientGroupID ?? (await r.clientGroupID),
        );
        expect(msg[1].mutations).to.have.lengthOf(1);
        expect(msg[1].requestID).to.equal(requestID);
      }
    }
  };

  await t([{mutations: [], expectedMessages: 0}]);
  await t([
    {
      mutations: [
        {clientID: 'c1', id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
      ],
      expectedMessages: 1,
    },
  ]);
  await t([
    {
      mutations: [
        {clientID: 'c1', id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
        {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 2},
        {clientID: 'c1', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
      ],
      expectedMessages: 3,
    },
  ]);

  // if for self client group skips [clientID, id] tuples already seen
  await t([
    {
      mutations: [
        {clientID: 'c1', id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
        {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 2},
        {clientID: 'c1', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
      ],
      expectedMessages: 3,
    },
    {
      mutations: [
        {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 2},
        {clientID: 'c1', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
        {clientID: 'c2', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
      ],
      expectedMessages: 1,
    },
  ]);

  // if not for self client group (i.e. mutation recovery) does not skip
  // [clientID, id] tuples already seen
  await t([
    {
      clientGroupID: 'c1',
      mutations: [
        {clientID: 'c1', id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
        {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 2},
        {clientID: 'c1', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
      ],
      expectedMessages: 3,
    },
    {
      clientGroupID: 'c1',
      mutations: [
        {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 2},
        {clientID: 'c1', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
        {clientID: 'c2', id: 2, name: 'mut1', args: {d: 3}, timestamp: 3},
      ],
      expectedMessages: 3,
    },
  ]);
});

test('pusher adjusts mutation timestamps to be unix timestamps', async () => {
  const r = reflectForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  clock.tick(300); // performance.now is 500, system time is startTime + 300

  const mutations = [
    {clientID: 'c1', id: 1, name: 'mut1', args: {d: 1}, timestamp: 100},
    {clientID: 'c2', id: 1, name: 'mut1', args: {d: 2}, timestamp: 200},
  ];
  const requestID = 'test-request-id';

  const pushReq: PushRequestV1 = {
    profileID: 'p1',
    clientGroupID: await r.clientGroupID,
    pushVersion: 1,
    schemaVersion: '1',
    mutations,
  };

  mockSocket.messages.length = 0;

  await r.pusher(pushReq, requestID);

  expect(mockSocket.messages).to.have.lengthOf(mutations.length);

  const msg0 = valita.parse(
    JSON.parse(mockSocket.messages[0]),
    pushMessageSchema,
  );
  expect(msg0[1].mutations[0].timestamp).to.equal(startTime + 100);
  const msg1 = valita.parse(
    JSON.parse(mockSocket.messages[1]),
    pushMessageSchema,
  );
  expect(msg1[1].mutations[0].timestamp).to.equal(startTime + 200);
});

test('puller with mutation recovery pull, success response', async () => {
  const r = reflectForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: 1,
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = r.puller(pullReq, 'test-request-id');

  await tickAFewTimes(clock);
  expect(mockSocket.messages.length).to.equal(1);
  expect(JSON.parse(mockSocket.messages[0])).to.deep.equal([
    'pull',
    {
      clientGroupID: 'test-client-group-id',
      cookie: 1,
      requestID: 'test-request-id',
    },
  ]);

  await r.triggerPullResponse({
    cookie: 2,
    requestID: 'test-request-id',
    lastMutationIDChanges: {cid1: 1},
  });

  const result = await resultPromise;

  expect(result).to.deep.equal({
    response: {
      cookie: 2,
      lastMutationIDChanges: {cid1: 1},
      patch: [],
    },
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
  });
});

test('puller with mutation recovery pull, response timeout', async () => {
  const r = reflectForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: 1,
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = r.puller(pullReq, 'test-request-id');

  await tickAFewTimes(clock);
  expect(mockSocket.messages.length).to.equal(1);
  expect(JSON.parse(mockSocket.messages[0])).to.deep.equal([
    'pull',
    {
      clientGroupID: 'test-client-group-id',
      cookie: 1,
      requestID: 'test-request-id',
    },
  ]);

  clock.tick(PULL_TIMEOUT_MS);

  let expectedE = undefined;
  try {
    await resultPromise;
  } catch (e) {
    expectedE = e;
  }
  expect(expectedE).property('message', 'Pull timed out');
});

test('puller with normal non-mutation recovery pull', async () => {
  const fetchStub = sinon.stub(window, 'fetch');
  const r = reflectForTest();
  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: await r.clientGroupID,
    cookie: 1,
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };

  const result = await r.puller(pullReq, 'test-request-id');
  expect(fetchStub.notCalled).true;
  expect(result).to.deep.equal({
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
  });
});

test('watchSmokeTest', async () => {
  const r = reflectForTest({
    roomID: 'watchSmokeTestRoom',
    mutators: {
      addData: async (
        tx: WriteTransaction,
        data: {[key: string]: JSONValue},
      ) => {
        for (const [key, value] of Object.entries(data)) {
          await tx.put(key, value);
        }
      },
      del: async (tx: WriteTransaction, key: string) => {
        await tx.del(key);
      },
    },
  });

  const spy = sinon.spy();
  const unwatch = r.experimentalWatch(spy);

  await r.mutate.addData({a: 1, b: 2});

  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'add',
        key: 'a',
        newValue: 1,
      },
      {
        op: 'add',
        key: 'b',
        newValue: 2,
      },
    ],
  ]);

  spy.resetHistory();
  await r.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await r.mutate.addData({a: 11});
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'change',
        key: 'a',
        newValue: 11,
        oldValue: 1,
      },
    ],
  ]);

  spy.resetHistory();
  await r.mutate.del('b');
  expect(spy.callCount).to.equal(1);
  expect(spy.lastCall.args).to.deep.equal([
    [
      {
        op: 'del',
        key: 'b',
        oldValue: 2,
      },
    ],
  ]);

  unwatch();

  spy.resetHistory();
  await r.mutate.addData({c: 6});
  expect(spy.callCount).to.equal(0);
});

test('poke log context includes requestID', async () => {
  const url = 'ws://example.com/';
  const log: unknown[][] = [];

  const {promise: foundRequestIDFromLogPromise, resolve} = resolver<string>();
  const logSink = {
    log(_level: LogLevel, context: Context | undefined, ..._args: unknown[]) {
      if (context?.requestID === 'test-request-id-poke') {
        resolve(context?.requestID);
      }
    },
  };

  const r = new TestReflect({
    socketOrigin: url,
    auth: '',
    userID: 'user-id',
    roomID: 'room-id',
    logSinks: [logSink],
    logLevel: 'debug',
  });

  log.length = 0;

  await r.triggerPoke({
    pokes: [
      {
        baseCookie: null,
        cookie: 1,
        lastMutationIDChanges: {c1: 1},
        patch: [],
        timestamp: 123456,
      },
    ],
    requestID: 'test-request-id-poke',
  });

  const foundRequestID = await foundRequestIDFromLogPromise;
  expect(foundRequestID).to.equal('test-request-id-poke');
});

test('Metrics', async () => {
  const fetchStub = sinon.stub(window, 'fetch');

  // This is just a smoke test -- it ensures that we send metrics once at startup.
  // Ideally we would run Reflect and put it into different error conditions and see
  // that the metrics are reported appropriately.

  const r = reflectForTest();
  await r.waitForConnectionState(ConnectionState.Connecting);
  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);

  for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
    await clock.tickAsync(PING_INTERVAL_MS);
    await r.triggerPong();
  }

  fetchStub.calledOnceWithExactly('https://example.com/api/metrics/v0/report', {
    method: 'POST',
    body: '{"series":[{"metric":"time_to_connect_ms","points":[[120,[0]]]}]}',
    keepalive: true,
  });
});

test('Authentication', async () => {
  const log: number[] = [];

  let authCounter = 0;

  const auth = () => {
    if (authCounter > 0) {
      log.push(Date.now());
    }

    if (authCounter++ > 3) {
      return `new-auth-token-${authCounter}`;
    }
    return 'auth-token';
  };

  const r = reflectForTest({auth});

  const emulateErrorWhenConnecting = async (
    tickMS: number,
    expectedAuthToken: string,
    expectedTimeOfCall: number,
  ) => {
    expect((await r.socket).protocol).equal(expectedAuthToken);
    await r.triggerError('Unauthorized', 'auth error ' + authCounter);
    expect(r.connectionState).equal(ConnectionState.Disconnected);
    await clock.tickAsync(tickMS);
    expect(log).length(1);
    expect(log[0]).equal(expectedTimeOfCall);
    log.length = 0;
  };

  await emulateErrorWhenConnecting(0, 'auth-token', startTime);
  await emulateErrorWhenConnecting(5_000, 'auth-token', startTime + 5_000);
  await emulateErrorWhenConnecting(5_000, 'auth-token', startTime + 10_000);
  await emulateErrorWhenConnecting(5_000, 'auth-token', startTime + 15_000);
  await emulateErrorWhenConnecting(
    5_000,
    'new-auth-token-5',
    startTime + 20_000,
  );
  await emulateErrorWhenConnecting(
    5_000,
    'new-auth-token-6',
    startTime + 25_000,
  );
  await emulateErrorWhenConnecting(
    5_000,
    'new-auth-token-7',
    startTime + 30_000,
  );

  let socket: MockSocket | undefined;
  {
    await r.waitForConnectionState(ConnectionState.Connecting);
    socket = await r.socket;
    expect(socket.protocol).equal('new-auth-token-8');
    await r.triggerConnected();
    await r.waitForConnectionState(ConnectionState.Connected);
    // getAuth should not be called again.
    expect(log).empty;
  }

  {
    // Ping/pong should happen every 5 seconds.
    await tickAFewTimes(clock, PING_INTERVAL_MS);
    expect((await r.socket).messages).deep.equal([
      JSON.stringify(['ping', {}]),
    ]);
    expect(r.connectionState).equal(ConnectionState.Connected);
    await r.triggerPong();
    expect(r.connectionState).equal(ConnectionState.Connected);
    // getAuth should not be called again.
    expect(log).empty;
    // Socket is kept as long as we are connected.
    expect(await r.socket).equal(socket);
  }
});

test('AuthInvalidated', async () => {
  // In steady state we can get an AuthInvalidated error if the tokens expire on the server.
  // At this point we should disconnect and reconnect with a new auth token.

  let authCounter = 1;

  const r = reflectForTest({
    auth: () => `auth-token-${authCounter++}`,
  });

  await r.triggerConnected();
  expect((await r.socket).protocol).equal('auth-token-1');

  await r.triggerError('AuthInvalidated', 'auth error');
  await r.waitForConnectionState(ConnectionState.Disconnected);

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect((await r.socket).protocol).equal('auth-token-2');
});

test('Disconnect on error', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await r.triggerError('ClientNotFound', 'client not found');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('No backoff on errors', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  const step = async (delta: number, message: string) => {
    await r.triggerError('ClientNotFound', message);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);

    await clock.tickAsync(delta - 1);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);
    await clock.tickAsync(1);
    expect(r.connectionState).to.equal(ConnectionState.Connecting);
  };

  const steps = async () => {
    await step(5_000, 'a');
    await step(5_000, 'a');
    await step(5_000, 'a');
    await step(5_000, 'a');
  };

  await steps();

  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await steps();
});

test('Ping pong', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await clock.tickAsync(PING_INTERVAL_MS - 1);
  expect((await r.socket).messages).empty;
  await clock.tickAsync(1);

  expect((await r.socket).messages).deep.equal([JSON.stringify(['ping', {}])]);
  await clock.tickAsync(PING_TIMEOUT_MS - 1);
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await clock.tickAsync(1);

  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('Ping timeout', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await clock.tickAsync(PING_INTERVAL_MS - 1);
  expect((await r.socket).messages).empty;
  await clock.tickAsync(1);
  expect((await r.socket).messages).deep.equal([JSON.stringify(['ping', {}])]);
  await clock.tickAsync(PING_TIMEOUT_MS - 1);
  await r.triggerPong();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await clock.tickAsync(1);
  expect(r.connectionState).to.equal(ConnectionState.Connected);
});

test('Connect timeout', async () => {
  const r = reflectForTest();

  await r.waitForConnectionState(ConnectionState.Connecting);

  const step = async (sleepMS: number) => {
    // Need to drain the microtask queue without changing the clock because we are
    // using the time below to check when the connect times out.
    for (let i = 0; i < 10; i++) {
      await clock.tickAsync(0);
    }

    expect(r.connectionState).to.equal(ConnectionState.Connecting);
    await clock.tickAsync(CONNECT_TIMEOUT_MS - 1);
    expect(r.connectionState).to.equal(ConnectionState.Connecting);
    await clock.tickAsync(1);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);

    // We got disconnected so we sleep for RUN_LOOP_INTERVAL_MS before trying again

    await clock.tickAsync(sleepMS - 1);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);
    await clock.tickAsync(1);
    expect(r.connectionState).to.equal(ConnectionState.Connecting);
  };

  await step(RUN_LOOP_INTERVAL_MS);

  // Try again to connect
  await step(RUN_LOOP_INTERVAL_MS);
  await step(RUN_LOOP_INTERVAL_MS);
  await step(RUN_LOOP_INTERVAL_MS);

  // And success after this...
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
});

test('Logs errors in connect', async () => {
  const log: [LogLevel, unknown[]][] = [];

  const r = reflectForTest({
    logSinks: [
      {
        log: (level, ...args) => {
          log.push([level, args]);
        },
      },
    ],
  });
  await r.triggerError('ClientNotFound', 'client-id-a');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);

  const index = log.findIndex(
    ([level, args]) =>
      level === 'error' && args.find(arg => /client-id-a/.test(String(arg))),
  );

  expect(index).to.not.equal(-1);
});

test('New connection logs', async () => {
  const log: [LogLevel, unknown[]][] = [];
  clock.setSystemTime(1000);
  const r = reflectForTest({
    logSinks: [
      {
        log: (level, ...args) => {
          log.push([level, args]);
        },
      },
    ],
  });
  await r.waitForConnectionState(ConnectionState.Connecting);
  await clock.tickAsync(500);
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await clock.tickAsync(500);
  await r.triggerPong();
  await r.triggerClose();
  await r.waitForConnectionState(ConnectionState.Disconnected);
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  const connectIndex = log.findIndex(
    ([level, args]) =>
      level === 'info' &&
      args.find(arg => /Connected/.test(String(arg))) &&
      args.find(
        arg =>
          arg instanceof Object &&
          (arg as {timeToConnectMs: number}).timeToConnectMs === 500,
      ),
  );

  const disconnectIndex = log.findIndex(
    ([level, args]) =>
      level === 'info' &&
      args.find(arg => /disconnecting/.test(String(arg))) &&
      args.find(
        arg =>
          arg instanceof Object &&
          (arg as {connectedAt: number}).connectedAt === 1500 &&
          (arg as {connectionDuration: number}).connectionDuration === 500 &&
          (arg as {messageCount: number}).messageCount === 2,
      ),
  );
  expect(connectIndex).to.not.equal(-1);
  expect(disconnectIndex).to.not.equal(-1);
});

async function testWaitsForConnection(
  fn: (r: TestReflect<MutatorDefs>) => Promise<unknown>,
) {
  const r = reflectForTest();

  const log: ('resolved' | 'rejected')[] = [];

  await r.triggerError('ClientNotFound', 'client-id-a');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  fn(r).then(
    () => log.push('resolved'),
    () => log.push('rejected'),
  );

  await tickAFewTimes(clock);

  // Rejections that happened in previous connect should not reject pusher.
  expect(log).to.deep.equal([]);

  await clock.tickAsync(RUN_LOOP_INTERVAL_MS);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);

  await r.triggerError('ClientNotFound', 'client-id-a');
  await tickAFewTimes(clock);
  expect(log).to.deep.equal(['rejected']);
}

test('pusher waits for connection', async () => {
  await testWaitsForConnection(async r => {
    const pushReq: PushRequestV1 = {
      profileID: 'p1',
      clientGroupID: await r.clientGroupID,
      pushVersion: 1,
      schemaVersion: '1',
      mutations: [],
    };
    return r.pusher(pushReq, 'request-id');
  });
});

test('puller waits for connection', async () => {
  await testWaitsForConnection(r => {
    const pullReq: PullRequestV1 = {
      profileID: 'test-profile-id',
      clientGroupID: 'test-client-group-id',
      cookie: 1,
      pullVersion: 1,
      schemaVersion: r.schemaVersion,
    };
    return r.puller(pullReq, 'request-id');
  });
});

test('Protocol mismatch', async () => {
  const fake = sinon.fake();
  const r = reflectForTest();
  r.onUpdateNeeded = fake;

  await r.triggerError('VersionNotSupported', 'prot mismatch');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;
  expect(fake.firstCall.args).deep.equal([{type: 'VersionNotSupported'}]);

  fake.resetHistory();
  r.onUpdateNeeded = null;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  expect(fake.called).false;
});

test('server ahead', async () => {
  const sink = new TestLogSink();
  const {promise, resolve} = resolver();
  const storage: Record<string, string> = {};
  sinon.replaceGetter(window, 'localStorage', () => storage as Storage);
  const r = reflectForTest({
    logSinks: [sink],
  });
  r.reload = resolve;

  await r.triggerError(
    'InvalidConnectionRequestBaseCookie',
    'unexpected BaseCookie',
  );
  await promise;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).to.equal(
    serverAheadReloadReason('InvalidConnectionRequestBaseCookie'),
  );
});

test('Constructing Reflect with a negative hiddenTabDisconnectDelay option throws an error', () => {
  let expected;
  try {
    reflectForTest({hiddenTabDisconnectDelay: -1});
  } catch (e) {
    expected = e;
  }
  expect(expected)
    .instanceOf(Error)
    .property(
      'message',
      'ReflectOptions.hiddenTabDisconnectDelay must not be negative.',
    );
});

suite('Disconnect on hide', () => {
  type Case = {
    name: string;
    hiddenTabDisconnectDelay?: number | undefined;
    test: (
      r: TestReflect<MutatorDefs>,
      changeVisibilityState: (
        newVisibilityState: DocumentVisibilityState,
      ) => void,
    ) => Promise<void>;
  };

  const cases: Case[] = [
    {
      name: 'default delay not during ping',
      test: async (r, changeVisibilityState) => {
        expect(PING_INTERVAL_MS).lessThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        expect(PING_INTERVAL_MS * 2).greaterThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await clock.tickAsync(PING_INTERVAL_MS); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS;
        await r.triggerPong();
        await clock.tickAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'default delay during ping',
      test: async (r, changeVisibilityState) => {
        expect(PING_INTERVAL_MS).lessThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        expect(PING_INTERVAL_MS + PING_TIMEOUT_MS).greaterThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        await clock.tickAsync(PING_INTERVAL_MS / 2);
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await clock.tickAsync(PING_INTERVAL_MS / 2); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS / 2;
        await clock.tickAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionState).to.equal(ConnectionState.Connected);
        await r.triggerPong();
        await clock.tickAsync(0);
      },
    },
    {
      name: 'custom delay longer than ping interval not during ping',
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 6.3),
      test: async (r, changeVisibilityState) => {
        let timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 6.3);
        changeVisibilityState('hidden');
        while (timeTillHiddenDisconnect > PING_INTERVAL_MS) {
          await clock.tickAsync(PING_INTERVAL_MS); // sends ping
          timeTillHiddenDisconnect -= PING_INTERVAL_MS;
          await r.triggerPong();
        }
        await clock.tickAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'custom delay longer than ping interval during ping',
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 6.3),
      test: async (r, changeVisibilityState) => {
        let timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 6.3);
        expect(timeTillHiddenDisconnect > PING_INTERVAL_MS + PING_TIMEOUT_MS);
        changeVisibilityState('hidden');
        while (timeTillHiddenDisconnect > PING_INTERVAL_MS + PING_TIMEOUT_MS) {
          await clock.tickAsync(PING_INTERVAL_MS);
          timeTillHiddenDisconnect -= PING_INTERVAL_MS;
          await r.triggerPong();
        }
        expect(timeTillHiddenDisconnect).lessThan(
          PING_INTERVAL_MS + PING_TIMEOUT_MS,
        );
        expect(timeTillHiddenDisconnect).greaterThan(PING_INTERVAL_MS);
        await clock.tickAsync(PING_INTERVAL_MS); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS;
        await clock.tickAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionState).to.equal(ConnectionState.Connected);
        await r.triggerPong();
        await clock.tickAsync(0);
      },
    },
    {
      name: 'custom delay shorter than ping interval not during ping',
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await clock.tickAsync(PING_INTERVAL_MS);
        await r.triggerPong();
        const timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 0.3);
        changeVisibilityState('hidden');
        await clock.tickAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'custom delay shorter than ping interval during ping',
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await clock.tickAsync(PING_INTERVAL_MS);
        const timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 0.3);
        changeVisibilityState('hidden');
        await clock.tickAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionState).to.equal(ConnectionState.Connected);
        await r.triggerPong();
        await clock.tickAsync(0);
      },
    },
    {
      name: 'custom delay 0, not during ping',
      hiddenTabDisconnectDelay: 0,
      test: async (r, changeVisibilityState) => {
        await clock.tickAsync(PING_INTERVAL_MS);
        await r.triggerPong();
        changeVisibilityState('hidden');
        await clock.tickAsync(0);
      },
    },
    {
      name: 'custom delay 0, during ping',
      hiddenTabDisconnectDelay: 0,
      test: async (r, changeVisibilityState) => {
        await clock.tickAsync(PING_INTERVAL_MS);
        changeVisibilityState('hidden');
        await clock.tickAsync(0);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionState).to.equal(ConnectionState.Connected);
        await r.triggerPong();
        await clock.tickAsync(0);
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const {hiddenTabDisconnectDelay} = c;

      let visibilityState = 'visible';
      sinon.stub(document, 'visibilityState').get(() => visibilityState);
      const changeVisibilityState = (
        newVisibilityState: DocumentVisibilityState,
      ) => {
        assert(visibilityState !== newVisibilityState);
        visibilityState = newVisibilityState;
        document.dispatchEvent(new Event('visibilitychange'));
      };

      const r = reflectForTest({
        hiddenTabDisconnectDelay,
      });
      const makeOnOnlineChangePromise = () =>
        new Promise(resolve => {
          r.onOnlineChange = resolve;
        });
      let onOnlineChangeP = makeOnOnlineChangePromise();

      await r.triggerConnected();
      expect(r.connectionState).to.equal(ConnectionState.Connected);
      expect(await onOnlineChangeP).true;
      expect(r.online).true;

      onOnlineChangeP = makeOnOnlineChangePromise();

      await c.test(r, changeVisibilityState);

      expect(r.connectionState).to.equal(ConnectionState.Disconnected);
      expect(await onOnlineChangeP).false;
      expect(r.online).false;

      // Stays disconnected as long as we are hidden.
      while (Date.now() < 100_000) {
        await clock.tickAsync(1_000);
        expect(r.connectionState).to.equal(ConnectionState.Disconnected);
        expect(r.online).false;
        expect(document.visibilityState).to.equal('hidden');
      }

      onOnlineChangeP = makeOnOnlineChangePromise();

      visibilityState = 'visible';
      document.dispatchEvent(new Event('visibilitychange'));

      await r.waitForConnectionState(ConnectionState.Connecting);
      await r.triggerConnected();
      expect(r.connectionState).to.equal(ConnectionState.Connected);
      expect(await onOnlineChangeP).true;
      expect(r.online).true;
    });
  }
});

test('InvalidConnectionRequest', async () => {
  const testLogSink = new TestLogSink();
  const r = reflectForTest({
    logSinks: [testLogSink],
  });
  await r.triggerError('InvalidConnectionRequest', 'test');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);
  const msg = testLogSink.messages.at(-1);
  assert(msg);

  expect(msg[0]).equal('error');

  const err = msg[2].at(-2);
  assert(err instanceof ServerError);
  expect(err.message).equal('InvalidConnectionRequest: test');

  const data = msg[2].at(-1);
  expect(data).deep.equal({
    lmid: 0,
    baseCookie: null,
  });
});

suite('Invalid Downstream message', () => {
  type Case = {
    name: string;
    duringPing: boolean;
  };

  const cases: Case[] = [
    {name: 'no ping', duringPing: false},
    {name: 'during ping', duringPing: true},
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const testLogSink = new TestLogSink();
      const r = reflectForTest({
        logSinks: [testLogSink],
        logLevel: 'debug',
      });
      await r.triggerConnected();
      expect(r.connectionState).to.equal(ConnectionState.Connected);

      if (c.duringPing) {
        await waitForUpstreamMessage(r, 'ping', clock);
      }

      await r.triggerPoke({
        pokes: [
          {
            baseCookie: null,
            cookie: 1,
            lastMutationIDChanges: {c1: 1},
            // @ts-expect-error - invalid field
            patch: [{op: 'put', key: 'k1', valueXXX: 'v1'}],
            timestamp: 123456,
          },
        ],
        requestID: 'test-request-id-poke',
      });
      await clock.tickAsync(0);

      if (c.duringPing) {
        await r.triggerPong();
      }

      expect(r.online).eq(true);
      expect(r.connectionState).eq(ConnectionState.Connected);

      const found = testLogSink.messages.some(m =>
        m[2].some(
          v => v instanceof Error && v.message.includes('Invalid union value.'),
        ),
      );
      expect(found).true;
    });
  }
});

test('experimentalKVStore', async () => {
  const r1 = reflectForTest({
    mutators: {
      putFoo: async (tx, val: string) => {
        await tx.put('foo', val);
      },
    },
  });
  await r1.mutate.putFoo('bar');
  expect(await r1.query(tx => tx.get('foo'))).to.equal('bar');
  expect(await idbExists(r1.idbName)).is.true;

  const r2 = reflectForTest({
    createKVStore: name => new ExperimentalMemKVStore(name),
    mutators: {
      putFoo: async (tx, val: string) => {
        await tx.put('foo', val);
      },
    },
  });
  await r2.mutate.putFoo('bar');
  expect(await r2.query(tx => tx.get('foo'))).to.equal('bar');
  expect(await idbExists(r2.idbName)).is.false;
});

test('Close during connect should sleep', async () => {
  const testLogSink = new TestLogSink();
  const r = reflectForTest({
    logSinks: [testLogSink],
    logLevel: 'debug',
  });

  await r.triggerConnected();

  await r.waitForConnectionState(ConnectionState.Connected);
  await clock.tickAsync(0);
  expect(r.online).equal(true);

  (await r.socket).close();
  await r.waitForConnectionState(ConnectionState.Disconnected);
  await r.waitForConnectionState(ConnectionState.Connecting);

  (await r.socket).close();
  await r.waitForConnectionState(ConnectionState.Disconnected);
  await clock.tickAsync(0);
  expect(r.online).equal(false);
  const hasSleeping = testLogSink.messages.some(m =>
    m[2].some(v => v === 'Sleeping'),
  );
  expect(hasSleeping).true;

  await clock.tickAsync(RUN_LOOP_INTERVAL_MS);

  await r.waitForConnectionState(ConnectionState.Connecting);
  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);
  await clock.tickAsync(0);
  expect(r.online).equal(true);
});
