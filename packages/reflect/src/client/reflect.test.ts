import {expect} from '@esm-bundle/chai';
import type {
  JSONValue,
  LogLevel,
  PushRequest,
  WriteTransaction,
} from 'replicache';
import * as sinon from 'sinon';
import {Mutation, pushMessageSchema} from '../protocol/push.js';
import type {NullableVersion} from '../types/version.js';
import {resolver} from '@rocicorp/resolver';
import {
  CloseKind,
  ConnectionState,
  CONNECT_TIMEOUT_MS,
  createSocket,
  MAX_RUN_LOOP_INTERVAL_MS,
  PING_INTERVAL_MS,
  PING_TIMEOUT_MS,
  RUN_LOOP_INTERVAL_MS,
} from './reflect.js';
import {
  MockSocket,
  reflectForTest,
  TestReflect,
  tickAFewTimes,
} from './test-utils.js';
// Why use fakes when we can use the real thing!
import {Metrics, gaugeValue, DatadogSeries} from '@rocicorp/datadog-util';
import {camelToSnake, DID_NOT_CONNECT_VALUE, Metric} from './metrics.js';
import {ErrorKind} from '../protocol/error.js';

let clock: sinon.SinonFakeTimers;

setup(() => {
  clock = sinon.useFakeTimers();
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
    await r.triggerError(ErrorKind.InvalidMessage, 'aaa');
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
    await r.triggerError(ErrorKind.Unauthorized, 'bbb');
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
    await r.triggerError(ErrorKind.Unauthorized, 'ccc');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).true;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);

    await r.waitForConnectionState(ConnectionState.Connecting);
    await r.triggerError(ErrorKind.Unauthorized, 'ddd');
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
    await r.triggerError(ErrorKind.InvalidMessage, 'eee');
    await r.waitForConnectionState(ConnectionState.Disconnected);
    await clock.tickAsync(0);
    expect(r.online).false;
    expect(onlineCount).to.equal(0);
    expect(offlineCount).to.equal(0);
  }

  await r.close();
});

test('onOnlineChange reflection on Reflect class', async () => {
  const f = () => 42;
  const r = reflectForTest({
    onOnlineChange: f,
  });
  await tickAFewTimes(clock);

  expect(r.onOnlineChange).to.equal(f);
  await r.close();
});

test('disconnects if ping fails', async () => {
  const watchdogInterval = RUN_LOOP_INTERVAL_MS;
  const pingTimeout = 2000;
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

  await r.close();
});

test('createSocket', () => {
  const nowStub = sinon.stub(performance, 'now').returns(0);

  const t = (
    socketURL: string,
    baseCookie: NullableVersion,
    clientID: string,
    roomID: string,
    auth: string,
    lmid: number,
    expectedURL: string,
    expectedProtocol = '',
  ) => {
    const mockSocket = createSocket(
      socketURL,
      baseCookie,
      clientID,
      roomID,
      auth,
      lmid,
      'wsidx',
    ) as unknown as MockSocket;
    expect(`${mockSocket.url}`).equal(expectedURL);
    expect(mockSocket.protocol).equal(expectedProtocol);
  };

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    1234,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    123,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'auth with []',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
    'auth%20with%20%5B%5D',
  );

  nowStub.returns(456);
  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=456&lmid=0&wsid=wsidx',
  );
});

test('pusher sends one mutation per push message', async () => {
  const t = async (
    mutations: Mutation[],
    expectedMessages: number,
    requestID = 'request-id',
  ) => {
    const r = reflectForTest();
    await r.triggerConnected();
    const mockSocket = await r.socket;

    const pushBody: PushRequest = {
      profileID: 'profile-id',
      clientID: 'client-id',
      pushVersion: 0,
      schemaVersion: '1',
      mutations,
    };

    const req = new Request('http://example.com/push', {
      body: JSON.stringify(pushBody),
      method: 'POST',
      headers: {'X-Replicache-RequestID': requestID},
    });

    await r.pusher(req);

    expect(mockSocket.messages).to.have.lengthOf(expectedMessages);

    for (const raw of mockSocket.messages) {
      const msg = pushMessageSchema.parse(JSON.parse(raw));
      expect(msg[1].mutations).to.have.lengthOf(1);
      expect(msg[1].requestID).to.equal(requestID);
    }

    await r.close();
  };

  await t([], 0);
  await t([{id: 1, name: 'mut1', args: {d: 1}, timestamp: 1}], 1);
  await t(
    [
      {id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
      {id: 2, name: 'mut1', args: {d: 2}, timestamp: 2},
      {id: 3, name: 'mut1', args: {d: 3}, timestamp: 3},
    ],
    3,
  );

  // skips ids already seen
  await t(
    [
      {id: 1, name: 'mut1', args: {d: 1}, timestamp: 1},
      {id: 1, name: 'mut1', args: {d: 2}, timestamp: 1},
    ],
    1,
  );
});

test('watchSmokeTest', async () => {
  const rep = reflectForTest({
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
  const unwatch = rep.experimentalWatch(spy);

  await rep.mutate.addData({a: 1, b: 2});

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
  await rep.mutate.addData({a: 1, b: 2});
  expect(spy.callCount).to.equal(0);

  await rep.mutate.addData({a: 11});
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
  await rep.mutate.del('b');
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
  await rep.mutate.addData({c: 6});
  expect(spy.callCount).to.equal(0);
});

test('poke log context includes requestID', async () => {
  const url = 'ws://example.com/';
  const log: unknown[][] = [];

  const {promise: foundRequestIDFromLogPromise, resolve} = resolver<string>();
  const logSink = {
    log(_level: LogLevel, ...args: unknown[]) {
      for (const arg of args) {
        if (typeof arg === 'string' && arg.startsWith('requestID=')) {
          const foundRequestID = arg.slice('requestID='.length);
          resolve(foundRequestID);
        }
      }
    },
  };

  const reflect = new TestReflect({
    socketOrigin: url,
    auth: '',
    userID: 'user-id',
    roomID: 'room-id',
    logSinks: [logSink],
    logLevel: 'debug',
  });

  log.length = 0;

  await reflect.triggerPoke({
    baseCookie: null,
    cookie: 1,
    lastMutationID: 1,
    patch: [],
    timestamp: 123456,
    requestID: 'request-id-x',
  });

  const foundRequestID = await foundRequestIDFromLogPromise;
  expect(foundRequestID).to.equal('request-id-x');
});

test('metrics updated when connected', async () => {
  const m = new Metrics();
  const ttc = m.gauge(Metric.TimeToConnectMs);
  const lce = m.state(Metric.LastConnectError);
  clock.setSystemTime(1000 * 1000);
  const r = reflectForTest({
    metrics: m,
  });
  expect(val(ttc)?.value).to.equal(DID_NOT_CONNECT_VALUE);
  expect(val(lce)).to.be.undefined;

  await r.waitForConnectionState(ConnectionState.Connecting);

  const start = asNumber(r.connectingStart);

  clock.setSystemTime(start + 42 * 1000);
  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);

  expect(val(ttc)?.value).to.equal(42 * 1000);
  expect(val(lce)).to.be.undefined;

  // Ensure TimeToConnect gets set when we reconnect.
  await r.triggerClose();
  await r.waitForConnectionState(ConnectionState.Disconnected);
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await r.waitForConnectionState(ConnectionState.Connecting);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);

  const restart = asNumber(r.connectingStart);
  clock.setSystemTime(restart + 666 * 1000);
  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);
  // Gauge value is in seconds.
  expect(val(ttc)?.value).to.equal(666 * 1000);
  expect(val(lce)).to.be.undefined;
});

function val(g: {flush(): DatadogSeries | undefined}):
  | {
      metric: string;
      tsSec: number;
      value: number;
    }
  | undefined {
  const series = g.flush();
  return series && gaugeValue(series);
}

test('metrics when connect fails', async () => {
  const m = new Metrics();
  const ttc = m.gauge(Metric.TimeToConnectMs);
  const lce = m.state(Metric.LastConnectError);
  clock.setSystemTime(1000 * 1000);
  const r = reflectForTest({
    metrics: m,
  });

  // Trigger a close while still connecting.
  await r.waitForConnectionState(ConnectionState.Connecting);
  await r.triggerClose();
  await tickAFewTimes(clock, RUN_LOOP_INTERVAL_MS);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);
  expect(val(ttc)?.value).to.equal(DID_NOT_CONNECT_VALUE);
  let gotLceVal = val(lce);
  expect(gotLceVal?.metric).to.equal(
    [
      camelToSnake(Metric.LastConnectError),
      camelToSnake(CloseKind.AbruptClose),
    ].join('_'),
  );
  expect(gotLceVal?.value).to.equal(1);

  // Trigger an error while still connecting.
  const start = asNumber(r.connectingStart);
  clock.setSystemTime(start + 42 * 1000);
  await r.triggerError(ErrorKind.Unauthorized, 'boom');
  await tickAFewTimes(clock);
  expect(val(ttc)?.value).to.equal(DID_NOT_CONNECT_VALUE);
  gotLceVal = val(lce);
  expect(gotLceVal?.metric).to.equal(
    [
      camelToSnake(Metric.LastConnectError),
      camelToSnake(ErrorKind.Unauthorized),
    ].join('_'),
  );

  // Ensure LastConnectError gets cleared when we successfully reconnect.
  await tickAFewTimes(clock, RUN_LOOP_INTERVAL_MS);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);
  await r.triggerConnected();
  await tickAFewTimes(clock);
  expect(val(lce)).to.be.undefined;
});

function asNumber(v: unknown): number {
  if (typeof v !== 'number') {
    throw new Error('not a number');
  }
  return v;
}

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
    await r.triggerError(ErrorKind.Unauthorized, 'auth error ' + authCounter);
    expect(r.connectionState).equal(ConnectionState.Disconnected);
    await clock.tickAsync(tickMS);
    expect(log).length(1);
    expect(log[0]).equal(expectedTimeOfCall);
    log.length = 0;
  };

  await emulateErrorWhenConnecting(0, 'auth-token', 0);
  await emulateErrorWhenConnecting(5_000, 'auth-token', 5_000);
  await emulateErrorWhenConnecting(10_000, 'auth-token', 15_000);
  await emulateErrorWhenConnecting(20_000, 'auth-token', 35_000);
  await emulateErrorWhenConnecting(40_000, 'new-auth-token-5', 75_000);
  // Clamped at MAX_WATCHDOG_INTERVAL_MS.
  await emulateErrorWhenConnecting(60_000, 'new-auth-token-6', 135_000);
  await emulateErrorWhenConnecting(60_000, 'new-auth-token-7', 195_000);

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

  await r.close();
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

  await r.triggerError(ErrorKind.AuthInvalidated, 'auth error');
  await r.waitForConnectionState(ConnectionState.Disconnected);

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect((await r.socket).protocol).equal('auth-token-2');

  await r.close();
});

test('Disconnect on error', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await r.triggerError(ErrorKind.ClientNotFound, 'client not found');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await r.close();
});

test('Backoff on errors', async () => {
  const r = reflectForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  const step = async (delta: number, message: string) => {
    await r.triggerError(ErrorKind.ClientNotFound, message);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);

    await clock.tickAsync(delta - 1);
    expect(r.connectionState).to.equal(ConnectionState.Disconnected);
    await clock.tickAsync(1);
    expect(r.connectionState).to.equal(ConnectionState.Connecting);
  };

  const steps = async () => {
    await step(5_000, 'a');
    await step(10_000, 'b');
    await step(20_000, 'c');
    await step(40_000, 'd');
    expect(MAX_RUN_LOOP_INTERVAL_MS).equal(60_000);
    await step(60_000, 'e');
    await step(60_000, 'f');
  };

  await steps();

  // success resets the backoff.
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await steps();

  await r.close();
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

  await r.close();
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

  await r.close();
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
  await step(2 * RUN_LOOP_INTERVAL_MS);
  await step(4 * RUN_LOOP_INTERVAL_MS);
  await step(8 * RUN_LOOP_INTERVAL_MS);
  expect(MAX_RUN_LOOP_INTERVAL_MS).equal(60_000);
  await step(60_000);

  // And success after this...
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await r.close();
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
  await r.triggerError(ErrorKind.ClientNotFound, 'client-id-a');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);

  const index = log.findIndex(
    ([level, args]) =>
      level === 'error' && args.find(arg => /client-id-a/.test(String(arg))),
  );

  expect(index).to.not.equal(-1);

  await r.close();
});
