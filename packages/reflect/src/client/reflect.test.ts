import {expect} from '@esm-bundle/chai';
import type {JSONValue, WriteTransaction} from 'replicache';
import * as sinon from 'sinon';
import {Mutation, pushMessageSchema, PushBody} from '../protocol/push.js';
import type {NullableVersion} from '../types/version.js';
import {ConnectionState, createSocket} from './reflect.js';
import {MockSocket, reflectForTest, tickAFewTimes} from './test-utils.js';

let clock: sinon.SinonFakeTimers;

setup(() => {
  clock = sinon.useFakeTimers();
});

teardown(() => {
  sinon.restore();
});

test('onOnlineChange callback', async () => {
  let onlineCount = 0;
  let offlineCount = 0;

  const r = reflectForTest({
    onOnlineChange: (online) => {
      if (online) {
        onlineCount++;
      } else {
        offlineCount++;
      }
    },
  });

  expect(r.connectionState).to.equal(ConnectionState.Connecting);
  expect(onlineCount).to.equal(0);
  expect(offlineCount).to.equal(0);

  await tickAFewTimes(clock);
  r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  expect(onlineCount).to.equal(1);
  expect(offlineCount).to.equal(0);

  await tickAFewTimes(clock);
  r.triggerClose();
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  expect(onlineCount).to.equal(1);
  expect(offlineCount).to.equal(1);

  // let the watchdog timer fire
  await tickAFewTimes(clock, 5000);
  r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  expect(onlineCount).to.equal(2);
  expect(offlineCount).to.equal(1);

  r.onOnlineChange = null;
  await tickAFewTimes(clock);
  r.triggerClose();
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  expect(onlineCount).to.equal(2);
  expect(offlineCount).to.equal(1);

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
  const watchdogInterval = 5000;
  const pingTimeout = 2000;
  const r = reflectForTest();

  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);

  r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  r.triggerPong();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  r.triggerPong();
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
    expectedProtocol?: string,
  ) => {
    const mockSocket = createSocket(
      socketURL,
      baseCookie,
      clientID,
      roomID,
      auth,
      lmid,
      // @ts-expect-error MockSocket is not compatible with WebSocket
      MockSocket,
    ) as unknown as MockSocket;
    expect(mockSocket.args).to.deep.equal([expectedURL, expectedProtocol]);
  };

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=0',
  );

  t(
    'ws://example.com/',
    1234,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=1234&ts=0&lmid=0',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    123,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=123',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'auth with []',
    0,
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=0&lmid=0',
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
    'ws://example.com/connect?clientID=clientID&roomID=roomID&baseCookie=&ts=456&lmid=0',
  );
});

test('pusher sends one mutation per push message', async () => {
  const t = async (mutations: Mutation[], expectedMessages: number) => {
    const r = reflectForTest();
    await tickAFewTimes(clock);
    r.triggerConnected();
    const mockSocket = r.socket as unknown as MockSocket;

    const overwrittenAndNotUsedLol = 42;

    const pushBody: PushBody = {
      pushVersion: 0,
      schemaVersion: '1',
      mutations,
      timestamp: overwrittenAndNotUsedLol,
    };

    const req = new Request('http://example.com/push', {
      body: JSON.stringify(pushBody),
      method: 'POST',
    });

    await r.pusher(req);

    expect(mockSocket.messages).to.have.lengthOf(expectedMessages);

    for (const raw of mockSocket.messages) {
      const msg = pushMessageSchema.parse(JSON.parse(raw));
      expect(msg[1].mutations).to.have.lengthOf(1);
    }

    await r.close();
    await tickAFewTimes(clock);
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
