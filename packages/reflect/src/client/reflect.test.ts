import {expect} from '@esm-bundle/chai';
import * as sinon from 'sinon';
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
