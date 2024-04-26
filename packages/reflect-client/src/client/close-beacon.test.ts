import {LogContext} from '@rocicorp/logger';
import {resetAllConfig, setConfig} from 'reflect-shared/src/config.js';
import * as sinon from 'sinon';
import {afterEach, beforeEach, expect, suite, test} from 'vitest';
import {CloseBeaconManager} from './close-beacon.js';
import {TestLogSink} from './test-utils.js';

beforeEach(() => {
  setConfig('closeBeacon', true);
});

afterEach(() => {
  sinon.restore();
  resetAllConfig();
});

test('sendCloseBeacon', () => {
  const fetchStub = sinon
    .stub(globalThis, 'fetch')
    .returns(Promise.resolve(new Response()));
  const sink = new TestLogSink();
  const lc = new LogContext('debug', {}, sink);
  const server = 'http://localhost:8080';
  const roomID = 'roomID1';
  const userID = 'userID1';
  const clientID = 'clientID1';
  const auth = () => 'auth1';
  const lastMutationID = () => 1;

  const m = new CloseBeaconManager(
    {roomID, userID, clientID},
    lc,
    server,
    auth,
    lastMutationID,
    new AbortController().signal,
    undefined,
  );
  m.send('Pagehide');

  expect(fetchStub.calledOnce).equal(true);
  expect(fetchStub.firstCall.args[0].toString()).equal(
    'http://localhost:8080/api/sync/v1/close?roomID=roomID1&userID=userID1&clientID=clientID1',
  );
  expect(fetchStub.firstCall.args[1]).deep.equal({
    body: '{"lastMutationID":1}',
    headers: {
      'authorization': 'Bearer auth1',
      'content-type': 'application/json',
    },
    keepalive: true,
    method: 'POST',
  });
});

suite('sendCloseBeacon no auth', () => {
  for (const auth of [undefined, ''] as const) {
    test(typeof auth, () => {
      const fetchStub = sinon
        .stub(globalThis, 'fetch')
        .returns(Promise.resolve(new Response()));
      const sink = new TestLogSink();
      const lc = new LogContext('debug', {}, sink);
      const server = 'http://localhost:8080';
      const roomID = 'roomID2';
      const userID = 'userID2';
      const clientID = 'clientID2';
      const localAuth = () => auth;
      const lastMutationID = () => 2;

      const m = new CloseBeaconManager(
        {roomID, userID, clientID},
        lc,
        server,
        localAuth,
        lastMutationID,
        new AbortController().signal,
        undefined,
      );
      m.send('Pagehide');

      expect(fetchStub.calledOnce).equal(true);
      expect(fetchStub.firstCall.args[0].toString()).equal(
        'http://localhost:8080/api/sync/v1/close?roomID=roomID2&userID=userID2&clientID=clientID2',
      );
      expect(fetchStub.firstCall.args[1]).deep.equal({
        body: '{"lastMutationID":2}',
        headers: {
          'content-type': 'application/json',
        },
        keepalive: true,
        method: 'POST',
      });
    });
  }
});

test('sendCloseBeacon no server is a noop', () => {
  const fetchStub = sinon
    .stub(globalThis, 'fetch')
    .returns(Promise.resolve(new Response()));
  const sink = new TestLogSink();
  const lc = new LogContext('debug', {}, sink);
  const server = null;
  const roomID = 'roomID3';
  const userID = 'userID3';
  const clientID = 'clientID3';
  const auth = () => undefined;
  const lastMutationID = () => 3;

  const m = new CloseBeaconManager(
    {roomID, userID, clientID},
    lc,
    server,
    auth,
    lastMutationID,
    new AbortController().signal,
    undefined,
  );
  m.send('Pagehide');

  expect(fetchStub.called).equal(false);
});

suite('initCloseBeaconForPageHide', () => {
  const cases = [
    {persisted: true, expectedCalledOnce: false},
    {
      persisted: false,
      expectedCalledOnce: true,
      expectedURL:
        'http://localhost:8080/api/sync/v1/close?roomID=roomID4&userID=userID4&clientID=clientID4',
      expectedRequestInit: {
        body: '{"lastMutationID":4}',
        headers: {
          'authorization': 'Bearer auth4',
          'content-type': 'application/json',
        },
        keepalive: true,
        method: 'POST',
      },
    },
  ] as const;

  for (const c of cases) {
    test(`persisted: ${c.persisted}`, () => {
      const fetchStub = sinon
        .stub(globalThis, 'fetch')
        .returns(Promise.resolve(new Response()));

      const sink = new TestLogSink();
      const lc = new LogContext('debug', {}, sink);
      const server = 'http://localhost:8080';
      const roomID = 'roomID4';
      const userID = 'userID4';
      const clientID = 'clientID4';
      const auth = () => 'auth4';
      const lastMutationID = () => 4;
      const ac = new AbortController();
      const {signal} = ac;

      const fakeWindow = new EventTarget();

      new CloseBeaconManager(
        {roomID, userID, clientID},
        lc,
        server,
        auth,
        lastMutationID,
        signal,
        fakeWindow as Window,
      );

      const e = new PageTransitionEvent('pagehide', {persisted: c.persisted});
      fakeWindow.dispatchEvent(e);

      expect(fetchStub.calledOnce).equal(c.expectedCalledOnce);
      if (c.expectedCalledOnce) {
        expect(fetchStub.firstCall.args[0].toString()).equal(c.expectedURL);
        expect(fetchStub.firstCall.args[1]).deep.equal(c.expectedRequestInit);
      }
    });
  }
});
