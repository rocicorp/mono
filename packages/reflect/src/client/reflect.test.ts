import {expect} from '@esm-bundle/chai';
import type {
  JSONValue,
  PullRequestV1,
  PushRequestV1,
  WriteTransaction,
} from 'replicache';
import * as sinon from 'sinon';
import {Mutation, pushMessageSchema} from '../protocol/push.js';
import type {NullableVersion} from '../types/version.js';
import {ConnectionState, createSocket} from './reflect.js';
import {MockSocket, reflectForTest, tickAFewTimes} from './test-utils.js';
// fetch-mock has invalid d.ts file so we removed that on npm install.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
import fetchMock from 'fetch-mock/esm/client';

let clock: sinon.SinonFakeTimers;

setup(() => {
  clock = sinon.useFakeTimers();
});

teardown(() => {
  sinon.restore();
  fetchMock.restore();
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

  await r.waitForSocket(clock);

  expect(r.connectionState).to.equal(ConnectionState.Connecting);
  expect(onlineCount).to.equal(0);
  expect(offlineCount).to.equal(0);

  await tickAFewTimes(clock);
  r.triggerConnected();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  expect(onlineCount).to.equal(1);
  expect(offlineCount).to.equal(0);

  await tickAFewTimes(clock);
  r.triggerClose();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  expect(onlineCount).to.equal(1);
  expect(offlineCount).to.equal(1);

  // let the watchdog timer fire
  await tickAFewTimes(clock, 5000);
  r.triggerConnected();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  expect(onlineCount).to.equal(2);
  expect(offlineCount).to.equal(1);

  r.onOnlineChange = null;
  await tickAFewTimes(clock);
  r.triggerClose();
  await tickAFewTimes(clock);
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
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  r.triggerPong();
  await tickAFewTimes(clock);
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  await tickAFewTimes(clock, watchdogInterval);
  r.triggerPong();
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
    expectedProtocol?: string,
  ) => {
    const mockSocket = createSocket(
      socketURL,
      baseCookie,
      clientID,
      'testClientGroupID',
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
    'ws://example.com/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&baseCookie=&ts=0&lmid=0',
  );

  t(
    'ws://example.com/',
    1234,
    'clientID',
    'roomID',
    '',
    0,
    'ws://example.com/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&baseCookie=1234&ts=0&lmid=0',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    '',
    123,
    'ws://example.com/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&baseCookie=&ts=0&lmid=123',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'roomID',
    'auth with []',
    0,
    'ws://example.com/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&baseCookie=&ts=0&lmid=0',
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
    'ws://example.com/connect?clientID=clientID&clientGroupID=testClientGroupID&roomID=roomID&baseCookie=&ts=456&lmid=0',
  );
});

test('pusher sends one mutation per push message', async () => {
  const t = async (
    pushes: {
      mutations: Mutation[];
      expectedMessages: number;
      clientGroupID?: string;
    }[],
  ) => {
    const r = reflectForTest();
    await tickAFewTimes(clock);
    r.triggerConnected();

    const mockSocket = r.socket as unknown as MockSocket;

    for (const push of pushes) {
      const {mutations, expectedMessages, clientGroupID} = push;

      const pushReq: PushRequestV1 = {
        profileID: 'p1',
        clientGroupID: clientGroupID ?? (await r.clientGroupID),
        pushVersion: 1,
        schemaVersion: '1',
        mutations,
      };

      mockSocket.messages.length = 0;

      await r.pusher(pushReq);

      expect(mockSocket.messages).to.have.lengthOf(expectedMessages);

      for (const raw of mockSocket.messages) {
        const msg = pushMessageSchema.parse(JSON.parse(raw));
        expect(msg[1].clientGroupID).to.equal(
          clientGroupID ?? (await r.clientGroupID),
        );
        expect(msg[1].mutations).to.have.lengthOf(1);
      }
    }

    await r.close();
    await tickAFewTimes(clock);
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

test('puller with mutation recovery pull, success response', async () => {
  const r = reflectForTest();
  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: 1,
    pullVersion: 1,
    schemaVersion: '1',
  };

  const pullResponseBody = {
    cookie: 2,
    lastMutationIDChanges: {cid1: 1},
    patch: [],
  };
  fetchMock.post(
    'https://example.com/pull?roomID=test-room-id',
    async (_url: string, _options: RequestInit, request: Request) => {
      expect(await request.json()).to.deep.equal(pullReq);
      return pullResponseBody;
    },
  );
  const result = await r.puller(pullReq);
  expect(result).to.deep.equal({
    response: pullResponseBody,
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
  });
});

test('puller with mutation recovery pull, error response', async () => {
  const r = reflectForTest();
  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: 1,
    pullVersion: 1,
    schemaVersion: '1',
  };

  const errorMessage = 'Pull error';
  const errorStatusCode = 500;
  fetchMock.post(
    'https://example.com/pull?roomID=test-room-id',
    async (_url: string, _options: RequestInit, request: Request) => {
      expect(await request.json()).to.deep.equal(pullReq);
      return new Response(errorMessage, {status: errorStatusCode});
    },
  );
  const result = await r.puller(pullReq);
  expect(result).to.deep.equal({
    httpRequestInfo: {
      errorMessage,
      httpStatusCode: errorStatusCode,
    },
  });
});

test('puller with normal, non-mutation recovery, pull', async () => {
  const r = reflectForTest();
  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: await r.clientGroupID,
    cookie: 1,
    pullVersion: 1,
    schemaVersion: '1',
  };

  const result = await r.puller(pullReq);
  expect(fetchMock.called()).to.be.false;
  expect(result).to.deep.equal({
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
  });
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
