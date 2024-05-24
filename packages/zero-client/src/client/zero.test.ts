import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {resetAllConfig} from 'reflect-shared/src/config.js';
import type {PullRequestV1, PushRequestV1} from 'replicache';
import {assert} from 'shared/src/asserts.js';
import {TestLogSink} from 'shared/src/logging-test-utils.js';
import * as valita from 'shared/src/valita.js';
import * as sinon from 'sinon';
import {afterEach, beforeEach, expect, suite, test, vi} from 'vitest';
import {ErrorKind, initConnectionMessageSchema} from 'zero-protocol';
import {
  Mutation,
  MutationType,
  pushMessageSchema,
} from 'zero-protocol/src/push.js';
import type {NullableVersion} from 'zero-protocol/src/version.js';
import type {AST} from '../../../zql/src/zql/ast/ast.js';
import type {EntityQuery} from '../mod.js';
import type {Update} from './crud.js';
import type {WSString} from './http-string.js';
import type {ZeroOptions} from './options.js';
import {RELOAD_REASON_STORAGE_KEY} from './reload-error-handler.js';
import {ServerError} from './server-error.js';
import {
  MockSocket,
  TestZero,
  tickAFewTimes,
  waitForUpstreamMessage,
  zeroForTest,
} from './test-utils.js'; // Why use fakes when we can use the real thing!
import {
  CONNECT_TIMEOUT_MS,
  ConnectionState,
  DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
  PING_INTERVAL_MS,
  PING_TIMEOUT_MS,
  PULL_TIMEOUT_MS,
  QueryDefs,
  RUN_LOOP_INTERVAL_MS,
  createSocket,
  serverAheadReloadReason,
} from './zero.js';

let clock: sinon.SinonFakeTimers;
const startTime = 1678829450000;

let fetchStub: sinon.SinonStub<
  Parameters<typeof fetch>,
  ReturnType<typeof fetch>
>;

beforeEach(() => {
  clock = sinon.useFakeTimers();
  clock.setSystemTime(startTime);
  sinon.replace(
    globalThis,
    'WebSocket',
    MockSocket as unknown as typeof WebSocket,
  );
  fetchStub = sinon
    .stub(globalThis, 'fetch')
    .returns(Promise.resolve(new Response()));
});

afterEach(() => {
  sinon.restore();
  resetAllConfig();
});

test('onOnlineChange callback', async () => {
  let onlineCount = 0;
  let offlineCount = 0;

  const r = zeroForTest({
    logLevel: 'debug',
    queries: {
      foo: () => ({id: 'a', val: 'a'}),
    },
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
    await clock.tickAsync(1);
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
});

test('onOnlineChange reflection on Zero class', async () => {
  const f = () => 42;
  const r = zeroForTest({
    onOnlineChange: f,
  });
  await tickAFewTimes(clock);

  expect(r.onOnlineChange).to.equal(f);
});

test('disconnects if ping fails', async () => {
  const watchdogInterval = RUN_LOOP_INTERVAL_MS;
  const pingTimeout = 5000;
  const r = zeroForTest();

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect(r.connectionState).to.equal(ConnectionState.Connecting);

  await r.triggerConnected();
  await r.waitForConnectionState(ConnectionState.Connected);
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  (await r.socket).messages.length = 0;

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

suite('createSocket', () => {
  const t = (
    socketURL: WSString,
    baseCookie: NullableVersion,
    clientID: string,
    userID: string,
    auth: string | undefined,
    jurisdiction: 'eu' | undefined,
    lmid: number,
    debugPerf: boolean,
    now: number,
    expectedURL: string,
    expectedProtocol = '',
  ) => {
    test(expectedURL, () => {
      sinon.stub(performance, 'now').returns(now);
      const mockSocket = createSocket(
        socketURL,
        baseCookie,
        clientID,
        'testClientGroupID',
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
    });
  };

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    undefined,
    0,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    '1234',
    'clientID',
    'userID',
    '',
    undefined,
    0,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    '1234',
    'clientID',
    'userID',
    '',
    undefined,
    0,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    undefined,
    123,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    undefined,
    undefined,
    123,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    undefined,
    0,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
    'auth%20with%20%5B%5D',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    'eu',
    0,
    false,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx',
    'auth%20with%20%5B%5D',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    'eu',
    0,
    true,
    0,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx&debugPerf=true',
    'auth%20with%20%5B%5D',
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    undefined,
    0,
    false,
    456,
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx',
  );
});

suite('initConnection', () => {
  test('sent when connected message received but before ConnectionState.Connected', async () => {
    const r = zeroForTest();
    const mockSocket = await r.socket;
    mockSocket.onUpstream = msg => {
      expect(
        valita.parse(JSON.parse(msg), initConnectionMessageSchema),
      ).toEqual(['initConnection', {desiredQueriesPatch: []}]);
      expect(r.connectionState).toEqual(ConnectionState.Connecting);
    };

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sends desired queries patch', async () => {
    type E = {
      id: string;
      value: number;
    };
    const r = zeroForTest({
      queries: {
        e: v => v as E,
      },
    });
    const mockSocket = await r.socket;

    mockSocket.onUpstream = msg => {
      expect(
        valita.parse(JSON.parse(msg), initConnectionMessageSchema),
      ).toEqual([
        'initConnection',
        {
          desiredQueriesPatch: [
            {
              ast: {
                aggregate: [],
                orderBy: [[['e', 'id']], 'asc'],
                select: [[['e', '*'], '*']],
                table: 'e',
              } satisfies AST,
              hash: '2dytaxdo1gtrn',
              op: 'put',
            },
          ],
        },
      ]);
      expect(r.connectionState).toEqual(ConnectionState.Connecting);
    };

    expect(mockSocket.messages.length).toEqual(0);
    r.query.e
      .select('*')
      .prepare()
      .subscribe(() => {});
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });
});

test('pusher sends one mutation per push message', async () => {
  const t = async (
    pushes: {
      mutations: Mutation[];
      expectedPushMessages: number;
      clientGroupID?: string;
      requestID?: string;
    }[],
  ) => {
    const r = zeroForTest();
    await r.triggerConnected();

    const mockSocket = await r.socket;

    for (const push of pushes) {
      const {
        mutations,
        expectedPushMessages,
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

      expect(mockSocket.messages).to.have.lengthOf(expectedPushMessages);
      for (let i = 1; i < mockSocket.messages.length; i++) {
        const raw = mockSocket.messages[i];
        const msg = valita.parse(JSON.parse(raw), pushMessageSchema);
        expect(msg[1].clientGroupID).to.equal(
          clientGroupID ?? (await r.clientGroupID),
        );
        expect(msg[1].mutations).to.have.lengthOf(1);
        expect(msg[1].requestID).to.equal(requestID);
      }
    }
  };

  await t([{mutations: [], expectedPushMessages: 0}]);
  await t([
    {
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 1,
          name: 'mut1',
          args: [{d: 1}],
          timestamp: 1,
        },
      ],
      expectedPushMessages: 1,
    },
  ]);
  await t([
    {
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 1,
          name: 'mut1',
          args: [{d: 1}],
          timestamp: 1,
        },
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 1,
          name: 'mut1',
          args: [{d: 2}],
          timestamp: 2,
        },
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
      ],
      expectedPushMessages: 3,
    },
  ]);

  // if for self client group skips [clientID, id] tuples already seen
  await t([
    {
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 1,
          name: 'mut1',
          args: [{d: 1}],
          timestamp: 1,
        },
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 1,
          name: 'mut1',
          args: [{d: 2}],
          timestamp: 2,
        },
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
      ],
      expectedPushMessages: 3,
    },
    {
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 1,
          name: 'mut1',
          args: [{d: 2}],
          timestamp: 2,
        },
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
      ],
      expectedPushMessages: 1,
    },
  ]);

  // if not for self client group (i.e. mutation recovery) does not skip
  // [clientID, id] tuples already seen
  await t([
    {
      clientGroupID: 'c1',
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 1,
          name: 'mut1',
          args: [{d: 1}],
          timestamp: 1,
        },
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 1,
          name: 'mut1',
          args: [{d: 2}],
          timestamp: 2,
        },
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
      ],
      expectedPushMessages: 3,
    },
    {
      clientGroupID: 'c1',
      mutations: [
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 1,
          name: 'mut1',
          args: [{d: 2}],
          timestamp: 2,
        },
        {
          type: MutationType.Custom,
          clientID: 'c1',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
        {
          type: MutationType.Custom,
          clientID: 'c2',
          id: 2,
          name: 'mut1',
          args: [{d: 3}],
          timestamp: 3,
        },
      ],
      expectedPushMessages: 3,
    },
  ]);
});

test('pusher adjusts mutation timestamps to be unix timestamps', async () => {
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;
  clock.tick(300); // performance.now is 500, system time is startTime + 300

  const mutations = [
    {clientID: 'c1', id: 1, name: 'mut1', args: [{d: 1}], timestamp: 100},
    {clientID: 'c2', id: 1, name: 'mut1', args: [{d: 2}], timestamp: 200},
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
  const push0 = valita.parse(
    JSON.parse(mockSocket.messages[0]),
    pushMessageSchema,
  );
  expect(push0[1].mutations[0].timestamp).to.equal(startTime + 100);
  const push1 = valita.parse(
    JSON.parse(mockSocket.messages[1]),
    pushMessageSchema,
  );
  expect(push1[1].mutations[0].timestamp).to.equal(startTime + 200);
});

test('puller with mutation recovery pull, success response', async () => {
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
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
      cookie: '1',
      requestID: 'test-request-id',
    },
  ]);

  await r.triggerPullResponse({
    cookie: '2',
    requestID: 'test-request-id',
    lastMutationIDChanges: {cid1: 1},
  });

  const result = await resultPromise;

  expect(result).to.deep.equal({
    response: {
      cookie: '2',
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
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
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
      cookie: '1',
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
  const r = zeroForTest();
  const pullReq: PullRequestV1 = {
    profileID: 'test-profile-id',
    clientGroupID: await r.clientGroupID,
    cookie: '1',
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

test('smokeTest', async () => {
  const cases: {
    name: string;
    enableServer: boolean;
  }[] = [
    {
      name: 'socket enabled',
      enableServer: true,
    },
    {
      name: 'socket disabled',
      enableServer: false,
    },
  ];

  for (const c of cases) {
    // zeroForTest adds the socket by default.
    type Issue = {
      id: string;
      value: number;
    };
    const serverOptions = c.enableServer ? {} : {server: null};
    const r = zeroForTest({
      ...serverOptions,
      queries: {
        issues: v => v as Issue,
      },
    });

    const spy = vi.fn();
    const unsubscribe = r.query.issues.select('*').prepare().subscribe(spy);

    await r.mutate.issues.create({id: 'a', value: 1});
    await r.mutate.issues.create({id: 'b', value: 2});

    // once for initial data, once for each mutation
    // once we have batch mutations this can go back to 2
    expect(spy).toHaveBeenCalledTimes(3);
    expect(spy.mock.calls[0][0]).toEqual([]);
    expect(spy.mock.calls[1][0]).toEqual([{id: 'a', value: 1}]);
    expect(spy.mock.calls[2][0]).toEqual([
      {id: 'a', value: 1},
      {id: 'b', value: 2},
    ]);

    spy.mockReset();

    await r.mutate.issues.create({id: 'a', value: 1});
    await r.mutate.issues.create({id: 'b', value: 2});

    expect(spy).toHaveBeenCalledTimes(0);

    await r.mutate.issues.set({id: 'a', value: 11});

    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.lastCall[0]).toEqual([
      {id: 'a', value: 11},
      {id: 'b', value: 2},
    ]);

    spy.mockReset();
    await r.mutate.issues.delete({id: 'b'});
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.lastCall[0]).toEqual([{id: 'a', value: 11}]);

    unsubscribe();

    spy.mockReset();
    await r.mutate.issues.create({id: 'c', value: 6});
    expect(spy).toHaveBeenCalledTimes(0);
  }
});

// TODO: Reenable metrics
// test('Metrics', async () => {
//   // This is just a smoke test -- it ensures that we send metrics once at startup.
//   // Ideally we would run Zero and put it into different error conditions and see
//   // that the metrics are reported appropriately.

//   const r = zeroForTest();
//   await r.waitForConnectionState(ConnectionState.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionState(ConnectionState.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await clock.tickAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).to.be.true;
// });

// test('Metrics not reported when enableAnalytics is false', async () => {
//   const r = zeroForTest({enableAnalytics: false});
//   await r.waitForConnectionState(ConnectionState.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionState(ConnectionState.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await clock.tickAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).to.be.false;
// });

// test('Metrics not reported when server indicates local development', async () => {
//   const r = zeroForTest({server: 'http://localhost:8000'});
//   await r.waitForConnectionState(ConnectionState.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionState(ConnectionState.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await clock.tickAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).to.be.false;
// });

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

  const r = zeroForTest({auth});

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
    const socket = await r.socket;
    expectInitConnectionMessage(socket.messages[0]);
    expect(socket.messages[1]).deep.equal(JSON.stringify(['ping', {}]));
    expect(r.connectionState).equal(ConnectionState.Connected);
    await r.triggerPong();
    expect(r.connectionState).equal(ConnectionState.Connected);
    // getAuth should not be called again.
    expect(log).empty;
    // Socket is kept as long as we are connected.
    expect(await r.socket).equal(socket);
  }
});

test(ErrorKind.AuthInvalidated, async () => {
  // In steady state we can get an AuthInvalidated error if the tokens expire on the server.
  // At this point we should disconnect and reconnect with a new auth token.

  let authCounter = 1;

  const r = zeroForTest({
    auth: () => `auth-token-${authCounter++}`,
  });

  await r.triggerConnected();
  expect((await r.socket).protocol).equal('auth-token-1');

  await r.triggerError(ErrorKind.AuthInvalidated, 'auth error');
  await r.waitForConnectionState(ConnectionState.Disconnected);

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect((await r.socket).protocol).equal('auth-token-2');
});

test('Disconnect on error', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await r.triggerError(ErrorKind.ClientNotFound, 'client not found');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('No backoff on errors', async () => {
  const r = zeroForTest();
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
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  (await r.socket).messages.length = 0;

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
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  (await r.socket).messages.length = 0;

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

const connectTimeoutMessage = 'Rejecting connect resolver due to timeout';

function expectInitConnectionMessage(message: string) {
  expect(
    valita.parse(JSON.parse(message), initConnectionMessageSchema),
  ).not.toBeUndefined();
}

function expectLogMessages(r: TestZero<QueryDefs>) {
  return expect(
    r.testLogSink.messages.flatMap(([level, _context, msg]) =>
      level === 'debug' ? msg : [],
    ),
  );
}

test('Connect timeout', async () => {
  const r = zeroForTest({logLevel: 'debug'});

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
    expectLogMessages(r).contain(connectTimeoutMessage);

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

test('socketOrigin', async () => {
  const cases: {
    name: string;
    socketEnabled: boolean;
  }[] = [
    {
      name: 'socket enabled',
      socketEnabled: true,
    },
    {
      name: 'socket disabled',
      socketEnabled: false,
    },
  ];

  for (const c of cases) {
    const r = zeroForTest(c.socketEnabled ? {} : {server: null});

    await tickAFewTimes(clock);

    expect(r.connectionState, c.name).to.equal(
      c.socketEnabled
        ? ConnectionState.Connecting
        : ConnectionState.Disconnected,
    );
  }
});

test('Logs errors in connect', async () => {
  const r = zeroForTest({});
  await r.triggerError(ErrorKind.ClientNotFound, 'client-id-a');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);

  const index = r.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'error' && args.find(arg => /client-id-a/.test(String(arg))),
  );

  expect(index).to.not.equal(-1);
});

test('New connection logs', async () => {
  clock.setSystemTime(1000);
  const r = zeroForTest({logLevel: 'info'});
  await r.waitForConnectionState(ConnectionState.Connecting);
  await clock.tickAsync(500);
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await clock.tickAsync(500);
  await r.triggerPong();
  await r.triggerClose();
  await r.waitForConnectionState(ConnectionState.Disconnected);
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  const connectIndex = r.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'info' &&
      args.find(arg => /Connected/.test(String(arg))) &&
      args.find(
        arg =>
          arg instanceof Object &&
          (arg as {timeToConnectMs: number}).timeToConnectMs === 500,
      ),
  );

  const disconnectIndex = r.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
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
  fn: (r: TestZero<QueryDefs>) => Promise<unknown>,
) {
  const r = zeroForTest();

  const log: ('resolved' | 'rejected')[] = [];

  await r.triggerError(ErrorKind.ClientNotFound, 'client-id-a');
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

  await r.triggerError(ErrorKind.ClientNotFound, 'client-id-a');
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
  const r = zeroForTest();
  r.onUpdateNeeded = fake;

  await r.triggerError(ErrorKind.VersionNotSupported, 'prot mismatch');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;
  expect(fake.firstCall.args).deep.equal([
    {type: ErrorKind.VersionNotSupported},
  ]);

  fake.resetHistory();
  r.onUpdateNeeded = null;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  expect(fake.called).false;
});

test('server ahead', async () => {
  const {promise, resolve} = resolver();
  const storage: Record<string, string> = {};
  sinon.replaceGetter(window, 'localStorage', () => storage as Storage);
  const r = zeroForTest();
  r.reload = resolve;

  await r.triggerError(
    ErrorKind.InvalidConnectionRequestBaseCookie,
    'unexpected BaseCookie',
  );
  await promise;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).to.equal(
    serverAheadReloadReason(ErrorKind.InvalidConnectionRequestBaseCookie),
  );
});

test('Constructing Zero with a negative hiddenTabDisconnectDelay option throws an error', () => {
  let expected;
  try {
    zeroForTest({hiddenTabDisconnectDelay: -1});
  } catch (e) {
    expected = e;
  }
  expect(expected)
    .instanceOf(Error)
    .property(
      'message',
      'ZeroOptions.hiddenTabDisconnectDelay must not be negative.',
    );
});

suite('Disconnect on hide', () => {
  type Case = {
    name: string;
    hiddenTabDisconnectDelay?: number | undefined;
    test: (
      r: TestZero<QueryDefs>,
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

      const r = zeroForTest({
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

test(ErrorKind.InvalidConnectionRequest, async () => {
  const r = zeroForTest({});
  await r.triggerError(ErrorKind.InvalidConnectionRequest, 'test');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);
  const msg = r.testLogSink.messages.at(-1);
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
      const r = zeroForTest({
        logLevel: 'debug',
      });
      await r.triggerConnected();
      expect(r.connectionState).to.equal(ConnectionState.Connected);

      if (c.duringPing) {
        await waitForUpstreamMessage(r, 'ping', clock);
      }

      await r.triggerPokeStart({
        // @ts-expect-error - invalid field
        pokeIDXX: '1',
        baseCookie: null,
        cookie: '1',
        timestamp: 123456,
      });
      await clock.tickAsync(0);

      if (c.duringPing) {
        await r.triggerPong();
      }

      expect(r.online).eq(true);
      expect(r.connectionState).eq(ConnectionState.Connected);

      const found = r.testLogSink.messages.some(m =>
        m[2].some(
          v => v instanceof Error && v.message.includes('Invalid union value.'),
        ),
      );
      expect(found).true;
    });
  }
});

test('kvStore option', async () => {
  const spy = sinon.spy(IDBFactory.prototype, 'open');

  type E = {
    id: string;
    value: number;
  };

  const t = async (
    kvStore: ZeroOptions<Record<string, never>>['kvStore'],
    userID: string,
    expectedIDBOpenCalled: boolean,
    expectedValue: E[],
  ) => {
    const r = zeroForTest({
      server: null,
      userID,
      kvStore,
      queries: {
        e: v => v as E,
      },
    });
    expect(await r.query.e.select('*').prepare().exec()).deep.equal(
      expectedValue,
    );
    await r.mutate.e.create({id: 'a', value: 1});
    expect(
      await r.query.e.select('*').where('id', '=', 'a').prepare().exec(),
    ).deep.equal([{id: 'a', value: 1}]);
    // Wait for persist to finish
    await tickAFewTimes(clock, 2000);
    await r.close();
    expect(spy.called).equal(expectedIDBOpenCalled, 'IDB existed!');

    spy.resetHistory();
  };

  const uuid = Math.random().toString().slice(2);

  await t('idb', 'kv-store-test-user-id-1' + uuid, true, []);
  await t('idb', 'kv-store-test-user-id-1' + uuid, true, [{id: 'a', value: 1}]);
  await t('mem', 'kv-store-test-user-id-2' + uuid, false, []);
  // Defaults to idb
  await t(undefined, 'kv-store-test-user-id-3' + uuid, true, []);
});

test('Close during connect should sleep', async () => {
  const r = zeroForTest({
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
  const hasSleeping = r.testLogSink.messages.some(m =>
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

test('Zero close should stop timeout', async () => {
  const r = zeroForTest({
    logLevel: 'debug',
  });

  await r.waitForConnectionState(ConnectionState.Connecting);
  await r.close();
  await clock.tickAsync(CONNECT_TIMEOUT_MS);
  expectLogMessages(r).not.contain(connectTimeoutMessage);
});

test('Zero close should stop timeout, close delayed', async () => {
  const r = zeroForTest({
    logLevel: 'debug',
  });

  await r.waitForConnectionState(ConnectionState.Connecting);
  await clock.tickAsync(CONNECT_TIMEOUT_MS / 2);
  await r.close();
  await clock.tickAsync(CONNECT_TIMEOUT_MS / 2);
  expectLogMessages(r).not.contain(connectTimeoutMessage);
});

test('ensure we get the same query object back', () => {
  type Issue = {
    id: string;
    title: string;
  };
  type Comment = {
    id: string;
    issueID: string;
    text: string;
  };
  const z = zeroForTest({
    queries: {
      issue: v => v as Issue,
      comment: v => v as Comment,
    },
  });
  const issueQuery1 = z.query.issue;
  const issueQuery2 = z.query.issue;
  expect(issueQuery1).to.equal(issueQuery2);

  const commentQuery1 = z.query.comment;
  const commentQuery2 = z.query.comment;
  expect(commentQuery1).to.equal(commentQuery2);

  expect(issueQuery1).to.not.equal(commentQuery1);
});

test('the type of collection should be inferred from options with parse', () => {
  type Issue = {
    id: string;
    title: string;
  };
  type Comment = {
    id: string;
    issueID: string;
    text: string;
  };
  const r = zeroForTest({
    queries: {
      issue: v => v as Issue,
      comment: v => v as Comment,
    },
  });

  const c: {
    readonly issue: EntityQuery<{issue: Issue}, []>;
    readonly comment: EntityQuery<{comment: Comment}, []>;
  } = r.query;
  expect(c).not.undefined;

  const issueQ: EntityQuery<{issue: Issue}> = r.query.issue;
  const commentQ: EntityQuery<{comment: Comment}> = r.query.comment;
  expect(issueQ).not.undefined;
  expect(commentQ).not.undefined;
});

suite('CRUD', () => {
  type Issue = {
    id: string;
    title: string;
  };
  type Comment = {
    id: string;
    issueID: string;
    text: string;
  };
  const makeZero = () =>
    zeroForTest({
      queries: {
        issue: v => v as Issue,
        comment: v => v as Comment,
      },
    });

  test('create', async () => {
    const z = makeZero();

    const createIssue: (issue: Issue) => Promise<void> = z.mutate.issue.create;
    await createIssue({id: 'a', title: 'A'});
    expect(await z.query.issue.select('*').prepare().exec()).toEqual([
      {id: 'a', title: 'A'},
    ]);

    // create again should not change anything
    await createIssue({id: 'a', title: 'Again'});
    expect(await z.query.issue.select('*').prepare().exec()).toEqual([
      {id: 'a', title: 'A'},
    ]);
  });

  test('set', async () => {
    const z = makeZero();

    await z.mutate.comment.create({id: 'a', issueID: '1', text: 'A text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '1', text: 'A text'},
    ]);

    const setComment: (comment: Comment) => Promise<void> =
      z.mutate.comment.set;
    await setComment({id: 'b', issueID: '2', text: 'B text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '1', text: 'A text'},
      {id: 'b', issueID: '2', text: 'B text'},
    ]);

    // set allows updating
    await setComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '11', text: 'AA text'},
      {id: 'b', issueID: '2', text: 'B text'},
    ]);
  });

  test('update', async () => {
    const z = makeZero();

    await z.mutate.comment.create({id: 'a', issueID: '1', text: 'A text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '1', text: 'A text'},
    ]);

    const updateComment: (comment: Update<Comment>) => Promise<void> =
      z.mutate.comment.update;
    await updateComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '11', text: 'AA text'},
    ]);

    await updateComment({id: 'a', text: 'AAA text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '11', text: 'AAA text'},
    ]);

    // update is a noop if not existing
    await updateComment({id: 'b', issueID: '2', text: 'B text'});
    expect(await z.query.comment.select('*').prepare().exec()).toEqual([
      {id: 'a', issueID: '11', text: 'AAA text'},
    ]);
  });

  test('do not expose _zero_crud', () => {
    const z = zeroForTest({
      queries: {
        issue: v => v as {id: string; title: string},
      },
    });

    expect(
      (z.mutate as unknown as Record<string, unknown>)._zero_crud,
    ).toBeUndefined();
  });
});

test('mutate is a function for batching', async () => {
  type Issue = {
    id: string;
    title: string;
  };
  type Comment = {
    id: string;
    issueID: string;
    text: string;
  };
  const z = zeroForTest({
    queries: {
      issue: v => v as Issue,
      comment: v => v as Comment,
    },
  });

  const x = await z.mutate(async m => {
    expect(
      (m as unknown as Record<string, unknown>)._zero_crud,
    ).toBeUndefined();
    await m.issue.create({id: 'a', title: 'A'});
    await m.comment.create({
      id: 'b',
      issueID: 'a',
      text: 'Comment for issue A',
    });
    await m.comment.update({
      id: 'b',
      text: 'Comment for issue A was changed',
    });
    return 123 as const;
  });

  expect(x).toBe(123);

  expect(await z.query.issue.select('*').prepare().exec()).toEqual([
    {id: 'a', title: 'A'},
  ]);
  expect(await z.query.comment.select('*').prepare().exec()).toEqual([
    {id: 'b', issueID: 'a', text: 'Comment for issue A was changed'},
  ]);

  expect(
    (z.mutate as unknown as Record<string, unknown>)._zero_crud,
  ).toBeUndefined();
});

test('calling mutate on the non batch version should throw inside a batch', async () => {
  type Issue = {
    id: string;
    title: string;
  };
  type Comment = {
    id: string;
    issueID: string;
    text: string;
  };
  const z = zeroForTest({
    queries: {
      issue: v => v as Issue,
      comment: v => v as Comment,
    },
  });

  await expect(
    z.mutate(async m => {
      await m.issue.create({id: 'a', title: 'A'});
      await z.mutate.issue.create({id: 'b', title: 'B'});
    }),
  ).rejects.toThrow('Cannot call mutate.issue.create inside a batch');

  // make sure that we did not update the issue collection.
  await expect(z.query.issue.select('*').prepare().exec()).resolves.toEqual([]);

  await z.mutate.comment.create({id: 'a', text: 'A', issueID: 'a'});
  await expect(z.query.comment.select('*').prepare().exec()).resolves.toEqual([
    {id: 'a', text: 'A', issueID: 'a'},
  ]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.update({id: 'a', text: 'A2'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.update inside a batch');
  // make sure that we did not update the comment collection.
  await expect(z.query.comment.select('*').prepare().exec()).resolves.toEqual([
    {id: 'a', text: 'A', issueID: 'a'},
  ]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.set({id: 'a', text: 'A2', issueID: 'a'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.set inside a batch');
  // make sure that we did not update the comment collection.
  await expect(z.query.comment.select('*').prepare().exec()).resolves.toEqual([
    {id: 'a', text: 'A', issueID: 'a'},
  ]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.delete({id: 'a'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.delete inside a batch');
  // make sure that we did not delete the comment row
  await expect(z.query.comment.select('*').prepare().exec()).resolves.toEqual([
    {id: 'a', text: 'A', issueID: 'a'},
  ]);

  await expect(
    z.mutate(async () => {
      await z.mutate(() => {});
    }),
  ).rejects.toThrow('Cannot call mutate inside a batch');
});
