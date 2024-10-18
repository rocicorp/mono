import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import * as sinon from 'sinon';
import {afterEach, beforeEach, expect, suite, test} from 'vitest';
import type {
  PullRequestV1,
  PushRequestV1,
} from '../../../replicache/src/mod.js';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.js';
import {assert} from '../../../shared/src/asserts.js';
import {TestLogSink} from '../../../shared/src/logging-test-utils.js';
import * as valita from '../../../shared/src/valita.js';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {
  changeDesiredQueriesMessageSchema,
  decodeSecProtocols,
  encodeSecProtocols,
  ErrorKind,
  initConnectionMessageSchema,
} from '../../../zero-protocol/src/mod.js';
import {
  type Mutation,
  MutationType,
  pushMessageSchema,
} from '../../../zero-protocol/src/push.js';
import type {NullableVersion} from '../../../zero-protocol/src/version.js';
import type {TypedView} from '../mod.js';
import type {WSString} from './http-string.js';
import type {ZeroOptions} from './options.js';
import type {QueryManager} from './query-manager.js';
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
  createSocket,
  DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
  PING_INTERVAL_MS,
  PING_TIMEOUT_MS,
  PULL_TIMEOUT_MS,
  RUN_LOOP_INTERVAL_MS,
  type Schema,
  type UpdateNeededReason,
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
});

test('onOnlineChange callback', async () => {
  let onlineCount = 0;
  let offlineCount = 0;

  const r = zeroForTest({
    logLevel: 'debug',
    schema: {
      version: 1,
      tables: {
        foo: {
          tableName: 'foo',
          columns: {
            id: {type: 'string'},
            val: {type: 'string'},
          },
          primaryKey: ['id'],
          relationships: {},
        },
      },
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

const mockRep = {
  query() {
    return Promise.resolve([]);
  },
} as unknown as ReplicacheImpl;
const mockQueryManager = {
  getQueriesPatch() {
    return Promise.resolve([]);
  },
} as unknown as QueryManager;

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
  ) => {
    const schemaVersion = 3;
    test(expectedURL, async () => {
      sinon.stub(performance, 'now').returns(now);
      const mockSocket = (
        await createSocket(
          mockRep,
          mockQueryManager,
          socketURL,
          baseCookie,
          clientID,
          'testClientGroupID',
          schemaVersion,
          userID,
          auth,
          jurisdiction,
          lmid,
          'wsidx',
          debugPerf,
          new LogContext('error', undefined, new TestLogSink()),
        )
      )[0] as unknown as MockSocket;
      expect(`${mockSocket.url}`).equal(expectedURL);
      expect(mockSocket.protocol).equal(
        encodeSecProtocols(['initConnection', {desiredQueriesPatch: []}], auth),
      );
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&jurisdiction=eu&baseCookie=&ts=0&lmid=0&wsid=wsidx&debugPerf=true',
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
    'ws://example.com/api/sync/v1/connect?clientID=clientID&clientGroupID=testClientGroupID&schemaVersion=3&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx',
  );
});

suite('initConnection', () => {
  test('not sent when connected message received but before ConnectionState.Connected', async () => {
    const r = zeroForTest();
    const mockSocket = await r.socket;

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    // upon receiving `connected` we do not sent `initConnection` since it is sent
    // when opening the connection.
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('sends desired queries patch in sec-protocol header', async () => {
    const r = zeroForTest({
      schema: {
        version: 1,
        tables: {
          e: {
            tableName: 'e',
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            relationships: {},
          },
        },
      },
    });

    const view = r.query.e.select('id', 'value').materialize();
    view.addListener(() => {});

    const mockSocket = await r.socket;

    expect(
      valita.parse(
        JSON.parse(decodeSecProtocols(mockSocket.protocol)[0]),
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        desiredQueriesPatch: [
          {
            ast: {
              table: 'e',
              orderBy: [['id', 'asc']],
            } satisfies AST,
            hash: '1jnb9n35hhddz',
            op: 'put',
          },
        ],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('sends changeDesiredQueries if new queries are added after initConnection but before connected', async () => {
    const r = zeroForTest({
      schema: {
        version: 1,
        tables: {
          e: {
            tableName: 'e',
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            relationships: {},
          },
        },
      },
    });

    const mockSocket = await r.socket;
    mockSocket.onUpstream = msg => {
      expect(
        valita.parse(JSON.parse(msg), changeDesiredQueriesMessageSchema),
      ).toEqual([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              ast: {
                table: 'e',
                orderBy: [['id', 'asc']],
              } satisfies AST,
              hash: '1jnb9n35hhddz',
              op: 'put',
            },
          ],
        },
      ]);
      expect(r.connectionState).toEqual(ConnectionState.Connecting);
    };

    expect(
      valita.parse(
        JSON.parse(decodeSecProtocols(mockSocket.protocol)[0]),
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        desiredQueriesPatch: [],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);

    const view = r.query.e.select('id', 'value').materialize();
    view.addListener(() => {});

    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('changeDesiredQueries does not include queries sent with initConnection', async () => {
    const r = zeroForTest({
      schema: {
        version: 1,
        tables: {
          e: {
            tableName: 'e',
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            relationships: {},
          },
        },
      },
    });

    const view1 = r.query.e.select('id', 'value').materialize();
    view1.addListener(() => {});

    const mockSocket = await r.socket;
    expect(mockSocket.messages.length).toEqual(0);

    const view2 = r.query.e.select('id', 'value').materialize();
    view2.addListener(() => {});
    await r.triggerConnected();
    // no `changeDesiredQueries` sent since the query was already included in `initConnection`
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('changeDesiredQueries does include removal of a query sent with initConnection if it was removed before `connected`', async () => {
    const r = zeroForTest({
      schema: {
        version: 1,
        tables: {
          e: {
            tableName: 'e',
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            relationships: {},
          },
        },
      },
    });

    const view1 = r.query.e.select('id', 'value').materialize();
    const removeListener = view1.addListener(() => {});

    const mockSocket = await r.socket;
    mockSocket.onUpstream = msg => {
      expect(
        valita.parse(JSON.parse(msg), changeDesiredQueriesMessageSchema),
      ).toEqual([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              hash: '1jnb9n35hhddz',
              op: 'del',
            },
          ],
        },
      ]);
    };
    expect(mockSocket.messages.length).toEqual(0);

    removeListener();
    view1.destroy();
    // no `changeDesiredQueries` sent yet since we're not connected
    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    // changedDesiredQueries has been sent.
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
        expect(msg[1].schemaVersion).to.equal(1);
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
    const serverOptions = c.enableServer ? {} : {server: null};
    const r = zeroForTest({
      ...serverOptions,
      schema: {
        version: 1,
        tables: {
          issues: {
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            tableName: 'issues',
            relationships: {},
          },
        },
      },
    });

    const calls: Array<Array<unknown>> = [];
    const view = r.query.issues.select('id', 'value').materialize();
    const unsubscribe = view.addListener(c => {
      calls.push([...c]);
    });
    view.hydrate();

    await r.mutate.issues.create({id: 'a', value: 1});
    await r.mutate.issues.create({id: 'b', value: 2});

    // we get called for initial hydration, even though there's no data.
    // plus once for the each transaction
    // we test multiple changes in a transactions below
    expect(calls.length).eq(3);
    expect(calls[0]).toEqual([]);
    expect(calls[1]).toEqual([{id: 'a', value: 1}]);
    expect(calls[2]).toEqual([
      {id: 'a', value: 1},
      {id: 'b', value: 2},
    ]);

    calls.length = 0;

    await r.mutate.issues.create({id: 'a', value: 1});
    await r.mutate.issues.create({id: 'b', value: 2});

    expect(calls.length).eq(0);

    await r.mutate.issues.set({id: 'a', value: 11});

    // Although the set() results in a remove and add flowing through the pipeline,
    // they are in same tx, so we only get one call coming out.
    expect(calls.length).eq(1);
    expect(calls[0]).toEqual([
      {id: 'a', value: 11},
      {id: 'b', value: 2},
    ]);

    calls.length = 0;
    await r.mutate.issues.delete({id: 'b'});
    expect(calls.length).eq(1);
    expect(calls[0]).toEqual([{id: 'a', value: 11}]);

    unsubscribe();

    calls.length = 0;
    await r.mutate.issues.create({id: 'c', value: 6});
    expect(calls.length).eq(0);
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
    expect(decodeSecProtocols((await r.socket).protocol)[1]).equal(
      expectedAuthToken,
    );
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
    expect(decodeSecProtocols(socket.protocol)[1]).equal('new-auth-token-8');
    await r.triggerConnected();
    await r.waitForConnectionState(ConnectionState.Connected);
    // getAuth should not be called again.
    expect(log).empty;
  }

  {
    // Ping/pong should happen every 5 seconds.
    await tickAFewTimes(clock, PING_INTERVAL_MS);
    const socket = await r.socket;
    expect(socket.messages[0]).deep.equal(JSON.stringify(['ping', {}]));
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
  expect(decodeSecProtocols((await r.socket).protocol)[1]).equal(
    'auth-token-1',
  );

  await r.triggerError(ErrorKind.AuthInvalidated, 'auth error');
  await r.waitForConnectionState(ConnectionState.Disconnected);

  await r.waitForConnectionState(ConnectionState.Connecting);
  expect(decodeSecProtocols((await r.socket).protocol)[1]).equal(
    'auth-token-2',
  );
});

test('Disconnect on error', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);
  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('No backoff on errors', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionState).to.equal(ConnectionState.Connected);

  const step = async (delta: number, message: string) => {
    await r.triggerError(ErrorKind.InvalidMessage, message);
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

function expectLogMessages(r: TestZero<Schema>) {
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
  await r.triggerError(ErrorKind.InvalidMessage, 'bad-message');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
  await clock.tickAsync(0);

  const index = r.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'error' && args.find(arg => /bad-message/.test(String(arg))),
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
  fn: (r: TestZero<Schema>) => Promise<unknown>,
) {
  const r = zeroForTest();

  const log: ('resolved' | 'rejected')[] = [];

  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
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

  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
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

test('VersionNotSupported default handler', async () => {
  const storage: Record<string, string> = {};
  sinon.replaceGetter(window, 'localStorage', () => storage as Storage);
  const {promise, resolve} = resolver();
  const fake = sinon.fake(resolve);
  const r = zeroForTest(undefined, false);
  r.reload = fake;

  await r.triggerError(ErrorKind.VersionNotSupported, 'server test message');
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).to.equal(
    "The server no longer supports this client's protocol version. server test message",
  );
});

test('VersionNotSupported custom onUpdateNeeded handler', async () => {
  const {promise, resolve} = resolver();
  const fake = sinon.fake((_reason: UpdateNeededReason) => {
    resolve();
  });
  const r = zeroForTest();
  r.onUpdateNeeded = fake;

  await r.triggerError(ErrorKind.VersionNotSupported, 'server test message');
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;
});

test('VersionNotSupported null onUpdateNeeded handler', async () => {
  const r = zeroForTest();
  r.onUpdateNeeded = null;

  await r.triggerError(ErrorKind.VersionNotSupported, 'server test message');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('SchemaVersionNotSupported default handler', async () => {
  const storage: Record<string, string> = {};
  sinon.replaceGetter(window, 'localStorage', () => storage as Storage);
  const {promise, resolve} = resolver();
  const fake = sinon.fake(resolve);
  const r = zeroForTest(undefined, false);
  r.reload = fake;

  await r.triggerError(
    ErrorKind.SchemaVersionNotSupported,
    'server test message',
  );
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).to.equal(
    "The server no longer supports this client's schema version. server test message",
  );
});

test('SchemaVersionNotSupported custom onUpdateNeeded handler', async () => {
  const {promise, resolve} = resolver();
  const fake = sinon.fake((_reason: UpdateNeededReason) => {
    resolve();
  });
  const r = zeroForTest();
  r.onUpdateNeeded = fake;

  await r.triggerError(
    ErrorKind.SchemaVersionNotSupported,
    'server test message',
  );
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;
});

test('SchemaVersionNotSupported null onUpdateNeeded handler', async () => {
  const r = zeroForTest();
  r.onUpdateNeeded = null;

  await r.triggerError(
    ErrorKind.SchemaVersionNotSupported,
    'server test message',
  );
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
});

test('ClientNotFound default handler', async () => {
  const storage: Record<string, string> = {};
  sinon.replaceGetter(window, 'localStorage', () => storage as Storage);
  const {promise, resolve} = resolver();
  const fake = sinon.fake(() => {
    resolve();
  });
  const r = zeroForTest();
  r.reload = fake;

  await r.triggerError(ErrorKind.ClientNotFound, 'server test message');
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).to.equal(
    'Server could not find state needed to synchronize this client. server test message',
  );
});

test('ClientNotFound custom onClientStateNotFound handler', async () => {
  const {promise, resolve} = resolver();
  const fake = sinon.fake(() => {
    resolve();
  });
  const r = zeroForTest();
  r.onClientStateNotFound = fake;

  await r.triggerError(ErrorKind.ClientNotFound, 'server test message');
  await promise;
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);

  expect(fake.calledOnce).true;
});

test('ClientNotFound null handler', async () => {
  const r = zeroForTest();
  r.onClientStateNotFound = null;

  await r.triggerError(ErrorKind.ClientNotFound, 'server test message');
  expect(r.connectionState).to.equal(ConnectionState.Disconnected);
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
    'Server reported that client is ahead of server (InvalidConnectionRequestBaseCookie). This probably happened because the server is in development mode and restarted. Currently when this happens, the dev server loses its state and on reconnect sees the client as ahead. If you see this in other cases, it may be a bug in Zero.',
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
      r: TestZero<Schema>,
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

  const t = async <S extends Schema>(
    kvStore: ZeroOptions<S>['kvStore'],
    userID: string,
    expectedIDBOpenCalled: boolean,
    expectedValue: E[],
  ) => {
    const r = zeroForTest({
      server: null,
      userID,
      kvStore,
      schema: {
        version: 1,
        tables: {
          e: {
            columns: {
              id: {type: 'string'},
              value: {type: 'number'},
            },
            primaryKey: ['id'],
            tableName: 'e',
            relationships: {},
          },
        },
      },
    });
    const idIsAView = r.query.e
      .select('id', 'value')
      .where('id', '=', 'a')
      .materialize();
    idIsAView.hydrate();
    const allDataView = r.query.e.select('id', 'value').materialize();
    allDataView.hydrate();

    const waitForNextUpdate = (view: TypedView<unknown>) => {
      const {promise, resolve} = resolver();
      const unsubscribe = view.addListener(() => {
        resolve();
      });
      void promise.then(() => unsubscribe());
      return promise;
    };
    // TODO: we need a way to await hydration...
    await tickAFewTimes(clock);
    await waitForNextUpdate(allDataView);

    expect(allDataView.data).deep.equal(expectedValue);
    await r.mutate.e.create({id: 'a', value: 1});
    await tickAFewTimes(clock);
    await waitForNextUpdate(idIsAView);

    expect(idIsAView.data).deep.equal([{id: 'a', value: 1}]);
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
  const z = zeroForTest({
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            title: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'issue',
          relationships: {},
        },
        comment: {
          columns: {
            id: {type: 'string'},
            issueID: {type: 'string'},
            text: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'comment',
          relationships: {},
        },
      },
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
  const r = zeroForTest({
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            title: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'issue',
          relationships: {},
        },
        comment: {
          columns: {
            id: {type: 'string'},
            issueID: {type: 'string'},
            text: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'comment',
          relationships: {},
        },
      },
    },
  });

  const c = r.query;
  expect(c).not.undefined;

  const issueQ = r.query.issue;
  const commentQ = r.query.comment;
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
      schema: {
        version: 1,
        tables: {
          issue: {
            columns: {
              id: {type: 'string'},
              title: {type: 'string'},
            },
            primaryKey: ['id'],
            tableName: 'issue',
            relationships: {},
          },
          comment: {
            columns: {
              id: {type: 'string'},
              issueID: {type: 'string'},
              text: {type: 'string'},
            },
            primaryKey: ['id'],
            tableName: 'comment',
            relationships: {},
          },
        },
      },
    });

  test('create', async () => {
    const z = makeZero();

    const createIssue: (issue: Issue) => Promise<void> = z.mutate.issue.create;
    const view = z.query.issue.select('id', 'title').materialize();
    view.hydrate();
    await createIssue({id: 'a', title: 'A'});
    expect(view.data).toEqual([{id: 'a', title: 'A'}]);

    // create again should not change anything
    await createIssue({id: 'a', title: 'Again'});
    expect(view.data).toEqual([{id: 'a', title: 'A'}]);
  });

  test('set', async () => {
    const z = makeZero();

    const view = z.query.comment.select('id', 'issueID', 'text').materialize();
    await z.mutate.comment.create({id: 'a', issueID: '1', text: 'A text'});
    expect(view.data).toEqual([{id: 'a', issueID: '1', text: 'A text'}]);

    const setComment: (comment: Comment) => Promise<void> =
      z.mutate.comment.set;
    await setComment({id: 'b', issueID: '2', text: 'B text'});
    expect(view.data).toEqual([
      {id: 'a', issueID: '1', text: 'A text'},
      {id: 'b', issueID: '2', text: 'B text'},
    ]);

    // set allows updating
    await setComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(view.data).toEqual([
      {id: 'a', issueID: '11', text: 'AA text'},
      {id: 'b', issueID: '2', text: 'B text'},
    ]);
  });

  test('update', async () => {
    const z = makeZero();
    const view = z.query.comment.select('id', 'issueID').materialize();
    await z.mutate.comment.create({id: 'a', issueID: '1', text: 'A text'});
    expect(view.data).toEqual([{id: 'a', issueID: '1', text: 'A text'}]);

    const updateComment = z.mutate.comment.update;
    await updateComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(view.data).toEqual([{id: 'a', issueID: '11', text: 'AA text'}]);

    await updateComment({id: 'a', text: 'AAA text'});
    expect(view.data).toEqual([{id: 'a', issueID: '11', text: 'AAA text'}]);

    // update is a noop if not existing
    await updateComment({id: 'b', issueID: '2', text: 'B text'});
    expect(view.data).toEqual([{id: 'a', issueID: '11', text: 'AAA text'}]);
  });

  test('do not expose _zero_crud', () => {
    const z = zeroForTest({
      schema: {
        version: 1,
        tables: {
          issue: {
            columns: {
              id: {type: 'string'},
              title: {type: 'string'},
            },
            primaryKey: ['id'],
            tableName: 'issue',
            relationships: {},
          },
        },
      },
    });

    expect(
      (z.mutate as unknown as Record<string, unknown>)._zero_crud,
    ).toBeUndefined();
  });
});

suite('CRUD with compound primary key', () => {
  type Issue = {
    ids: string;
    idn: number;
    title: string;
  };
  type Comment = {
    ids: string;
    idn: number;
    issueIDs: string;
    issueIDn: number;
    text: string;
  };
  const makeZero = () =>
    zeroForTest({
      schema: {
        version: 1,
        tables: {
          issue: {
            columns: {
              ids: {type: 'string'},
              idn: {type: 'number'},
              title: {type: 'string'},
            },
            primaryKey: ['idn', 'ids'],
            tableName: 'issue',
            relationships: {},
          },
          comment: {
            columns: {
              ids: {type: 'string'},
              idn: {type: 'number'},
              issueIDs: {type: 'string'},
              issueIDn: {type: 'number'},
              text: {type: 'string'},
            },
            primaryKey: ['idn', 'ids'],
            tableName: 'comment',
            relationships: {},
          },
        },
      },
    });

  test('create', async () => {
    const z = makeZero();

    const createIssue: (issue: Issue) => Promise<void> = z.mutate.issue.create;
    const view = z.query.issue.select('ids', 'idn', 'title').materialize();
    view.hydrate();
    await createIssue({ids: 'a', idn: 1, title: 'A'});
    expect(view.data).toEqual([{ids: 'a', idn: 1, title: 'A'}]);

    // create again should not change anything
    await createIssue({ids: 'a', idn: 1, title: 'Again'});
    expect(view.data).toEqual([{ids: 'a', idn: 1, title: 'A'}]);
  });

  test('set', async () => {
    const z = makeZero();

    const view = z.query.comment
      .select('ids', 'idn', 'issueIDs', 'issueIDn', 'text')
      .materialize();
    await z.mutate.comment.create({
      ids: 'a',
      idn: 1,
      issueIDs: 'a',
      issueIDn: 1,
      text: 'A text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'a', issueIDn: 1, text: 'A text'},
    ]);

    const setComment: (comment: Comment) => Promise<void> =
      z.mutate.comment.set;
    await setComment({
      ids: 'b',
      idn: 2,
      issueIDs: 'b',
      issueIDn: 2,
      text: 'B text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'a', issueIDn: 1, text: 'A text'},
      {ids: 'b', idn: 2, issueIDs: 'b', issueIDn: 2, text: 'B text'},
    ]);

    // set allows updating
    await setComment({
      ids: 'a',
      idn: 1,
      issueIDs: 'aa',
      issueIDn: 11,
      text: 'AA text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'aa', issueIDn: 11, text: 'AA text'},
      {ids: 'b', idn: 2, issueIDs: 'b', issueIDn: 2, text: 'B text'},
    ]);
  });

  test('update', async () => {
    const z = makeZero();
    const view = z.query.comment
      .select('ids', 'idn', 'issueIDs', 'issueIDn')
      .materialize();
    await z.mutate.comment.create({
      ids: 'a',
      idn: 1,
      issueIDs: 'a',
      issueIDn: 1,
      text: 'A text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'a', issueIDn: 1, text: 'A text'},
    ]);

    const updateComment = z.mutate.comment.update;
    await updateComment({
      ids: 'a',
      idn: 1,
      issueIDs: 'aa',
      issueIDn: 11,
      text: 'AA text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'aa', issueIDn: 11, text: 'AA text'},
    ]);

    await updateComment({ids: 'a', idn: 1, text: 'AAA text'});
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'aa', issueIDn: 11, text: 'AAA text'},
    ]);

    // update is a noop if not existing
    await updateComment({
      ids: 'b',
      idn: 2,
      issueIDs: 'b',
      issueIDn: 2,
      text: 'B text',
    });
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, issueIDs: 'aa', issueIDn: 11, text: 'AAA text'},
    ]);
  });
});

test('Zero with invalid primary keys', () => {
  expect(() =>
    zeroForTest({
      schema: {
        version: 1,
        tables: {
          issue: {
            columns: {
              name: {type: 'string'},
              title: {type: 'string'},
            },
            primaryKey: ['id'],
            tableName: 'issue',
            relationships: {},
          },
        },
      },
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Primary key column "id" not found]`,
  );
});

test('mutate is a function for batching', async () => {
  const z = zeroForTest({
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            title: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'issue',
          relationships: {},
        },
        comment: {
          columns: {
            id: {type: 'string'},
            issueID: {type: 'string'},
            text: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'comment',
          relationships: {},
        },
      },
    },
  });
  const issueView = z.query.issue.select('id', 'title').materialize();
  issueView.hydrate();
  const commentView = z.query.comment
    .select('id', 'issueID', 'text')
    .materialize();
  commentView.hydrate();

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

  expect(issueView.data).toEqual([{id: 'a', title: 'A'}]);
  expect(commentView.data).toEqual([
    {id: 'b', issueID: 'a', text: 'Comment for issue A was changed'},
  ]);

  expect(
    (z.mutate as unknown as Record<string, unknown>)._zero_crud,
  ).toBeUndefined();
});

test('calling mutate on the non batch version should throw inside a batch', async () => {
  const z = zeroForTest({
    schema: {
      version: 1,
      tables: {
        issue: {
          columns: {
            id: {type: 'string'},
            title: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'issue',
          relationships: {},
        },
        comment: {
          columns: {
            id: {type: 'string'},
            issueID: {type: 'string'},
            text: {type: 'string'},
          },
          primaryKey: ['id'],
          tableName: 'comment',
          relationships: {},
        },
      },
    },
  });
  const commentView = z.query.comment
    .select('id', 'issueID', 'text')
    .materialize();
  commentView.hydrate();
  const issueView = z.query.issue.select('id', 'title').materialize();
  issueView.hydrate();

  await expect(
    z.mutate(async m => {
      await m.issue.create({id: 'a', title: 'A'});
      await z.mutate.issue.create({id: 'b', title: 'B'});
    }),
  ).rejects.toThrow('Cannot call mutate.issue.create inside a batch');

  // make sure that we did not update the issue collection.
  expect(issueView.data).toEqual([]);

  await z.mutate.comment.create({id: 'a', text: 'A', issueID: 'a'});
  expect(commentView.data).toEqual([{id: 'a', text: 'A', issueID: 'a'}]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.update({id: 'a', text: 'A2'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.update inside a batch');
  // make sure that we did not update the comment collection.
  expect(commentView.data).toEqual([{id: 'a', text: 'A', issueID: 'a'}]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.set({id: 'a', text: 'A2', issueID: 'a'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.set inside a batch');
  // make sure that we did not update the comment collection.
  expect(commentView.data).toEqual([{id: 'a', text: 'A', issueID: 'a'}]);

  await expect(
    z.mutate(async () => {
      await z.mutate.comment.delete({id: 'a'});
    }),
  ).rejects.toThrow('Cannot call mutate.comment.delete inside a batch');
  // make sure that we did not delete the comment row
  expect(commentView.data).toEqual([{id: 'a', text: 'A', issueID: 'a'}]);

  await expect(
    z.mutate(async () => {
      await z.mutate(() => {});
    }),
  ).rejects.toThrow('Cannot call mutate inside a batch');
});
