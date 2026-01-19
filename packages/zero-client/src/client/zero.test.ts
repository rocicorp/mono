import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from 'vitest';
import type {Write} from '../../../replicache/src/dag/store.ts';
import {
  setDeletedClients,
  type DeletedClients,
} from '../../../replicache/src/deleted-clients.ts';
import {makeClientV6} from '../../../replicache/src/persist/clients-test-helpers.ts';
import {
  getClients,
  setClients,
} from '../../../replicache/src/persist/clients.ts';
import {ReplicacheImpl} from '../../../replicache/src/replicache-impl.ts';
import type {
  ClientGroupID,
  ClientID,
} from '../../../replicache/src/sync/ids.ts';
import type {PullRequest} from '../../../replicache/src/sync/pull.ts';
import type {PushRequest} from '../../../replicache/src/sync/push.ts';
import {withWrite} from '../../../replicache/src/with-transactions.ts';
import {
  clearBrowserOverrides,
  overrideBrowserGlobal,
} from '../../../shared/src/browser-env.ts';
import {findLast} from '../../../shared/src/find-last.ts';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import * as valita from '../../../shared/src/valita.ts';
import {changeDesiredQueriesMessageSchema} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import {
  decodeSecProtocols,
  encodeSecProtocols,
  initConnectionMessageSchema,
} from '../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import * as MutationType from '../../../zero-protocol/src/mutation-type-enum.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
import {
  pushMessageSchema,
  type CRUDOp,
  type Mutation,
} from '../../../zero-protocol/src/push.ts';
import type {NullableVersion} from '../../../zero-protocol/src/version.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {DeleteID, UpdateValue} from '../../../zql/src/mutate/crud.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {
  defineMutator,
  defineMutatorWithType,
} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import type {Row} from '../../../zql/src/query/query.ts';
import {nanoid} from '../util/nanoid.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';
import type {ConnectionState} from './connection.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {DeleteClientsManager} from './delete-clients-manager.ts';
import {ClientError, isServerError} from './error.ts';
import type {WSString} from './http-string.ts';
import type {UpdateNeededReason, ZeroOptions} from './options.ts';
import type {QueryManager} from './query-manager.ts';
import {RELOAD_REASON_STORAGE_KEY} from './reload-error-handler.ts';
import type {TestZero} from './test-utils.ts';
import {
  asCustomQuery,
  MockSocket,
  queryID,
  storageMock,
  tickAFewTimes,
  waitForUpstreamMessage,
  zeroForTest,
} from './test-utils.ts'; // Why use fakes when we can use the real thing!
import {
  CONNECT_TIMEOUT_MS,
  createSocket,
  DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
  DEFAULT_PING_TIMEOUT_MS,
  getInternalReplicacheImplForTesting,
  PULL_TIMEOUT_MS,
  RUN_LOOP_INTERVAL_MS,
  type Zero,
} from './zero.ts';

const startTime = 1678829450000;

let rejectionHandler: (event: PromiseRejectionEvent) => void;
beforeEach(() => {
  vi.useFakeTimers({now: startTime});
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
  vi.stubGlobal(
    'fetch',
    vi.fn().mockReturnValue(Promise.resolve(new Response())),
  );

  rejectionHandler = event => {
    // oxlint-disable-next-line no-console
    console.error('Test rejection:', event.reason);
  };

  window.addEventListener('unhandledrejection', rejectionHandler);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

test('expose and unexpose', async () => {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const g = globalThis as any;
  expect(g.__zero).toBeUndefined();
  const z1 = zeroForTest();
  expect(g.__zero).toBe(z1);
  const z1p = z1.close();
  expect(g.__zero).toBe(z1);
  await z1p;
  expect(g.__zero).toBeUndefined();
  const z2 = zeroForTest();
  expect(g.__zero).toBe(z2);
  const z3 = zeroForTest();
  expect(g.__zero).toEqual({
    [z2.clientID]: z2,
    [z3.clientID]: z3,
  });
  const z4 = zeroForTest();
  expect(g.__zero).toEqual({
    [z2.clientID]: z2,
    [z3.clientID]: z3,
    [z4.clientID]: z4,
  });
  await z2.close();
  expect(g.__zero).toEqual({
    [z3.clientID]: z3,
    [z4.clientID]: z4,
  });
  await z3.close();
  expect(g.__zero).toBe(z4);
  await z4.close();
  expect(g.__zero).toBeUndefined();
});

test('query property provides table query builders', async () => {
  const schema = createSchema({
    tables: [
      table('user')
        .columns({
          id: string(),
          name: string(),
        })
        .primaryKey('id'),
      table('post')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyQueries: true,
  });

  const z = zeroForTest({schema});

  // query property should exist
  expect(z.query).toBeDefined();

  // Should have query builders for each table
  expect(z.query.user).toBeDefined();
  expect(z.query.post).toBeDefined();

  // Query builders should be usable
  const userQuery = z.query.user.where('id', '123');
  expect(userQuery).toBeDefined();

  const postQuery = z.query.post.where('title', 'Hello');
  expect(postQuery).toBeDefined();

  await z.close();
});

describe('onOnlineChange callback', () => {
  const getNewZero = () => {
    let onlineCount = 0;
    let offlineCount = 0;

    return {
      getOnlineCount: () => onlineCount,
      getOfflineCount: () => offlineCount,
      z: zeroForTest({
        logLevel: 'debug',
        schema: createSchema({
          tables: [
            table('foo')
              .columns({
                id: string(),
                val: string(),
              })
              .primaryKey('id'),
          ],
        }),
        onOnlineChange: online => {
          if (online) {
            onlineCount++;
          } else {
            offlineCount++;
          }
        },
      }),
    };
  };

  test('is offline by default', async () => {
    const {z} = getNewZero();
    await vi.advanceTimersByTimeAsync(1);
    expect(z.online).toBe(false);
  });

  test('does not trigger when reconnecting after close', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.online).toBe(false);
    expect(getOnlineCount()).toBe(0);
    expect(getOfflineCount()).toBe(0);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(0);
    await z.triggerClose();
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.online).toBe(false);
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('triggers after fatal error and reconnects', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    expect(z.online).toBe(true);
    await z.triggerError({
      kind: ErrorKind.InvalidMessage,
      message: 'aaa',
      origin: ErrorOrigin.Server,
    });
    await z.waitForConnectionStatus(ConnectionStatus.Error);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect.
    await z.connection.connect();
    await z.triggerConnected();
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('respects large backoff directives', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    const BACKOFF_MS = RUN_LOOP_INTERVAL_MS * 10;
    await z.triggerError({
      kind: ErrorKind.ServerOverloaded,
      message: 'slow down',
      origin: ErrorOrigin.ZeroCache,
      minBackoffMs: BACKOFF_MS,
    });
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect with the longer BACKOFF_MS.
    await tickAFewTimes(vi, BACKOFF_MS);
    await z.triggerConnected();
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('respects short backoff directives with reconnect params', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    expect(z.online).toBe(true);
    const BACKOFF_MS = 10;
    await z.triggerError({
      kind: ErrorKind.Rehome,
      message: 'rehomed',
      origin: ErrorOrigin.ZeroCache,
      maxBackoffMs: BACKOFF_MS,
      reconnectParams: {
        reason: 'rehomed',
        fromServer: 'foo/bar/baz',
      },
    });
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect with the longer BACKOFF_MS.
    await tickAFewTimes(vi, BACKOFF_MS);
    await z.triggerConnected();
    const connectMsg = findLast(
      z.testLogSink.messages,
      ([level, _context, args]) =>
        level === 'info' && args.find(arg => /Connecting to/.test(String(arg))),
    );
    expect(connectMsg?.[2][1]).matches(
      /&reason=rehomed&fromServer=foo%2Fbar%2Fbaz/,
    );
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('transitions to needs-auth on unauthorized error', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    expect(z.online).toBe(true);
    await z.triggerError({
      kind: ErrorKind.Unauthorized,
      message: 'bbb',
      origin: ErrorOrigin.ZeroCache,
    });
    await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
    expect(z.online).toBe(false);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // auth error triggered offline callback
    expect(getOfflineCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(0);
    // Call connect with new auth to resume
    await z.connection.connect({auth: 'new-token'});
    await z.triggerConnected();
    expect(z.online).toBe(true);
    // online callback triggered again after reconnect
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('stays in needs-auth state until connect is called', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await z.triggerError({
      kind: ErrorKind.PushFailed,
      reason: 'http',
      status: 401,
      message: 'ccc',
      origin: ErrorOrigin.ZeroCache,
      mutationIDs: [],
    });
    await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
    expect(z.online).toBe(false);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // auth error triggered offline callback
    expect(getOfflineCount()).toBe(1);
    // Wait a while - should stay in needs-auth state
    await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS * 10);
    expect(z.connectionStatus).toBe(ConnectionStatus.NeedsAuth);
    expect(z.online).toBe(false);
    // No additional callbacks
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // Call connect with new auth to resume
    await z.connection.connect({auth: 'new-token'});
    await z.triggerConnected();
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('triggers offline when ping times out', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS * 2);
    expect(z.online).toBe(false);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // and we got an offline callback on timeout
    expect(getOfflineCount()).toBe(1);
    // and back online
    await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);
    await z.triggerConnected();
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });
});

describe('connect error metrics', () => {
  test('server error while connected increments connectErrorCount', async () => {
    const z = zeroForTest({logLevel: 'debug'});
    try {
      await z.triggerConnected();
      await z.waitForConnectionStatus(ConnectionStatus.Connected);

      const initialLogCount = z.testLogSink.messages.length;
      await z.triggerError({
        kind: ErrorKind.Internal,
        message: 'boom',
        origin: ErrorOrigin.ZeroCache,
      });
      await z.waitForConnectionStatus(ConnectionStatus.Error);

      const newLogs = z.testLogSink.messages.slice(initialLogCount);
      const disconnectLog = newLogs.find(
        ([, , args]) => Array.isArray(args) && args[0] === 'disconnecting',
      );
      expect(disconnectLog).toBeDefined();
      const [, , args] = disconnectLog!;
      const [, payload] = args as [unknown, Record<string, unknown>];
      expect(payload.connectErrorCount).toBe(1);
      expect(payload.reason).toBe(ErrorKind.Internal);
    } finally {
      await z.close();
    }
  });

  test('abrupt close does not increment connectErrorCount', async () => {
    const z = zeroForTest({logLevel: 'debug'});
    try {
      await z.triggerConnected();
      await z.waitForConnectionStatus(ConnectionStatus.Connected);

      const initialLogCount = z.testLogSink.messages.length;
      await z.triggerClose();
      await z.waitForConnectionStatus(ConnectionStatus.Connecting);

      const newLogs = z.testLogSink.messages.slice(initialLogCount);
      const disconnectLog = newLogs.find(
        ([, , args]) => Array.isArray(args) && args[0] === 'disconnecting',
      );
      expect(disconnectLog).toBeDefined();
      const [, , args] = disconnectLog!;
      const [, payload] = args as [unknown, Record<string, unknown>];
      expect(payload.connectErrorCount).toBe(0);
      expect(payload.reason).toBe(ClientErrorKind.AbruptClose);
    } finally {
      await z.close();
    }
  });
});

test('onOnline listener', async () => {
  let online1 = 0;
  let offline1 = 0;
  let online2 = 0;
  let offline2 = 0;

  const z = zeroForTest({
    logLevel: 'debug',
  });

  const unsubscribe1 = z.onOnline(online => {
    if (online) {
      online1++;
    } else {
      offline1++;
    }
  });

  const unsubscribe2 = z.onOnline(online => {
    if (online) {
      online2++;
    } else {
      offline2++;
    }
  });

  // Offline by default.
  await vi.advanceTimersByTimeAsync(1);
  expect(z.online).toBe(false);

  // Connect: both listeners should be notified.
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(z.online).toBe(true);
  expect(online1).toBe(1);
  expect(offline1).toBe(0);
  expect(online2).toBe(1);
  expect(offline2).toBe(0);

  // Unsubscribe the first listener and trigger an error to go offline.
  unsubscribe1();
  await z.triggerError({
    kind: ErrorKind.InvalidMessage,
    message: 'oops',
    origin: ErrorOrigin.Server,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);
  await vi.advanceTimersByTimeAsync(0);
  expect(z.online).toBe(false);
  expect(online1).toBe(1);
  expect(offline1).toBe(0);
  expect(online2).toBe(1);
  expect(offline2).toBe(1);

  // Reconnect: only the second listener should be notified.
  await z.connection.connect();
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await z.triggerConnected();
  await vi.advanceTimersByTimeAsync(0);
  expect(z.online).toBe(true);
  expect(online1).toBe(1);
  expect(offline1).toBe(0);
  expect(online2).toBe(2);
  expect(offline2).toBe(1);

  unsubscribe2();
});

test('transition to connecting state if ping fails', async () => {
  const watchdogInterval = RUN_LOOP_INTERVAL_MS;
  const pingTimeout = 5000;
  const z = zeroForTest();

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  (await z.socket).messages.length = 0;

  // Wait DEFAULT_PING_TIMEOUT_MS which will trigger a ping
  // Pings timeout after DEFAULT_PING_TIMEOUT_MS so reply before that.
  await tickAFewTimes(vi, DEFAULT_PING_TIMEOUT_MS);
  expect((await z.socket).messages).toEqual(['["ping",{}]']);

  await z.triggerPong();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, watchdogInterval);
  await z.triggerPong();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, watchdogInterval);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, pingTimeout);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
});

test('does not ping when ping timeout is aborted by inbound message', async () => {
  const z = zeroForTest();

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  const socket = await z.socket;
  socket.messages.length = 0;

  await z.triggerPullResponse({
    cookie: 'cookie-1',
    requestID: 'req-1',
    lastMutationIDChanges: {},
  });

  const pingCountAfterAbort = socket.messages.filter(message =>
    message.startsWith('["ping"'),
  ).length;
  expect(pingCountAfterAbort).toBe(0);

  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);

  const pingMessages = socket.messages.filter(message =>
    message.startsWith('["ping"'),
  );
  expect(pingMessages).toHaveLength(1);

  await z.triggerPong();
});

const mockProfileID = 'pProfID1';

const mockRep = {
  query() {
    return Promise.resolve(new Map());
  },
  get profileID() {
    return Promise.resolve(mockProfileID);
  },
} as unknown as ReplicacheImpl;
const mockQueryManager = {
  getQueriesPatch() {
    return Promise.resolve([]);
  },
} as unknown as QueryManager;

const mockDeleteClientsManager = {
  getDeletedClients: () =>
    Promise.resolve([
      {
        clientGroupID: 'testClientGroupID',
        clientID: 'old-deleted-client',
      },
    ]),
} as unknown as DeleteClientsManager;

describe('createSocket', () => {
  const clientSchema: ClientSchema = {
    tables: {
      foo: {
        columns: {
          bar: {type: 'string'},
        },
        primaryKey: ['bar'],
      },
    },
  };

  test.each([
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/prefix' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/prefix/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/prefix/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/prefix/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: '1234',
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 123,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: undefined,
      lmid: 123,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: 'auth with []',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: 'auth with []',
      lmid: 0,
      debugPerf: false,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: 'auth with []',
      lmid: 0,
      debugPerf: true,
      now: 0,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&profileID=${mockProfileID}&debugPerf=true`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 456,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx&profileID=${mockProfileID}`,
    },
    {
      socketURL: 'ws://example.com/' as WSString,
      baseCookie: null,
      clientID: 'clientID',
      userID: 'userID',
      auth: '',
      lmid: 0,
      debugPerf: false,
      now: 456,
      expectedURL: `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx&profileID=${mockProfileID}&reason=rehome&backoff=100&lastTask=foo%2Fbar%26baz`,
      additionalConnectParams: {
        reason: 'rehome',
        backoff: '100',
        lastTask: 'foo/bar&baz',
        clientID: 'conflicting-parameter-ignored',
      },
    },
  ] satisfies {
    socketURL: WSString;
    baseCookie: NullableVersion;
    clientID: string;
    userID: string;
    auth: string | undefined;
    lmid: number;
    debugPerf: boolean;
    now: number;
    expectedURL: string;
    additionalConnectParams?: Record<string, string> | undefined;
  }[])(
    '$expectedURL',
    async ({
      socketURL,
      baseCookie,
      clientID,
      userID,
      auth,
      lmid,
      debugPerf,
      now,
      expectedURL,
      additionalConnectParams,
    }) => {
      const activeClients = new Set([clientID]);
      vi.spyOn(performance, 'now').mockReturnValue(now);
      const [mockSocket, queriesPatch, deletedClients] = await createSocket(
        mockRep,
        mockQueryManager,
        mockDeleteClientsManager,
        socketURL,
        baseCookie,
        clientID,
        'testClientGroupID',
        clientSchema,
        userID,
        auth,
        lmid,
        'wsidx',
        debugPerf,
        new LogContext('error', undefined, new TestLogSink()),
        undefined,
        undefined,
        undefined,
        undefined,
        additionalConnectParams,
        {activeClients},
        1048 * 8,
      );
      expect(`${mockSocket.url}`).toBe(expectedURL);
      expect(mockSocket.protocol).toBe(
        encodeSecProtocols(
          [
            'initConnection',
            {
              desiredQueriesPatch: [],
              deleted: {clientIDs: ['old-deleted-client']},
              ...(baseCookie === null ? {clientSchema} : {}),
              activeClients: [...activeClients],
            },
          ],
          auth,
        ),
      );
      expect(queriesPatch).toEqual(new Map());
      expect(deletedClients).toBeUndefined();

      const [mockSocket2, queriesPatch2, deletedClients2] = await createSocket(
        mockRep,
        mockQueryManager,
        mockDeleteClientsManager,
        socketURL,
        baseCookie,
        clientID,
        'testClientGroupID',
        clientSchema,
        userID,
        auth,
        lmid,
        'wsidx',
        debugPerf,
        new LogContext('error', undefined, new TestLogSink()),
        undefined,
        undefined,
        undefined,
        undefined,
        additionalConnectParams,
        {activeClients},
        0, // do not put any extra information into headers
      );
      expect(`${mockSocket.url}`).toBe(expectedURL);
      expect(mockSocket2.protocol).toBe(encodeSecProtocols(undefined, auth));
      // if we did not encode queries into the sec-protocol header, we should not have a queriesPatch
      expect(queriesPatch2).toBeUndefined();
      expect(deletedClients2?.clientIDs).toEqual(['old-deleted-client']);
    },
  );
});

describe('initConnection', () => {
  test('not sent when connected message received but before ConnectionStatus.Connected', async () => {
    const z = zeroForTest();
    const mockSocket = await z.socket;

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    // upon receiving `connected` we do not sent `initConnection` since it is sent
    // when opening the connection.
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength', async () => {
    const z = zeroForTest({
      maxHeaderLength: 0,
      schema: createSchema({
        tables: [
          table('abc')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });
    const mockSocket = await z.socket;
    mockSocket.onUpstream(msg => {
      expect(valita.parse(JSON.parse(msg), initConnectionMessageSchema))
        .toMatchInlineSnapshot(`
          [
            "initConnection",
            {
              "clientSchema": {
                "tables": {
                  "abc": {
                    "columns": {
                      "id": {
                        "type": "string",
                      },
                      "value": {
                        "type": "string",
                      },
                    },
                    "primaryKey": [
                      "id",
                    ],
                  },
                },
              },
              "desiredQueriesPatch": [],
            },
          ]
        `);
      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength, with deletedClients', async () => {
    const z = await zeroForTestWithDeletedClients({
      maxHeaderLength: 0,
      deletedClients: [{clientID: 'a'}],
      schema: createSchema({
        tables: [
          table('def')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const mockSocket = await z.socket;
    mockSocket.onUpstream(msg => {
      expect(valita.parse(JSON.parse(msg), initConnectionMessageSchema))
        .toMatchInlineSnapshot(`
      [
        "initConnection",
        {
          "clientSchema": {
            "tables": {
              "def": {
                "columns": {
                  "id": {
                    "type": "string",
                  },
                  "value": {
                    "type": "string",
                  },
                },
                "primaryKey": [
                  "id",
                ],
              },
            },
          },
          "deleted": {
            "clientIDs": [
              "a",
            ],
          },
          "desiredQueriesPatch": [],
        },
      ]
    `);
      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength, with deletedClientGroups', async () => {
    const z = await zeroForTestWithDeletedClients({
      maxHeaderLength: 0,
      deletedClients: [{clientGroupID: 'other', clientID: 'a'}],
      schema: createSchema({
        tables: [
          table('ijk')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const mockSocket = await z.socket;
    mockSocket.onUpstream(msg => {
      expect(valita.parse(JSON.parse(msg), initConnectionMessageSchema))
        .toMatchInlineSnapshot(`
          [
            "initConnection",
            {
              "clientSchema": {
                "tables": {
                  "ijk": {
                    "columns": {
                      "id": {
                        "type": "string",
                      },
                      "value": {
                        "type": "string",
                      },
                    },
                    "primaryKey": [
                      "id",
                    ],
                  },
                },
              },
              "desiredQueriesPatch": [],
            },
          ]
        `);
      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sends desired queries patch in sec-protocol header', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });

    const z = zeroForTest({
      schema,
    });

    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', undefined);

    const view = z.materialize(q);
    view.addListener(() => {});

    const mockSocket = await z.socket;

    expect(
      valita.parse(
        decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        activeClients: [z.clientID],
        clientSchema: {
          tables: {
            e: {
              columns: {
                id: {
                  type: 'string',
                },
                value: {
                  type: 'string',
                },
              },
              primaryKey: ['id'],
            },
          },
        },
        desiredQueriesPatch: [
          {
            args: [],
            hash: queryID(q),
            name: 'e',
            op: 'put',
            ttl: 300000,
          },
        ],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(0);
  });

  async function zeroForTestWithDeletedClients<
    const S extends Schema,
    MD extends CustomMutatorDefs | undefined = undefined,
    C = unknown,
  >(
    options: Partial<ZeroOptions<S, MD, C>> & {
      deletedClients?:
        | {clientGroupID?: ClientGroupID | undefined; clientID: ClientID}[]
        | undefined;
    },
  ): Promise<TestZero<S, MD, C>> {
    // We need to set the deleted clients before creating the zero instance but
    // we use a random name for the user ID. So we create a zero instance with a
    // random user ID, set the deleted clients, close it and then create a new
    // zero instance with the same user ID.
    const z = zeroForTest(options);
    const clientGroupID = await z.clientGroupID;

    const deletedClients =
      options.deletedClients?.map(pair => ({
        clientGroupID:
          pair.clientGroupID === undefined ? clientGroupID : pair.clientGroupID,
        clientID: pair.clientID,
      })) ?? [];

    await withWrite(z.perdag, async dagWrite => {
      await setDeletedClients(dagWrite, deletedClients);
      await addDeletedClientsToClientsMap(dagWrite, deletedClients);
    });

    await z.close();

    // Wait until all the locks are released.
    // This is needed because closing the Zero instance releases the locks
    // asynchronously and we need to wait until they are released before creating
    await vi.waitFor(async () => {
      const locks = await navigator.locks.query();
      return locks.held?.length === 0;
    });

    return zeroForTest({
      ...options,
      userID: z.userID,
    });
  }

  async function addDeletedClientsToClientsMap(
    dagWrite: Write,
    deletedClients: DeletedClients,
  ) {
    const clients = new Map(await getClients(dagWrite));
    for (const {clientGroupID, clientID} of deletedClients) {
      const c = makeClientV6({
        clientGroupID,
        heartbeatTimestampMs: Date.now(),
        refreshHashes: [],
      });

      clients.set(clientID, c);
    }
    await setClients(clients, dagWrite);
  }

  test('sends desired queries patch in sec-protocol header with deletedClients', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = await zeroForTestWithDeletedClients({
      schema,
      deletedClients: [{clientID: 'a'}],
    });

    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', undefined);
    const view = z.materialize(q);
    view.addListener(() => {});

    const mockSocket = await z.socket;

    const initConnectionMessage = valita.parse(
      decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
      initConnectionMessageSchema,
    );

    expect(initConnectionMessage).toEqual([
      'initConnection',
      {
        activeClients: [z.clientID],
        clientSchema: {
          tables: {
            e: {
              columns: {
                id: {
                  type: 'string',
                },
                value: {
                  type: 'string',
                },
              },
              primaryKey: ['id'],
            },
          },
        },
        deleted: {
          clientIDs: ['a'],
        },
        desiredQueriesPatch: [
          {
            args: [],
            hash: queryID(q),
            name: 'e',
            op: 'put',
            ttl: 300000,
          },
        ],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(0);
    await z.close();
  });

  test('sends desired queries patch in `initConnectionMessage` when the patch is over maxHeaderLength', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = zeroForTest({
      maxHeaderLength: 0,
      schema,
    });
    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', undefined);
    const mockSocket = await z.socket;

    mockSocket.onUpstream(msg => {
      expect(
        valita.parse(JSON.parse(msg), initConnectionMessageSchema),
      ).toEqual([
        'initConnection',
        {
          clientSchema: {
            tables: {
              e: {
                columns: {
                  id: {
                    type: 'string',
                  },
                  value: {
                    type: 'string',
                  },
                },
                primaryKey: ['id'],
              },
            },
          },
          desiredQueriesPatch: [
            {
              args: [],
              hash: queryID(q),
              name: 'e',
              op: 'put',
              ttl: 300000,
            },
          ],
        },
      ]);

      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    const view = z.materialize(q);
    view.addListener(() => {});
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);

    await z.close();
  });

  test('sends desired queries patch in `initConnectionMessage` when the patch is over maxHeaderLength with deleted clients', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = await zeroForTestWithDeletedClients({
      maxHeaderLength: 0,
      schema,
      deletedClients: [{clientID: 'a'}],
    });

    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', undefined);

    const mockSocket = await z.socket;

    mockSocket.onUpstream(msg => {
      expect(
        valita.parse(JSON.parse(msg), initConnectionMessageSchema),
      ).toEqual([
        'initConnection',
        {
          clientSchema: {
            tables: {
              e: {
                columns: {
                  id: {
                    type: 'string',
                  },
                  value: {
                    type: 'string',
                  },
                },
                primaryKey: ['id'],
              },
            },
          },
          deleted: {
            clientIDs: ['a'],
          },
          desiredQueriesPatch: [
            {
              args: [],
              hash: queryID(q),
              name: 'e',
              op: 'put',
              ttl: 300000,
            },
          ],
        },
      ]);

      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);

    const view = z.materialize(q);
    view.addListener(() => {});
    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sends changeDesiredQueries if new queries are added after initConnection but before connected', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = zeroForTest({schema});

    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', '💩');

    const mockSocket = await z.socket;
    mockSocket.onUpstream(msg => {
      expect(
        valita.parse(JSON.parse(msg), changeDesiredQueriesMessageSchema),
      ).toEqual([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              args: ['💩'],
              hash: queryID(q),
              name: 'e',
              op: 'put',
              ttl: 300000,
            },
          ],
        },
      ]);
      expect(z.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(
      valita.parse(
        decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        activeClients: [z.clientID],
        desiredQueriesPatch: [],
        clientSchema: {
          tables: {
            e: {
              columns: {
                id: {type: 'string'},
                value: {type: 'string'},
              },
              primaryKey: ['id'],
            },
          },
        },
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);

    const view = z.materialize(q);
    view.addListener(() => {});

    await z.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('changeDesiredQueries does not include queries sent with initConnection', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = zeroForTest({schema});

    const zql = createBuilder(schema);
    const view1 = z.materialize(zql.e);
    view1.addListener(() => {});

    const mockSocket = await z.socket;
    expect(mockSocket.messages.length).toEqual(0);

    const view2 = z.materialize(zql.e);
    view2.addListener(() => {});
    await z.triggerConnected();
    // no `changeDesiredQueries` sent since the query was already included in `initConnection`
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('changeDesiredQueries does include removal of a query sent with initConnection if it was removed before `connected`', async () => {
    const schema = createSchema({
      tables: [
        table('e')
          .columns({
            id: string(),
            value: string(),
          })
          .primaryKey('id'),
      ],
    });
    const z = zeroForTest({schema});

    const zql = createBuilder(schema);
    const q = asCustomQuery(zql.e, 'e', undefined);
    const view1 = z.materialize(q);
    const removeListener = view1.addListener(() => {});

    const hash = queryID(q);
    const mockSocket = await z.socket;
    mockSocket.onUpstream(msg => {
      expect(
        valita.parse(JSON.parse(msg), changeDesiredQueriesMessageSchema),
      ).toEqual([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              hash,
              op: 'del',
            },
          ],
        },
      ]);
    });
    expect(mockSocket.messages.length).toEqual(0);

    removeListener();
    view1.destroy();
    // no `changeDesiredQueries` sent yet since we're not connected
    expect(mockSocket.messages.length).toEqual(0);
    await z.triggerConnected();
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
    const z = zeroForTest();
    await z.triggerConnected();

    const mockSocket = await z.socket;

    for (const push of pushes) {
      const {
        mutations,
        expectedPushMessages,
        clientGroupID,
        requestID = 'test-request-id',
      } = push;

      const pushReq: PushRequest = {
        profileID: 'p1',
        clientGroupID: clientGroupID ?? (await z.clientGroupID),
        pushVersion: 1,
        schemaVersion: '1',
        mutations,
      };

      mockSocket.messages.length = 0;

      await z.pusher(pushReq, requestID);

      expect(mockSocket.messages).toHaveLength(expectedPushMessages);
      for (let i = 1; i < mockSocket.messages.length; i++) {
        const raw = mockSocket.messages[i];
        const msg = valita.parse(JSON.parse(raw), pushMessageSchema);
        expect(msg[1].clientGroupID).toBe(
          clientGroupID ?? (await z.clientGroupID),
        );
        expect(msg[1].mutations).toHaveLength(1);
        expect(msg[1].requestID).toBe(requestID);
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

test('pusher maps CRUD mutation names', async () => {
  const t = async (
    pushes: {
      client: CRUDOp[];
      server: CRUDOp[];
    }[],
  ) => {
    const z = zeroForTest({
      schema: createSchema({
        tables: [
          table('issue')
            .from('issues')
            .columns({
              id: string(),
              title: string().optional(),
            })
            .primaryKey('id'),
          table('comment')
            .from('comments')
            .columns({
              id: string(),
              issueId: string().from('issue_id'),
              text: string().optional(),
            })
            .primaryKey('id'),
          table('compoundPKTest')
            .columns({
              id1: string().from('id_1'),
              id2: string().from('id_2'),
              text: string(),
            })
            .primaryKey('id1', 'id2'),
        ],
      }),
    });

    await z.triggerConnected();

    const mockSocket = await z.socket;

    for (const push of pushes) {
      const {client, server} = push;

      const pushReq: PushRequest = {
        profileID: 'p1',
        clientGroupID: await z.clientGroupID,
        pushVersion: 1,
        schemaVersion: '1',
        mutations: [
          {
            // type: MutationType.CRUD,
            clientID: 'c2',
            id: 2,
            name: '_zero_crud',
            args: {ops: client},
            timestamp: 3,
          },
        ],
      };

      mockSocket.messages.length = 0;

      await z.pusher(pushReq, 'test-request-id');

      expect(mockSocket.messages).toHaveLength(1);
      for (let i = 0; i < mockSocket.messages.length; i++) {
        const raw = mockSocket.messages[i];
        const msg = valita.parse(JSON.parse(raw), pushMessageSchema);
        expect(msg[1].mutations[0].args[0]).toEqual({ops: server});
      }
    }
  };

  await t([
    {
      client: [
        {
          op: 'insert',
          tableName: 'issue',
          primaryKey: ['id'],
          value: {id: 'foo', ownerId: 'bar', closed: true},
        },
        {
          op: 'update',
          tableName: 'comment',
          primaryKey: ['id'],
          value: {id: 'baz', issueId: 'foo', description: 'boom'},
        },
        {
          op: 'upsert',
          tableName: 'compoundPKTest',
          primaryKey: ['id1', 'id2'],
          value: {id1: 'voo', id2: 'doo', text: 'zoo'},
        },
        {
          op: 'delete',
          tableName: 'comment',
          primaryKey: ['id'],
          value: {id: 'boo'},
        },
      ],

      server: [
        {
          op: 'insert',
          tableName: 'issues',
          primaryKey: ['id'],
          value: {id: 'foo', ownerId: 'bar', closed: true},
        },
        {
          op: 'update',
          tableName: 'comments',
          primaryKey: ['id'],
          value: {id: 'baz', ['issue_id']: 'foo', description: 'boom'},
        },
        {
          op: 'upsert',
          tableName: 'compoundPKTest',
          primaryKey: ['id_1', 'id_2'],
          value: {['id_1']: 'voo', ['id_2']: 'doo', text: 'zoo'},
        },
        {
          op: 'delete',
          tableName: 'comments',
          primaryKey: ['id'],
          value: {id: 'boo'},
        },
      ],
    },
  ]);
});

test('pusher adjusts mutation timestamps to be unix timestamps', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const mockSocket = await z.socket;
  vi.advanceTimersByTime(300); // performance.now is 500, system time is startTime + 300

  const mutations = [
    {clientID: 'c1', id: 1, name: 'mut1', args: [{d: 1}], timestamp: 100},
    {clientID: 'c2', id: 1, name: 'mut1', args: [{d: 2}], timestamp: 200},
  ];
  const requestID = 'test-request-id';

  const pushReq: PushRequest = {
    profileID: 'p1',
    clientGroupID: await z.clientGroupID,
    pushVersion: 1,
    schemaVersion: '1',
    mutations,
  };

  mockSocket.messages.length = 0;

  await z.pusher(pushReq, requestID);

  expect(mockSocket.messages).toHaveLength(mutations.length);
  const push0 = valita.parse(
    JSON.parse(mockSocket.messages[0]),
    pushMessageSchema,
  );
  expect(push0[1].mutations[0].timestamp).toBe(startTime + 100);
  const push1 = valita.parse(
    JSON.parse(mockSocket.messages[1]),
    pushMessageSchema,
  );
  expect(push1[1].mutations[0].timestamp).toBe(startTime + 200);
});

test('puller with mutation recovery pull, success response', async () => {
  const z = zeroForTest();
  await z.triggerConnected();

  const mockSocket = await z.socket;

  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
    pullVersion: 1,
    schemaVersion: z.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = z.puller(pullReq, 'test-request-id');

  await tickAFewTimes(vi);
  expect(mockSocket.messages.length).toBe(1);
  expect(JSON.parse(mockSocket.messages[0])).toEqual([
    'pull',
    {
      clientGroupID: 'test-client-group-id',
      cookie: '1',
      requestID: 'test-request-id',
    },
  ]);

  await z.triggerPullResponse({
    cookie: '2',
    requestID: 'test-request-id',
    lastMutationIDChanges: {cid1: 1},
  });

  const result = await resultPromise;

  expect(result).toEqual({
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
  const z = zeroForTest();
  await z.triggerConnected();

  const mockSocket = await z.socket;

  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
    pullVersion: 1,
    schemaVersion: z.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = z.puller(pullReq, 'test-request-id');

  await tickAFewTimes(vi);
  expect(mockSocket.messages.length).toBe(1);
  expect(JSON.parse(mockSocket.messages[0])).toEqual([
    'pull',
    {
      clientGroupID: 'test-client-group-id',
      cookie: '1',
      requestID: 'test-request-id',
    },
  ]);

  vi.advanceTimersByTime(PULL_TIMEOUT_MS);

  let expectedE = undefined;
  try {
    await resultPromise;
  } catch (e) {
    expectedE = e;
  }

  expect(expectedE).instanceOf(ClientError);
  expect((expectedE as ClientError).kind).toBe(ClientErrorKind.PullTimeout);
  expect((expectedE as ClientError).errorBody.message).toBe('Pull timed out');
});

test('puller with normal non-mutation recovery pull', async () => {
  const z = zeroForTest();
  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: await z.clientGroupID,
    cookie: '1',
    pullVersion: 1,
    schemaVersion: z.schemaVersion,
  };

  const result = await z.puller(pullReq, 'test-request-id');
  expect(fetch).not.toBeCalled();
  expect(result).toEqual({
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
  });
});

test.each([
  {name: 'socket enabled', enableServer: true},
  {name: 'socket disabled', enableServer: false},
])('smokeTest - $name', async ({enableServer}) => {
  // zeroForTest adds the socket by default.
  const serverOptions = enableServer ? {} : {server: null};
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });
  type LocalSchema = typeof schema;
  type IssueRowDef = LocalSchema['tables']['issues'];
  type Issue = Row<IssueRowDef>;
  type DeleteIssue = DeleteID<IssueRowDef>;

  const mutators = defineMutatorsWithType<typeof schema>()({
    issues: {
      insert: defineMutatorWithType<typeof schema>()<Issue>(
        async ({tx, args}) => {
          await tx.mutate.issues.insert(args);
        },
      ),
      upsert: defineMutatorWithType<typeof schema>()<Issue>(
        async ({tx, args}) => {
          await tx.mutate.issues.upsert(args);
        },
      ),
      delete: defineMutatorWithType<typeof schema>()<DeleteIssue>(
        async ({tx, args}) => {
          await tx.mutate.issues.delete(args);
        },
      ),
    },
  });
  const z = zeroForTest({
    ...serverOptions,
    schema,
    mutators,
  });

  const calls: Array<Array<unknown>> = [];
  const zql = createBuilder(schema);
  const view = z.materialize(zql.issues);
  const unsubscribe = view.addListener(c => {
    calls.push([...c]);
  });

  // const mr = mutators.issues.upsert({id: 'a', value: 1});
  // const f = mr.mutator;
  // void f;
  // z.mutate(mr)

  // oxlint-disable-next-line no-explicit-any
  type SchemaOfZ = typeof z extends Zero<infer S, any, any> ? S : never;
  expectTypeOf<SchemaOfZ>().toEqualTypeOf<LocalSchema>();

  type SchemaOfZMutate = Parameters<
    typeof z.mutate
  >[0]['mutator']['~']['$schema'];

  expectTypeOf<SchemaOfZMutate>().toEqualTypeOf<LocalSchema>();

  type SchemaOfMutatorsIssuesInsert =
    (typeof mutators.issues.insert)['~']['$schema'];
  type X = SchemaOfMutatorsIssuesInsert['enableLegacyMutators'];

  expectTypeOf<
    SchemaOfMutatorsIssuesInsert['enableLegacyMutators']
  >().toEqualTypeOf<LocalSchema['enableLegacyMutators']>();
  // not sure what this is but just wanted to get type check working
  expectTypeOf<X>().toEqualTypeOf<true>();

  await z.mutate(mutators.issues.insert({id: 'a', value: 1})).client;
  await z.mutate(mutators.issues.insert({id: 'b', value: 2})).client;

  // we get called for initial hydration, even though there's no data.
  // plus once for the each transaction
  // we test multiple changes in a transactions below
  expect(calls.length).toBe(3);
  expect(calls[0]).toEqual([]);
  expect(calls[1]).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
  expect(calls[2]).toEqual([
    {id: 'a', value: 1, [refCountSymbol]: 1},
    {id: 'b', value: 2, [refCountSymbol]: 1},
  ]);

  calls.length = 0;

  await z.mutate(mutators.issues.insert({id: 'a', value: 1})).client;
  await z.mutate(mutators.issues.insert({id: 'b', value: 2})).client;

  expect(calls.length).eq(0);

  await z.mutate(mutators.issues.upsert({id: 'a', value: 11})).client;

  // Although the set() results in a remove and add flowing through the pipeline,
  // they are in same tx, so we only get one call coming out.
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([
    {id: 'a', value: 11, [refCountSymbol]: 1},
    {id: 'b', value: 2, [refCountSymbol]: 1},
  ]);

  calls.length = 0;
  await z.mutate(mutators.issues.delete({id: 'b'})).client;
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([{id: 'a', value: 11, [refCountSymbol]: 1}]);

  unsubscribe();

  calls.length = 0;
  await z.mutate(mutators.issues.insert({id: 'c', value: 6})).client;
  expect(calls.length).eq(0);
});

test.skip('passing cacheURL null allows queries without WS connection', async () => {
  const schema = createSchema({
    tables: [
      table('tasks')
        .columns({
          id: string(),
          title: string(),
          completed: boolean(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });

  type Schema = typeof schema;

  type TasksRowDef = Schema['tables']['tasks'];
  type Task = Row<TasksRowDef>;
  type UpdateTask = UpdateValue<TasksRowDef>;
  type DeleteTask = DeleteID<TasksRowDef>;

  const mutators = defineMutatorsWithType<typeof schema>()({
    tasks: {
      insert: defineMutator<Task, Schema>(({tx, args}) =>
        tx.mutate.tasks.insert(args),
      ),
      update: defineMutator<UpdateTask, Schema>(({tx, args}) =>
        tx.mutate.tasks.update(args),
      ),
      delete: defineMutator<DeleteTask, Schema>(({tx, args}) =>
        tx.mutate.tasks.delete(args),
      ),
    },
  });

  const z = zeroForTest({
    cacheURL: null,
    schema,
    mutators,
  });

  // Queries should still work locally
  const zql = createBuilder(schema);
  const view = z.materialize(zql.tasks);
  const calls: Array<Array<unknown>> = [];
  const unsubscribe = view.addListener(c => {
    calls.push([...c]);
  });

  // Initial hydration call with empty data
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([]);

  // Mutations should work locally
  await z.mutate(
    mutators.tasks.insert({id: 't1', title: 'Task 1', completed: false}),
  ).client;
  await z.mutate(
    mutators.tasks.insert({id: 't2', title: 'Task 2', completed: true}),
  ).client;

  // Verify listener was called for each mutation
  expect(calls.length).eq(3);
  expect(calls[1]).toEqual([
    {id: 't1', title: 'Task 1', completed: false, [refCountSymbol]: 1},
  ]);
  expect(calls[2]).toEqual([
    {id: 't1', title: 'Task 1', completed: false, [refCountSymbol]: 1},
    {id: 't2', title: 'Task 2', completed: true, [refCountSymbol]: 1},
  ]);

  calls.length = 0;

  // Update mutation should work
  await z.mutate(mutators.tasks.update({id: 't1', completed: true})).client;
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([
    {id: 't1', title: 'Task 1', completed: true, [refCountSymbol]: 1},
    {id: 't2', title: 'Task 2', completed: true, [refCountSymbol]: 1},
  ]);

  calls.length = 0;

  // Delete mutation should work
  await z.mutate(mutators.tasks.delete({id: 't2'})).client;
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([
    {id: 't1', title: 'Task 1', completed: true, [refCountSymbol]: 1},
  ]);

  unsubscribe();

  // Verify connection state indicates no server connection
  // The connection status should be in Disconnected state when server is null
  expect(z.connectionStatus).toBe(ConnectionStatus.Disconnected);
});

test('Authentication', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  // Initially connecting with 'initial-token'
  let currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );

  // Trigger auth error - should transition to needs-auth
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  // Reconnect with new auth token
  await z.connection.connect({auth: 'new-token-1'});
  currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'new-token-1',
  );
  await z.triggerConnected();

  // Ping/pong should work normally
  await tickAFewTimes(vi, DEFAULT_PING_TIMEOUT_MS);
  const socket = await z.socket;
  expect(socket.messages[0]).toEqual(JSON.stringify(['ping', {}]));
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await z.triggerPong();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  // Socket is kept as long as we are connected.
  expect(await z.socket).toBe(socket);

  // Another auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error 2',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  // Reconnect with another new auth token
  await z.connection.connect({auth: 'new-token-2'});
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'new-token-2',
  );
});

test('auth errors do not auto-retry', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await z.triggerConnected();

  // Trigger first auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'first auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  // Wait a long time - should stay in needs-auth state without retrying
  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS * 100);
  expect(z.connectionStatus).toBe(ConnectionStatus.NeedsAuth);

  // Only resumes when connect() is called with new auth
  await z.connection.connect({auth: 'new-token'});
  await z.triggerConnected();
});

test(ErrorKind.AuthInvalidated, async () => {
  // In steady state we can get an AuthInvalidated error if the tokens expire on the server.
  // At this point we should disconnect and transition to needs-auth state.

  const z = zeroForTest({
    auth: 'auth-token-1',
  });

  await z.triggerConnected();
  expect(decodeSecProtocols((await z.socket).protocol).authToken).toBe(
    'auth-token-1',
  );

  await z.triggerError({
    kind: ErrorKind.AuthInvalidated,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
  await vi.advanceTimersByTimeAsync(0);

  // Reconnect with new auth token
  await z.connection.connect({auth: 'auth-token-2'});
  await z.triggerConnected();
  const reconnectingSocket = await z.socket;
  expect(decodeSecProtocols(reconnectingSocket.protocol).authToken).toBe(
    'auth-token-2',
  );
});

test('connect() with null auth clears authentication', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  let currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );

  // Trigger auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
  await vi.advanceTimersByTimeAsync(0);

  // Reconnect with null auth - should clear auth token (empty string is used for no auth)
  await z.connection.connect({auth: null});
  currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(undefined);
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('connect() with undefined auth clears authentication', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  let currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );

  // Trigger auth error
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
  await vi.advanceTimersByTimeAsync(0);

  // Reconnect with undefined auth - should clear auth token (empty string is used for no auth)
  await z.connection.connect({auth: undefined});
  currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(undefined);
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('connect() without opts preserves existing auth', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger a non-auth error
  await z.triggerError({
    kind: ErrorKind.Internal,
    message: 'internal error',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.Error);


  // Reconnect without providing auth opts - should keep existing auth
  await z.connection.connect();
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'initial-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('can start with no auth and add it later', async () => {
  const z = zeroForTest({auth: undefined});

  await z.triggerConnected();
  let currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(undefined);
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Simulate server requiring auth
  await z.triggerError({
    kind: ErrorKind.Unauthorized,
    message: 'auth required',
    origin: ErrorOrigin.ZeroCache,
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
  await vi.advanceTimersByTimeAsync(0);

  // Add auth for the first time
  await z.connection.connect({auth: 'new-auth-token'});
  currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'new-auth-token',
  );
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
});

test('PushFailed with 401 status transitions to needs-auth', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger PushFailed with 401 status
  await z.triggerError({
    kind: ErrorKind.PushFailed,
    message: 'Unauthorized',
    origin: ErrorOrigin.ZeroCache,
    reason: ErrorReason.HTTP,
    status: 401,
    mutationIDs: [],
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

  await vi.advanceTimersByTimeAsync(0);

  // Verify we can reconnect with new auth
  await z.connection.connect({auth: 'new-token'});
  await z.triggerConnected();

  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'new-token',
  );
});

test('TransformFailed with 403 status transitions to needs-auth', async () => {
  const z = zeroForTest({auth: 'initial-token'});

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  // Trigger TransformFailed with 403 status
  await z.triggerError({
    kind: ErrorKind.TransformFailed,
    message: 'Forbidden',
    origin: ErrorOrigin.ZeroCache,
    reason: ErrorReason.HTTP,
    status: 403,
    queryIDs: ['query1'],
  });
  await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);
  await vi.advanceTimersByTimeAsync(0);

  // Verify we can reconnect with new auth
  await z.connection.connect({auth: 'new-token'});
  const currentSocket = await z.socket;
  expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
    'new-token',
  );
});

test('Disconnect on error', async () => {
  const z = zeroForTest();
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await z.triggerError({
    kind: ErrorKind.InvalidMessage,
    message: 'Bad message',
    origin: ErrorOrigin.ZeroCache,
  });
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);
});

test('No backoff on errors', async () => {
  const z = zeroForTest();
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  let currentSocket = await z.socket;

  const step = async (delta: number, message: string) => {
    await z.triggerError({
      kind: ErrorKind.InvalidMessage,
      message,
      origin: ErrorOrigin.Server,
    });
    expect(z.connectionStatus).toBe(ConnectionStatus.Error);

    const nextSocketPromise = z.socket;

    await vi.advanceTimersByTimeAsync(delta - 1);
    await z.connection.connect();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(1);
    const nextSocket = await nextSocketPromise;
    // ConnectionManager may keep the public status as Connecting while a new socket is prepared,
    // so detect the retry by waiting for a fresh socket instead of expecting a Connecting state.
    expect(nextSocket).not.equal(currentSocket);
    currentSocket = nextSocket;
  };

  const steps = async () => {
    await step(5_000, 'a');
    await step(5_000, 'b');
    await step(5_000, 'c');
    await step(5_000, 'd');
  };

  await steps();

  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  currentSocket = await z.socket;

  await steps();
});

test('Ping pong', async () => {
  const z = zeroForTest();
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  (await z.socket).messages.length = 0;

  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS - 1);
  expect((await z.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);

  expect((await z.socket).messages).toEqual([JSON.stringify(['ping', {}])]);
  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS - 1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);

  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
});

test('Ping timeout', async () => {
  const z = zeroForTest();
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  (await z.socket).messages.length = 0;

  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS - 1);
  expect((await z.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);
  expect((await z.socket).messages).toEqual([JSON.stringify(['ping', {}])]);
  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS - 1);
  await z.triggerPong();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
});

test('Custom pingTimeoutMs', async () => {
  const customTimeout = 1000; // 1 second instead of default 5 seconds
  const z = zeroForTest({pingTimeoutMs: customTimeout});
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  (await z.socket).messages.length = 0;

  // Should wait customTimeout before sending ping
  await vi.advanceTimersByTimeAsync(customTimeout - 1);
  expect((await z.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);
  expect((await z.socket).messages).toEqual([JSON.stringify(['ping', {}])]);

  // Should timeout after customTimeout if no pong
  await vi.advanceTimersByTimeAsync(customTimeout - 1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
});

test('Runtime pingTimeoutMs configuration', async () => {
  const z = zeroForTest(); // Start with default timeout
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  (await z.socket).messages.length = 0;

  // Verify initial timeout is the default
  expect(z.pingTimeoutMs).toBe(DEFAULT_PING_TIMEOUT_MS);

  // First ping cycle uses default timeout
  await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
  expect((await z.socket).messages).toEqual([JSON.stringify(['ping', {}])]);
  await z.triggerPong(); // Complete first cycle
  (await z.socket).messages.length = 0;

  // Change timeout at runtime
  const newTimeout = 2000;
  z.pingTimeoutMs = newTimeout;
  expect(z.pingTimeoutMs).toBe(newTimeout);

  // New timeout should take effect on next ping cycle
  await vi.advanceTimersByTimeAsync(newTimeout - 1);
  expect((await z.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);
  expect((await z.socket).messages).toEqual([JSON.stringify(['ping', {}])]);

  // Should timeout after newTimeout if no pong
  await vi.advanceTimersByTimeAsync(newTimeout - 1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
});

const connectTimeoutMessage = 'Rejecting connect resolver due to timeout';

function expectLogMessages(z: TestZero<Schema>) {
  return expect(
    z.testLogSink.messages.flatMap(([level, _context, msg]) =>
      level === 'debug' ? msg : [],
    ),
  );
}

test('Connect timeout', async () => {
  const z = zeroForTest({logLevel: 'debug'});

  const connectionStates: ConnectionState[] = [];
  const connectionStatusCleanup = z.connection.state.subscribe(state => {
    connectionStates.push(state);
  });

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  let currentSocket = await z.socket;

  expect(connectionStates).toEqual([
    {
      name: 'connecting',
    },
  ]);

  const step = async (sleepMS: number) => {
    // Need to drain the microtask queue without changing the clock because we are
    // using the time below to check when the connect times out.
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(0);
    }

    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS - 1);
    expect(z.connectionStatus).not.toBe(ConnectionStatus.Connected);
    await vi.advanceTimersByTimeAsync(1);
    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    expectLogMessages(z).contain(connectTimeoutMessage);
    const nextSocketPromise = z.socket;

    // We stay in connecting state and sleep for RUN_LOOP_INTERVAL_MS before trying again

    await vi.advanceTimersByTimeAsync(sleepMS - 1);
    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(1);
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(0);
    }
    const nextSocket = await nextSocketPromise;
    // After the timeout the state can remain Disconnected; confirming a new socket ensures
    // the reconnect attempt is happening even without a visible status change.
    expect(nextSocket).not.equal(currentSocket);
    currentSocket = nextSocket;
  };

  await step(RUN_LOOP_INTERVAL_MS);

  // Try again to connect
  await step(RUN_LOOP_INTERVAL_MS);
  await step(RUN_LOOP_INTERVAL_MS);
  await step(RUN_LOOP_INTERVAL_MS);

  expect(connectionStates.length).toEqual(1 + 4 * 2);
  expect([...new Set(connectionStates.map(s => s.name))]).toEqual([
    'connecting',
    'disconnected',
  ]);

  // And success after this...
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  expect([...new Set(connectionStates.map(s => s.name))]).toEqual([
    'connecting',
    'disconnected',
    'connected',
  ]);

  connectionStatusCleanup();
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
    const z = zeroForTest(c.socketEnabled ? {} : {cacheURL: null});

    await z.waitForConnectionStatus(
      c.socketEnabled
        ? ConnectionStatus.Connecting
        : ConnectionStatus.Disconnected,
    );
  }
});

test('Logs errors in connect', async () => {
  const z = zeroForTest({});
  await z.triggerError({
    kind: ErrorKind.InvalidMessage,
    message: 'bad-message',
    origin: ErrorOrigin.ZeroCache,
  });
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  const index = z.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'error' && args.find(arg => /bad-message/.test(String(arg))),
  );

  expect(index).not.toBe(-1);
});

test('New connection logs', async () => {
  vi.setSystemTime(1000);
  const z = zeroForTest({logLevel: 'info'});
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(500);
  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(500);
  await z.triggerPong();
  await z.triggerClose();
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
  const connectIndex = z.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'info' &&
      args.find(arg => /Connected/.test(String(arg))) &&
      args.find(
        arg =>
          arg instanceof Object &&
          (arg as {timeToConnectMs: number}).timeToConnectMs === 500,
      ),
  );

  const disconnectIndex = z.testLogSink.messages.findIndex(
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
  expect(connectIndex).not.toBe(-1);
  expect(disconnectIndex).not.toBe(-1);
});

async function testWaitsForConnection(
  fn: (z: TestZero<Schema>) => Promise<unknown>,
) {
  const z = zeroForTest();

  const log: ('resolved' | 'rejected')[] = [];
  await z.triggerError({
    kind: ErrorKind.InvalidMessage,
    message: 'Bad message',
    origin: ErrorOrigin.ZeroCache,
  });
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  fn(z).then(
    () => log.push('resolved'),
    () => log.push('rejected'),
  );

  // Rejections that happened in previous connect should not reject pusher.
  expect(log).toEqual([]);

  // Error state requires manual connect() to resume
  await z.connection.connect();
  const reconnectPromise = z.socket;
  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);
  await reconnectPromise;

  await z.triggerError({
    kind: ErrorKind.InvalidMessage,
    message: 'Bad message',
    origin: ErrorOrigin.ZeroCache,
  });
  await tickAFewTimes(vi);
  expect(log).toEqual(['rejected']);
}

test('pusher waits for connection', async () => {
  await testWaitsForConnection(async r => {
    const pushReq: PushRequest = {
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
    const pullReq: PullRequest = {
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
  vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  const {promise, resolve} = resolver();
  const fake = vi.fn(resolve);
  const z = zeroForTest(undefined, false);
  z.reload = fake;

  await z.triggerError({
    kind: ErrorKind.VersionNotSupported,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);

  expect(storage[RELOAD_REASON_STORAGE_KEY]).toMatchInlineSnapshot(
    `"["VersionNotSupported","The server no longer supports this client's protocol version. server test message"]"`,
  );
});

test('VersionNotSupported custom onUpdateNeeded handler', async () => {
  const {promise, resolve} = resolver();
  const fake = vi.fn((_reason: UpdateNeededReason) => {
    resolve();
  });
  const z = zeroForTest({onUpdateNeeded: fake});

  await z.triggerError({
    kind: ErrorKind.VersionNotSupported,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);
  expect(fake).toHaveBeenCalledWith({
    type: 'VersionNotSupported',
    message: 'server test message',
  });
});

test('SchemaVersionNotSupported default handler', async () => {
  const storage: Record<string, string> = {};
  vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  const {promise, resolve} = resolver();
  const fake = vi.fn(resolve);
  const z = zeroForTest(undefined, false);
  z.reload = fake;

  await z.triggerError({
    kind: ErrorKind.SchemaVersionNotSupported,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);

  expect(storage[RELOAD_REASON_STORAGE_KEY]).toMatchInlineSnapshot(
    `"["SchemaVersionNotSupported","Client and server schemas incompatible. server test message"]"`,
  );
});

test('SchemaVersionNotSupported custom onUpdateNeeded handler', async () => {
  const {promise, resolve} = resolver();
  const fake = vi.fn((_reason: UpdateNeededReason) => {
    resolve();
  });
  const z = zeroForTest({onUpdateNeeded: fake});

  await z.triggerError({
    kind: ErrorKind.SchemaVersionNotSupported,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);
  expect(fake).toHaveBeenCalledWith({
    type: 'SchemaVersionNotSupported',
    message: 'server test message',
  });
});

test('ClientNotFound default handler', async () => {
  const storage: Record<string, string> = {};
  vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  const {promise, resolve} = resolver();
  const fake = vi.fn(resolve);
  const z = zeroForTest(undefined, false);
  z.reload = fake;

  await z.triggerError({
    kind: ErrorKind.ClientNotFound,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);

  expect(storage[RELOAD_REASON_STORAGE_KEY]).toBe(
    `["ClientNotFound","Server could not find state needed to synchronize this client. server test message"]`,
  );
});

test('ClientNotFound custom onClientStateNotFound handler', async () => {
  const {promise, resolve} = resolver();
  const fake = vi.fn(() => {
    resolve();
  });
  const z = zeroForTest({onClientStateNotFound: fake});
  await z.triggerError({
    kind: ErrorKind.ClientNotFound,
    message: 'server test message',
    origin: ErrorOrigin.ZeroCache,
  });
  await promise;
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);
});

test('server ahead', async () => {
  const {promise, resolve} = resolver();
  const storage: Record<string, string> = {};
  vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  const z = zeroForTest();
  z.reload = resolve;

  await z.triggerError({
    kind: ErrorKind.InvalidConnectionRequestBaseCookie,
    message: 'unexpected BaseCookie',
    origin: ErrorOrigin.ZeroCache,
  });

  await vi.waitUntil(() => storage[RELOAD_REASON_STORAGE_KEY]);

  await promise;

  expect(storage[RELOAD_REASON_STORAGE_KEY]).toEqual(
    `["InvalidConnectionRequestBaseCookie","Server reported that client is ahead of server. This probably happened because the server is in development mode and restarted. Currently when this happens, the dev server loses its state and on reconnect sees the client as ahead. If you see this in other cases, it may be a bug in Zero."]`,
  );
});

test('Constructing Zero with a negative hiddenTabDisconnectDelay option throws an error', () => {
  let expected;
  try {
    zeroForTest({hiddenTabDisconnectDelay: -1});
  } catch (e) {
    expected = e;
  }

  expect(expected).toBeInstanceOf(ClientError);
  expect(expected).toHaveProperty('kind', ClientErrorKind.Internal);
});

describe('Disconnect on hide', () => {
  const document = new (class extends EventTarget {
    visibilityState = 'visible';
  })() as Document;

  beforeEach(() => {
    overrideBrowserGlobal('document', document);

    return () => {
      clearBrowserOverrides();
      vi.resetAllMocks();
    };
  });

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
        expect(DEFAULT_PING_TIMEOUT_MS).lessThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        expect(DEFAULT_PING_TIMEOUT_MS * 2).greaterThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS); // sends ping
        timeTillHiddenDisconnect -= DEFAULT_PING_TIMEOUT_MS;
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'default delay during ping',
      test: async (r, changeVisibilityState) => {
        expect(DEFAULT_PING_TIMEOUT_MS).lessThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        expect(DEFAULT_PING_TIMEOUT_MS * 2).greaterThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS / 2);
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS / 2); // sends ping
        timeTillHiddenDisconnect -= DEFAULT_PING_TIMEOUT_MS / 2;
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(0);
      },
    },
    {
      name: 'custom delay longer than ping interval not during ping',
      hiddenTabDisconnectDelay: Math.floor(DEFAULT_PING_TIMEOUT_MS * 6.3),
      test: async (r, changeVisibilityState) => {
        let timeTillHiddenDisconnect = Math.floor(
          DEFAULT_PING_TIMEOUT_MS * 6.3,
        );
        changeVisibilityState('hidden');
        while (timeTillHiddenDisconnect > DEFAULT_PING_TIMEOUT_MS) {
          await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS); // sends ping
          timeTillHiddenDisconnect -= DEFAULT_PING_TIMEOUT_MS;
          await r.triggerPong();
        }
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'custom delay longer than ping interval during ping',
      hiddenTabDisconnectDelay: Math.floor(DEFAULT_PING_TIMEOUT_MS * 6.3),
      test: async (r, changeVisibilityState) => {
        let timeTillHiddenDisconnect = Math.floor(
          DEFAULT_PING_TIMEOUT_MS * 6.3,
        );
        expect(timeTillHiddenDisconnect > DEFAULT_PING_TIMEOUT_MS * 2);
        changeVisibilityState('hidden');
        while (timeTillHiddenDisconnect > DEFAULT_PING_TIMEOUT_MS * 2) {
          await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
          timeTillHiddenDisconnect -= DEFAULT_PING_TIMEOUT_MS;
          await r.triggerPong();
        }
        expect(timeTillHiddenDisconnect).lessThan(DEFAULT_PING_TIMEOUT_MS * 2);
        expect(timeTillHiddenDisconnect).greaterThan(DEFAULT_PING_TIMEOUT_MS);
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS); // sends ping
        timeTillHiddenDisconnect -= DEFAULT_PING_TIMEOUT_MS;
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(0);
      },
    },
    {
      name: 'custom delay shorter than ping interval not during ping',
      hiddenTabDisconnectDelay: Math.floor(DEFAULT_PING_TIMEOUT_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
        await r.triggerPong();
        const timeTillHiddenDisconnect = Math.floor(
          DEFAULT_PING_TIMEOUT_MS * 0.3,
        );
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'custom delay shorter than ping interval during ping',
      hiddenTabDisconnectDelay: Math.floor(DEFAULT_PING_TIMEOUT_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
        const timeTillHiddenDisconnect = Math.floor(
          DEFAULT_PING_TIMEOUT_MS * 0.3,
        );
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(0);
      },
    },
    {
      name: 'custom delay 0, not during ping',
      hiddenTabDisconnectDelay: 0,
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
        await r.triggerPong();
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(0);
      },
    },
    {
      name: 'custom delay 0, during ping',
      hiddenTabDisconnectDelay: 0,
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(DEFAULT_PING_TIMEOUT_MS);
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(0);
        // Disconnect due to visibility does not happen until pong is received
        // and microtask queue is processed.
        expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(0);
      },
    },
  ];

  test.each(cases)('$name', async c => {
    const {hiddenTabDisconnectDelay} = c;

    let visibilityState: DocumentVisibilityState = 'visible';
    vi.spyOn(document, 'visibilityState', 'get').mockImplementation(
      () => visibilityState,
    );
    const changeVisibilityState = (
      newVisibilityState: DocumentVisibilityState,
    ) => {
      assert(visibilityState !== newVisibilityState);
      visibilityState = newVisibilityState;
      document.dispatchEvent(new Event('visibilitychange'));
    };

    let resolveOnlineChangePromise: (v: boolean) => void = () => {};

    const z = zeroForTest({
      hiddenTabDisconnectDelay,
      onOnlineChange: online => {
        resolveOnlineChangePromise(online);
      },
    });
    const makeOnOnlineChangePromise = () =>
      new Promise(resolve => {
        resolveOnlineChangePromise = resolve;
      });
    let onOnlineChangeP = makeOnOnlineChangePromise();

    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
    expect(await onOnlineChangeP).toBe(true);
    expect(z.online).toBe(true);

    onOnlineChangeP = makeOnOnlineChangePromise();

    await c.test(z, changeVisibilityState);

    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    expect(await onOnlineChangeP).toBe(false);
    expect(z.online).toBe(false);

    // Stays disconnected as long as we are hidden.
    assert(z.connectionState.name === ConnectionStatus.Connecting);
    const timeUntilGlobalDisconnect =
      z.connectionState.disconnectAt - Date.now();
    assert(timeUntilGlobalDisconnect > 0);
    if (timeUntilGlobalDisconnect > 1) {
      await vi.advanceTimersByTimeAsync(timeUntilGlobalDisconnect - 1);
    }
    expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
    expect(z.online).false;
    expect(document.visibilityState).toBe('hidden');

    await tickAFewTimes(vi, RUN_LOOP_INTERVAL_MS);
    expect(z.connectionStatus).toBe(ConnectionStatus.Disconnected);
    expect(z.online).false;

    onOnlineChangeP = makeOnOnlineChangePromise();

    visibilityState = 'visible';
    document.dispatchEvent(new Event('visibilitychange'));

    await z.socket;
    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
    expect(await onOnlineChangeP).toBe(true);
    expect(z.online).toBe(true);

    await z.close();
  });
});

test(ErrorKind.InvalidConnectionRequest, async () => {
  const z = zeroForTest({});
  await z.triggerError({
    kind: ErrorKind.InvalidConnectionRequest,
    message: 'test',
    origin: ErrorOrigin.ZeroCache,
  });
  expect(z.connectionStatus).toBe(ConnectionStatus.Error);
  const msg = z.testLogSink.messages.at(-1);
  assert(msg);

  expect(msg[0]).toBe('error');

  const err = msg[2][1];
  assert(isServerError(err), 'error should be a server error');

  expect(err.kind).toEqual(ErrorKind.InvalidConnectionRequest);
});

describe('Invalid Downstream message', () => {
  afterEach(() => vi.resetAllMocks());

  test.each([
    {name: 'no ping', duringPing: false},
    {name: 'during ping', duringPing: true},
  ])('$name', async c => {
    const z = zeroForTest({
      logLevel: 'debug',
    });
    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

    if (c.duringPing) {
      await waitForUpstreamMessage(z, 'ping', vi);
    }

    await z.triggerPokeStart({
      // @ts-expect-error - invalid field
      pokeIDXX: '1',
      baseCookie: null,
      cookie: '1',
      timestamp: 123456,
    });

    // Invalid downstream messages trigger error state
    expect(z.online).toEqual(false);
    expect(z.connectionStatus).toEqual(ConnectionStatus.Error);
  });
});

describe('Downstream message with unknown fields', () => {
  afterEach(() => vi.resetAllMocks());

  test('unknown fields do not result in a parse error', async () => {
    const z = zeroForTest({
      logLevel: 'debug',
    });
    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

    await z.triggerPokeStart({
      pokeID: '1',
      // @ts-expect-error - invalid field
      pokeIDXX: '1',
      baseCookie: null,
      cookie: '1',
      timestamp: 123456,
    });

    expect(z.online).eq(true);
    expect(z.connectionStatus).eq(ConnectionStatus.Connected);

    expect(
      z.testLogSink.messages.some(m =>
        m[2].some(
          v =>
            v instanceof Error &&
            v.message.includes('Invalid message received from server'),
        ),
      ),
    ).toBe(false);
  });
});

describe('Downstream handler errors', () => {
  test('disconnects with internal error when handler throws', async () => {
    const spy = vi
      .spyOn(DeleteClientsManager.prototype, 'clientsDeletedOnServer')
      .mockImplementation(() => {
        throw new Error('handler boom');
      });

    const z = zeroForTest({
      logLevel: 'debug',
    });

    await z.triggerConnected();

    await z.triggerMessage([
      'deleteClients',
      {clientIDs: ['a-client']},
    ] as unknown as Downstream);

    expect(spy).toHaveBeenCalledTimes(1);
    expect(z.connectionStatus).toBe(ConnectionStatus.Error);

    assert(z.connectionState.name === ConnectionStatus.Error);
    const {reason} = z.connectionState;
    expect(reason).toBeInstanceOf(ClientError);
    expect(reason.kind).toBe(ClientErrorKind.Internal);
    expect(reason.message).toBe('handler boom');

    spy.mockRestore();

    await z.close();
  });
});

describe('Mutation responses poked down', () => {
  afterEach(() => vi.resetAllMocks());

  test('poke down partial responses, rest resolved by lmid advance', async () => {
    const schema = createSchema({
      tables: [
        table('issues')
          .columns({id: string(), value: number()})
          .primaryKey('id'),
      ],
      enableLegacyMutators: true,
    });
    const z = zeroForTest({
      logLevel: 'debug',
      schema,
      mutators: {
        issues: {
          foo: (tx: Transaction<typeof schema>, {foo}: {foo: number}) =>
            tx.mutate.issues.insert({id: foo.toString(), value: foo}),
        },
      } as const,
    });
    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

    const mutation = z.mutate.issues.foo({foo: 1});
    const mutation2 = z.mutate.issues.foo({foo: 2});
    await mutation.client;
    await mutation2.client;

    await z.triggerPoke(null, '1', {
      lastMutationIDChanges: {
        [z.clientID]: 5,
      },
      mutationsPatch: [
        {
          mutation: {
            id: {
              clientID: z.clientID,
              id: 1,
            },
            result: {
              error: 'app',
              message: '...test ',
            },
          },
          op: 'put',
        },
      ],
    });

    await vi.advanceTimersByTimeAsync(100);
    const result = await mutation.server;
    expect(result.type).toBe('error');
    assert(result.type === 'error');
    assert(result.error.type === 'app');
    expect(result.error.message).toBe('...test ');
    expect(result.error.details).toBeUndefined();

    await z.close();
  });
});

test('kvStore option', async () => {
  const spy = vi.spyOn(IDBFactory.prototype, 'open');

  type E = {
    id: string;
    value: number;
    [refCountSymbol]: number;
  };

  const localSchema = createSchema({
    tables: [
      table('e')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });
  const mutators = defineMutatorsWithType<typeof localSchema>()({
    insertE: defineMutatorWithType<typeof localSchema>()<{
      id: string;
      value: number;
    }>(({tx, args}) => tx.mutate.e.insert(args)),
  });

  const t = async (
    kvStore: ZeroOptions<typeof localSchema>['kvStore'],
    userID: string,
    expectedIDBOpenCalled: boolean,
    expectedValue: E[],
  ) => {
    const z = zeroForTest({
      userID,
      kvStore,
      schema: localSchema,
      mutators,
    });

    // Use persist as a way to ensure we have read the data out of IDB.
    await z.persist();

    const zql = createBuilder(localSchema);
    const idIsAView = z.materialize(zql.e.where('id', '=', 'a'));
    const allDataView = z.materialize(zql.e);
    expect(allDataView.data).toEqual(expectedValue);

    await z.mutate(mutators.insertE({id: 'a', value: 1})).client;

    expect(idIsAView.data).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
    // Wait for persist to finish
    await z.persist();

    await z.close();
    expect(spy.mock.calls.length > 0).toBe(expectedIDBOpenCalled);

    spy.mockClear();
  };

  const uuid = Math.random().toString().slice(2);

  await t('idb', 'kv-store-test-user-id-1' + uuid, true, []);
  await t('idb', 'kv-store-test-user-id-1' + uuid, true, [
    {id: 'a', value: 1, [refCountSymbol]: 1},
  ]);
  await t('mem', 'kv-store-test-user-id-2' + uuid, false, []);
  // Defaults to idb
  await t(undefined, 'kv-store-test-user-id-3' + uuid, true, []);
});

test('Close during connect should sleep', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
  });

  await z.triggerConnected();

  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(z.online).toBe(true);

  (await z.socket).close();
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  const reconnectAfterFirstClose = z.socket;
  await reconnectAfterFirstClose;

  (await z.socket).close();
  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  expect(z.online).toBe(false);
  await vi.advanceTimersByTimeAsync(0);
  const hasSleeping = z.testLogSink.messages.some(m =>
    m[2].some(v => v === 'Sleeping'),
  );
  expect(hasSleeping).toBe(true);

  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);

  const reconnectAfterSleep = z.socket;
  await reconnectAfterSleep;
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(z.online).toBe(true);
});

test('Zero close should stop timeout', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
  });

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await z.close();
  await z.waitForConnectionStatus(ConnectionStatus.Closed);
  expect(z.closed).toBe(true);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS);
  expectLogMessages(z).not.contain(connectTimeoutMessage);
});

test('Zero close should stop timeout, close delayed', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
  });

  await z.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS / 2);
  await z.close();
  await z.waitForConnectionStatus(ConnectionStatus.Closed);
  expect(z.closed).toBe(true);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS / 2);
  expectLogMessages(z).not.contain(connectTimeoutMessage);
});

describe('CRUD', () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .from('issues')
        .columns({
          id: string(),
          title: string().optional(),
        })
        .primaryKey('id'),
      table('comment')
        .from('comments')
        .columns({
          id: string(),
          issueID: string().from('issue_id'),
          text: string().optional(),
        })
        .primaryKey('id'),
      table('compoundPKTest')
        .columns({
          id1: string(),
          id2: string(),
          text: string(),
        })
        .primaryKey('id1', 'id2'),
    ],
  });
  const makeZero = () =>
    zeroForTest({schema: {...schema, enableLegacyMutators: true}});

  test('create', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);

    const createIssue = z.mutate.issue.insert;
    const view = z.materialize(zql.issue);
    await createIssue({id: 'a', title: 'A'});
    expect(view.data).toEqual([{id: 'a', title: 'A', [refCountSymbol]: 1}]);

    // create again should not change anything
    await createIssue({id: 'a', title: 'Again'});
    expect(view.data).toEqual([{id: 'a', title: 'A', [refCountSymbol]: 1}]);

    // Optional fields can be set to null/undefined or left off completely.
    await createIssue({id: 'b'});
    expect(view.data).toEqual([
      {id: 'a', title: 'A', [refCountSymbol]: 1},
      {id: 'b', title: null, [refCountSymbol]: 1},
    ]);

    await createIssue({id: 'c', title: undefined});
    expect(view.data).toEqual([
      {id: 'a', title: 'A', [refCountSymbol]: 1},
      {id: 'b', title: null, [refCountSymbol]: 1},
      {id: 'c', title: null, [refCountSymbol]: 1},
    ]);

    await createIssue({id: 'd', title: null});
    expect(view.data).toEqual([
      {id: 'a', title: 'A', [refCountSymbol]: 1},
      {id: 'b', title: null, [refCountSymbol]: 1},
      {id: 'c', title: null, [refCountSymbol]: 1},
      {id: 'd', title: null, [refCountSymbol]: 1},
    ]);
  });

  test('set', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);

    const view = z.materialize(zql.comment);
    await z.mutate.comment.insert({id: 'a', issueID: '1', text: 'A text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '1',
        text: 'A text',
        [refCountSymbol]: 1,
      },
    ]);

    const setComment = z.mutate.comment.upsert;
    await setComment({id: 'b', issueID: '2', text: 'B text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '1',
        text: 'A text',
        [refCountSymbol]: 1,
      },
      {
        id: 'b',
        issueID: '2',
        text: 'B text',
        [refCountSymbol]: 1,
      },
    ]);

    // set allows updating
    await setComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AA text',
        [refCountSymbol]: 1,
      },
      {
        id: 'b',
        issueID: '2',
        text: 'B text',
        [refCountSymbol]: 1,
      },
    ]);

    // Optional fields can be set to null/undefined or left off completely.
    await setComment({id: 'c', issueID: '3'});
    expect(view.data[view.data.length - 1]).toEqual({
      id: 'c',
      issueID: '3',
      text: null,
      [refCountSymbol]: 1,
    });

    await setComment({id: 'd', issueID: '4', text: undefined});
    expect(view.data[view.data.length - 1]).toEqual({
      id: 'd',
      issueID: '4',
      text: null,
      [refCountSymbol]: 1,
    });

    await setComment({id: 'e', issueID: '5', text: undefined});
    expect(view.data[view.data.length - 1]).toEqual({
      id: 'e',
      issueID: '5',
      text: null,
      [refCountSymbol]: 1,
    });

    // Setting with undefined/null/missing leaves existing values as-is
    await setComment({id: 'a', issueID: '11'});
    expect(view.data[0]).toEqual({
      id: 'a',
      issueID: '11',
      text: 'AA text',
      [refCountSymbol]: 1,
    });

    await setComment({id: 'a', issueID: '11', text: 'foo'});
    expect(view.data[0]).toEqual({
      id: 'a',
      issueID: '11',
      text: 'foo',
      [refCountSymbol]: 1,
    });

    await setComment({id: 'a', issueID: '11', text: undefined});
    expect(view.data[0]).toEqual({
      id: 'a',
      issueID: '11',
      text: 'foo',
      [refCountSymbol]: 1,
    });

    await setComment({id: 'a', issueID: '11', text: 'foo'});
    expect(view.data[0]).toEqual({
      id: 'a',
      issueID: '11',
      text: 'foo',
      [refCountSymbol]: 1,
    });
  });

  test('update', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);
    const view = z.materialize(zql.comment);
    await z.mutate.comment.insert({id: 'a', issueID: '1', text: 'A text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '1',
        text: 'A text',
        [refCountSymbol]: 1,
      },
    ]);

    const updateComment = z.mutate.comment.update;
    await updateComment({id: 'a', issueID: '11', text: 'AA text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AA text',
        [refCountSymbol]: 1,
      },
    ]);

    await updateComment({id: 'a', text: 'AAA text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
    ]);

    // update is a noop if not existing
    await updateComment({id: 'b', issueID: '2', text: 'B text'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
    ]);

    // All fields take previous value if left off or set to undefined.
    await updateComment({id: 'a', issueID: '11'});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
    ]);

    await updateComment({id: 'a', issueID: '11', text: undefined});
    expect(view.data).toEqual([
      {
        id: 'a',
        issueID: '11',
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
    ]);

    // 'optional' fields can be explicitly set to null to overwrite previous
    // value.
    await updateComment({id: 'a', issueID: '11', text: null});
    expect(view.data).toEqual([
      {id: 'a', issueID: '11', text: null, [refCountSymbol]: 1},
    ]);
  });

  test('compoundPK', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);
    const view = z.materialize(zql.compoundPKTest);
    await z.mutate.compoundPKTest.insert({id1: 'a', id2: 'a', text: 'a'});
    expect(view.data).toEqual([
      {id1: 'a', id2: 'a', text: 'a', [refCountSymbol]: 1},
    ]);

    await z.mutate.compoundPKTest.upsert({id1: 'a', id2: 'a', text: 'aa'});
    expect(view.data).toEqual([
      {id1: 'a', id2: 'a', text: 'aa', [refCountSymbol]: 1},
    ]);

    await z.mutate.compoundPKTest.update({id1: 'a', id2: 'a', text: 'aaa'});
    expect(view.data).toEqual([
      {id1: 'a', id2: 'a', text: 'aaa', [refCountSymbol]: 1},
    ]);

    await z.mutate.compoundPKTest.delete({id1: 'a', id2: 'a'});
    expect(view.data).toEqual([]);
  });

  test('do not expose _zero_crud', () => {
    const z = zeroForTest({
      schema: createSchema({
        tables: [
          table('issue')
            .columns({
              id: string(),
              title: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    expect(
      (z.mutate as unknown as Record<string, unknown>)._zero_crud,
    ).toBeUndefined();
  });
});

describe('CRUD with compound primary key', () => {
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
  const schema = createSchema({
    tables: [
      table('issue')
        .columns({
          ids: string(),
          idn: number(),
          title: string(),
        })
        .primaryKey('idn', 'ids'),
      table('comment')
        .columns({
          ids: string(),
          idn: number(),
          issueIDs: string(),
          issueIDn: number(),
          text: string(),
        })
        .primaryKey('idn', 'ids'),
    ],
  });
  const makeZero = () =>
    zeroForTest({schema: {...schema, enableLegacyMutators: true}});

  test('create', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);

    const createIssue: (issue: Issue) => Promise<void> = z.mutate.issue.insert;
    const view = z.materialize(zql.issue);
    await createIssue({ids: 'a', idn: 1, title: 'A'});
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, title: 'A', [refCountSymbol]: 1},
    ]);

    // create again should not change anything
    await createIssue({ids: 'a', idn: 1, title: 'Again'});
    expect(view.data).toEqual([
      {ids: 'a', idn: 1, title: 'A', [refCountSymbol]: 1},
    ]);
  });

  test('set', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);

    const view = z.materialize(zql.comment);
    await z.mutate.comment.insert({
      ids: 'a',
      idn: 1,
      issueIDs: 'a',
      issueIDn: 1,
      text: 'A text',
    });
    expect(view.data).toEqual([
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'a',
        issueIDn: 1,
        text: 'A text',
        [refCountSymbol]: 1,
      },
    ]);

    const setComment: (comment: Comment) => Promise<void> =
      z.mutate.comment.upsert;
    await setComment({
      ids: 'b',
      idn: 2,
      issueIDs: 'b',
      issueIDn: 2,
      text: 'B text',
    });
    expect(view.data).toEqual([
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'a',
        issueIDn: 1,
        text: 'A text',
        [refCountSymbol]: 1,
      },
      {
        ids: 'b',
        idn: 2,
        issueIDs: 'b',
        issueIDn: 2,
        text: 'B text',
        [refCountSymbol]: 1,
      },
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
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'aa',
        issueIDn: 11,
        text: 'AA text',
        [refCountSymbol]: 1,
      },
      {
        ids: 'b',
        idn: 2,
        issueIDs: 'b',
        issueIDn: 2,
        text: 'B text',
        [refCountSymbol]: 1,
      },
    ]);
  });

  test('update', async () => {
    const z = makeZero();
    const zql = createBuilder(schema);
    const view = z.materialize(zql.comment);
    await z.mutate.comment.insert({
      ids: 'a',
      idn: 1,
      issueIDs: 'a',
      issueIDn: 1,
      text: 'A text',
    });
    expect(view.data).toEqual([
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'a',
        issueIDn: 1,
        text: 'A text',
        [refCountSymbol]: 1,
      },
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
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'aa',
        issueIDn: 11,
        text: 'AA text',
        [refCountSymbol]: 1,
      },
    ]);

    await updateComment({ids: 'a', idn: 1, text: 'AAA text'});
    expect(view.data).toEqual([
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'aa',
        issueIDn: 11,
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
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
      {
        ids: 'a',
        idn: 1,
        issueIDs: 'aa',
        issueIDn: 11,
        text: 'AAA text',
        [refCountSymbol]: 1,
      },
    ]);
  });
});

test('mutate is a function for batching', async () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
      table('comment')
        .columns({
          id: string(),
          issueID: string(),
          text: string(),
        })
        .primaryKey('id'),
    ],
  });
  const z = zeroForTest({schema: {...schema, enableLegacyMutators: true}});
  const zql = createBuilder(schema);
  const issueView = z.materialize(zql.issue);
  const commentView = z.materialize(zql.comment);

  const x = await z.mutateBatch(async m => {
    expect(
      (m as unknown as Record<string, unknown>)._zero_crud,
    ).toBeUndefined();
    await m.issue.insert({id: 'a', title: 'A'});
    await m.comment.insert({
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

  expect(issueView.data).toEqual([{id: 'a', title: 'A', [refCountSymbol]: 1}]);
  expect(commentView.data).toEqual([
    {
      id: 'b',
      issueID: 'a',
      text: 'Comment for issue A was changed',
      [refCountSymbol]: 1,
    },
  ]);

  expect(
    (z.mutate as unknown as Record<string, unknown>)._zero_crud,
  ).toBeUndefined();
});

test('custom mutations get pushed', async () => {
  const schema = createSchema({
    tables: [
      table('issues').columns({id: string(), value: number()}).primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });
  const z = zeroForTest({
    schema,
    mutators: {
      issues: {
        foo: (tx: Transaction<typeof schema>, {foo}: {foo: number}) =>
          tx.mutate.issues.insert({id: foo.toString(), value: foo}),
      },
    } as const,
  });
  await z.triggerConnected();
  const mockSocket = await z.socket;
  mockSocket.messages.length = 0;

  await Promise.all([
    z.mutate.issues.foo({foo: 42}),
    z.mutate.issues.foo({foo: 43}),
  ]);
  await z.mutate.issues.foo({foo: 44});
  await tickAFewTimes(vi, RUN_LOOP_INTERVAL_MS);

  expect(
    mockSocket.messages.map(x => {
      const ret = JSON.parse(x);
      if ('requestID' in ret[1]) {
        delete ret[1].requestID;
      }
      return ret;
    }),
  ).toEqual([
    [
      'push',
      {
        timestamp: 1678829450000,
        clientGroupID: await z.clientGroupID,
        mutations: [
          {
            type: 'custom',
            timestamp: 1678829450000,
            id: 1,
            clientID: z.clientID,
            name: 'issues|foo',
            args: [{foo: 42}],
          },
        ],
        pushVersion: 1,
      },
    ],
    [
      'push',
      {
        timestamp: 1678829450000,
        clientGroupID: await z.clientGroupID,
        mutations: [
          {
            type: 'custom',
            timestamp: 1678829450000,
            id: 2,
            clientID: z.clientID,
            name: 'issues|foo',
            args: [{foo: 43}],
          },
        ],
        pushVersion: 1,
      },
    ],
    [
      'push',
      {
        timestamp: 1678829450000,
        clientGroupID: await z.clientGroupID,
        mutations: [
          {
            type: 'custom',
            timestamp: 1678829450000,
            id: 3,
            clientID: z.clientID,
            name: 'issues|foo',
            args: [{foo: 44}],
          },
        ],
        pushVersion: 1,
      },
    ],
    ['ping', {}],
  ]);
});

test('calling mutate on the non batch version should throw inside a batch', async () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
      table('comment')
        .columns({
          id: string(),
          issueID: string(),
          text: string(),
        })
        .primaryKey('id'),
    ],
  });
  const z = zeroForTest({schema: {...schema, enableLegacyMutators: true}});
  const zql = createBuilder(schema);
  const issueView = z.materialize(zql.issue);

  await z.mutateBatch(async m => {
    // This works even with the nested await because what batch is doing is
    // gathering up the mutations as data. No transaction is actually opened
    // until the callback returns.
    await m.issue.insert({id: 'a', title: 'A'});
    await z.mutate.issue.insert({id: 'b', title: 'B'});
  });

  expect(issueView.data).toEqual([
    {
      id: 'a',
      title: 'A',
      [refCountSymbol]: 1,
    },
    {
      id: 'b',
      title: 'B',
      [refCountSymbol]: 1,
    },
  ]);

  await expect(
    z.mutateBatch(async () => {
      // Because this mutate happens before the batch mutation even starts, it
      // still ends up applied.
      await z.mutate.issue.delete({id: 'a'});
      throw new Error('bonk');
    }),
  ).rejects.toThrow('bonk');

  expect(issueView.data).toEqual([{id: 'b', title: 'B', [refCountSymbol]: 1}]);

  await z.mutateBatch(async m => {
    await m.issue.insert({id: 'c', title: 'C'});
    await z.mutateBatch(async n => {
      await n.issue.delete({id: 'b'});
    });
  });

  expect(issueView.data).toEqual([{id: 'c', title: 'C', [refCountSymbol]: 1}]);
});

describe('WebSocket event error handling', () => {
  test('onOpen catches unexpected errors and transitions to error state', async () => {
    const z = zeroForTest({logLevel: 'debug'});
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    const socket = (await z.socket) as unknown as MockSocket;

    (socket as {url: string}).url = 'not a valid url';
    socket.dispatchEvent(new Event('open'));

    await z.waitForConnectionStatus(ConnectionStatus.Error);
    expect(z.connectionStatus).toBe(ConnectionStatus.Error);
    assert(z.connectionState.name === ConnectionStatus.Error);
    const {reason} = z.connectionState;
    expect(reason).toBeInstanceOf(ClientError);
    expect(reason.kind).toBe(ClientErrorKind.Internal);
    expect(reason.message).toContain('URL');
    expect(
      z.testLogSink.messages.some(
        ([level, _context, args]) =>
          level === 'error' && args[0] === 'Unhandled error in onOpen',
      ),
    ).toBe(true);

    await z.close().catch(() => {});
  });

  test('onClose catches unexpected errors and transitions to error state', async () => {
    const z = zeroForTest({logLevel: 'debug'});
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    const socket = (await z.socket) as unknown as MockSocket;

    (socket as {url: string}).url = 'still not a valid url';
    socket.dispatchEvent(
      new CloseEvent('close', {code: 1006, reason: 'test', wasClean: false}),
    );

    await z.waitForConnectionStatus(ConnectionStatus.Error);
    expect(z.connectionStatus).toBe(ConnectionStatus.Error);
    assert(z.connectionState.name === ConnectionStatus.Error);
    const {reason} = z.connectionState;
    expect(reason).toBeInstanceOf(ClientError);
    expect(reason.kind).toBe(ClientErrorKind.Internal);
    expect(reason.message).toContain('URL');
    expect(
      z.testLogSink.messages.some(
        ([level, _context, args]) =>
          level === 'error' && args[0] === 'Unhandled error in onClose',
      ),
    ).toBe(true);

    await z.close().catch(() => {});
  });
});

test('Logging stack on close', async () => {
  const z = zeroForTest({logLevel: 'debug'});
  await z.triggerConnected();
  const mockSocket = await z.socket;
  mockSocket.messages.length = 0;

  await z.close();
  await z.waitForConnectionStatus(ConnectionStatus.Closed);
  expect(z.closed).toBe(true);
  expect(z.connectionStatus).toBe(ConnectionStatus.Closed);

  expect(z.testLogSink.messages).toEqual(
    expect.arrayContaining([
      expect.arrayContaining([
        'debug',
        expect.objectContaining({
          clientID: expect.any(String),
          close: undefined,
        }),
        expect.arrayContaining([
          'Closing Zero instance. Stack:',
          expect.stringMatching(/(close).+(zero\.test\.ts)/s),
        ]),
      ]),
    ]),
  );
});

test('Close should call socket close', async () => {
  const z = zeroForTest();
  const socket = await z.socket;
  const close = (socket.close = vi.fn(socket.close));
  await z.close();
  expect(socket.closed).toBe(true);
  expect(close).toHaveBeenCalledOnce();
  expect(close).toHaveBeenCalledWith(1000);
});

test('push is called on initial connect and reconnect', async () => {
  const pushSpy = vi.spyOn(ReplicacheImpl.prototype, 'push');
  const z = zeroForTest({
    logLevel: 'debug',
    schema: createSchema({
      tables: [
        table('foo')
          .columns({
            id: string(),
            val: string(),
          })
          .primaryKey('id'),
      ],
    }),
  });

  {
    // Connect and check that we sent a push
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    expect(z.online).toBe(false);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    expect(z.online).toBe(true);
    expect(pushSpy).toBeCalledTimes(1);

    // disconnect and reconnect and check that we sent a push
    await z.triggerClose();
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    expect(pushSpy).toBeCalledTimes(2);
  }
});

test('We should send a deleteClient when a Zero instance is closed', async () => {
  // We need the same clientGroupID for both instances to test the
  // deleteClients message.
  const userID = nanoid();
  const z1 = zeroForTest({userID});
  const z2 = zeroForTest({userID});

  expect(await z1.clientGroupID).toBe(await z2.clientGroupID);

  await z1.triggerConnected();
  await z2.triggerConnected();

  expect(z1.connectionStatus).toBe(ConnectionStatus.Connected);
  expect(z2.connectionStatus).toBe(ConnectionStatus.Connected);

  const mockSocket1 = await z1.socket;
  const mockSocket2 = await z2.socket;

  expect(mockSocket1.messages).toMatchInlineSnapshot(`[]`);
  expect(mockSocket2.messages).toMatchInlineSnapshot(`[]`);

  await z1.close();

  vi.useRealTimers();

  await vi.waitFor(() => {
    expect(mockSocket1.messages).toMatchInlineSnapshot(`[]`);
    expect(mockSocket2.messages.map(s => JSON.parse(s))).toEqual([
      ['deleteClients', {clientIDs: [z1.clientID]}],
    ]);
  });

  await z2.close();
});

describe('Zero replicache refresh integration', () => {
  describe('enableRefresh', () => {
    test('enableRefresh is false when Connecting or Connected, true when Error', async () => {
      const z = zeroForTest();

      // Initial state: Connecting
      expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
      // enableRefresh should be false during connecting
      expect(z.enableRefresh()).toBe(false);

      // Connected
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
      // enableRefresh should be false when connected
      expect(z.enableRefresh()).toBe(false);

      // Trigger error to transition to Error state
      await z.triggerError({
        kind: ErrorKind.Internal,
        message: 'test error',
        origin: ErrorOrigin.ZeroCache,
      });

      await z.waitForConnectionStatus(ConnectionStatus.Error);
      expect(z.enableRefresh()).toBe(true);

      // Reconnect
      await z.connection.connect();

      // Status should transition to Connecting
      await z.waitForConnectionStatus(ConnectionStatus.Connecting);
      // enableRefresh should be false during connecting
      expect(z.enableRefresh()).toBe(false);

      // Connected again
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

      // enableRefresh should be false when connected
      expect(z.enableRefresh()).toBe(false);
    });

    test('enableRefresh is true when Disconnected', async () => {
      const z = zeroForTest();

      // Ensure connected first
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
      expect(z.enableRefresh()).toBe(false);

      // Trigger Offline error to transition to Disconnected
      z.connectionManager.disconnected(
        new ClientError({
          kind: ClientErrorKind.Offline,
          message: 'offline',
        }),
      );

      await z.waitForConnectionStatus(ConnectionStatus.Disconnected);

      // enableRefresh should be true
      expect(z.enableRefresh()).toBe(true);
    });

    test('enableRefresh is true when NeedsAuth', async () => {
      const z = zeroForTest();

      // Ensure connected first
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
      expect(z.enableRefresh()).toBe(false);

      // Trigger AuthInvalidated error to transition to NeedsAuth
      await z.triggerError({
        kind: ErrorKind.AuthInvalidated,
        message: 'session expired',
        origin: ErrorOrigin.Server,
      });

      await z.waitForConnectionStatus(ConnectionStatus.NeedsAuth);

      // enableRefresh should be true
      expect(z.enableRefresh()).toBe(true);
    });
  });

  describe('runRefresh', () => {
    test('calls runRefresh on status transition disconnect', async () => {
      const z = zeroForTest();
      const rep = getInternalReplicacheImplForTesting(z);

      // Spy on runRefresh
      const runRefreshSpy = vi
        .spyOn(rep, 'runRefresh')
        .mockImplementation(() => {
          expect(z.connectionStatus).toBe(ConnectionStatus.Error);
          expect(z.enableRefresh()).toBe(true);
          return Promise.resolve();
        });

      // Ensure connected first
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

      // Check initial state
      expect(runRefreshSpy).not.toHaveBeenCalled();

      // Trigger disconnect via error
      await z.triggerError({
        kind: ErrorKind.Internal,
        message: 'test error',
        origin: ErrorOrigin.ZeroCache,
      });

      await z.waitForConnectionStatus(ConnectionStatus.Error);

      // verify called
      await vi.waitFor(() => expect(runRefreshSpy).toHaveBeenCalled());
    });

    test('calls runRefresh on NO_STATUS_TRANSITION disconnects', async () => {
      const z = zeroForTest();
      const rep = getInternalReplicacheImplForTesting(z);

      // Spy on runRefresh
      const runRefreshSpy = vi
        .spyOn(rep, 'runRefresh')
        .mockImplementation(() => {
          // Despite connection status being Connecting, enableRefresh should
          // still be true because of #forceEnableRefresh
          expect(z.connectionStatus).toBe(ConnectionStatus.Connecting);
          expect(z.enableRefresh()).toBe(true);
          return Promise.resolve();
        });

      // Ensure connected first
      await z.triggerConnected();
      expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

      // Check initial state
      expect(runRefreshSpy).not.toHaveBeenCalled();

      // Trigger disconnect via error that causes NO_STATUS_TRANSITION
      // ErrorKind.ServerOverloaded returns NO_STATUS_TRANSITION
      await z.triggerError({
        kind: ErrorKind.ServerOverloaded,
        message: 'slow down',
        origin: ErrorOrigin.ZeroCache,
      });

      // Status should transition to Connecting because NO_STATUS_TRANSITION
      // calls connecting()
      await z.waitForConnectionStatus(ConnectionStatus.Connecting);

      // verify called
      await vi.waitFor(() => expect(runRefreshSpy).toHaveBeenCalled());
    });
  });
});
