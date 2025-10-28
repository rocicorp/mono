import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
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
import {assert} from '../../../shared/src/asserts.ts';
import {
  clearBrowserOverrides,
  overrideBrowserGlobal,
} from '../../../shared/src/browser-env.ts';
import {TestLogSink} from '../../../shared/src/logging-test-utils.ts';
import * as valita from '../../../shared/src/valita.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {changeDesiredQueriesMessageSchema} from '../../../zero-protocol/src/change-desired-queries.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import {
  decodeSecProtocols,
  encodeSecProtocols,
  initConnectionMessageSchema,
} from '../../../zero-protocol/src/connect.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import * as MutationType from '../../../zero-protocol/src/mutation-type-enum.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
import {
  pushMessageSchema,
  type CRUDOp,
  type Mutation,
} from '../../../zero-protocol/src/push.ts';
import type {NullableVersion} from '../../../zero-protocol/src/version.ts';
import {
  createSchema,
  type Schema,
} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {nanoid} from '../util/nanoid.ts';
import type {CustomMutatorDefs} from './custom.ts';
import type {DeleteClientsManager} from './delete-clients-manager.ts';
import type {WSString} from './http-string.ts';
import type {UpdateNeededReason, ZeroOptions} from './options.ts';
import type {QueryManager} from './query-manager.ts';
import {RELOAD_REASON_STORAGE_KEY} from './reload-error-handler.ts';
import {ClientError, isServerError, ServerError} from './error.ts';
import {
  MockSocket,
  storageMock,
  TestZero,
  tickAFewTimes,
  waitForUpstreamMessage,
  zeroForTest,
} from './test-utils.ts'; // Why use fakes when we can use the real thing!
import {ZeroLogContext} from './zero-log-context.ts';
import {
  CONNECT_TIMEOUT_MS,
  createSocket,
  DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
  DEFAULT_DISCONNECT_TIMEOUT_MS,
  PING_INTERVAL_MS,
  PING_TIMEOUT_MS,
  PULL_TIMEOUT_MS,
  RUN_LOOP_INTERVAL_MS,
} from './zero.ts';
import {ConnectionStatus} from './connection-status.ts';
import type {ConnectionState} from './connection-manager.ts';
import {ClientErrorKind} from './client-error-kind.ts';

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
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(0);
    await z.triggerClose();
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    // Still connected because we haven't yet failed to reconnect.
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(0);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(0);
  });

  test('triggers after fatal error and reconnects', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    await z.triggerError(ErrorKind.InvalidMessage, 'aaa');
    await z.waitForConnectionStatus(ConnectionStatus.Error);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect.
    await tickAFewTimes(vi, RUN_LOOP_INTERVAL_MS);
    await z.connection.connect();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('respects large backoff directives', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    const BACKOFF_MS = RUN_LOOP_INTERVAL_MS * 10;
    await z.triggerError(ErrorKind.ServerOverloaded, 'slow down', {
      minBackoffMs: BACKOFF_MS,
    });
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect with the longer BACKOFF_MS.
    await tickAFewTimes(vi, BACKOFF_MS);
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('respects short backoff directives with reconnect params', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    const BACKOFF_MS = 10;
    await z.triggerError(ErrorKind.Rehome, 'rehomed', {
      maxBackoffMs: BACKOFF_MS,
      reconnectParams: {
        reason: 'rehomed',
        fromServer: 'foo/bar/baz',
      },
    });
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(false);
    // we connected once and then disconnected once
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect with the longer BACKOFF_MS.
    await tickAFewTimes(vi, BACKOFF_MS);
    await z.triggerConnected();
    const connectMsg = z.testLogSink.messages.findLast(
      ([level, _context, args]) =>
        level === 'info' && args.find(arg => /Connecting to/.test(String(arg))),
    );
    expect(connectMsg?.[2][1]).matches(
      /&reason=rehomed&fromServer=foo%2Fbar%2Fbaz/,
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // we reconnected once more
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('does not trigger offline on first unauthorized error', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    await z.triggerError(ErrorKind.Unauthorized, 'bbb');
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // we did not get an offline callback on the first error, as expected
    expect(getOfflineCount()).toBe(0);
    // And followed by a reconnect.
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // online is only called once, since the first auth error
    // did not trigger an offline callback
    expect(getOnlineCount()).toBe(1);
    expect(getOfflineCount()).toBe(0);
  });

  test('triggers offline on second unauthorized error', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    await z.triggerError(ErrorKind.Unauthorized, 'ccc');
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // we did not get an offline callback on the first error, as expected
    expect(getOfflineCount()).toBe(0);
    const reconnectingSocket = z.socket;
    await reconnectingSocket;
    await z.triggerError(ErrorKind.Unauthorized, 'ddd');
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await tickAFewTimes(vi, RUN_LOOP_INTERVAL_MS);
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(false);
    expect(getOnlineCount()).toBe(1);
    // on the second error, we got an offline callback
    expect(getOfflineCount()).toBe(1);
    // And followed by a reconnect.
    const reconnectAfterOffline = z.socket;
    await reconnectAfterOffline;
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    expect(getOnlineCount()).toBe(2);
    expect(getOfflineCount()).toBe(1);
  });

  test('triggers offline when ping times out', async () => {
    const {z, getOnlineCount, getOfflineCount} = getNewZero();
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS + PING_TIMEOUT_MS);
    expect(z.online).toBe(false);
    // we connected once
    expect(getOnlineCount()).toBe(1);
    // and we got an offline callback on timeout
    expect(getOfflineCount()).toBe(1);
    // and back online
    await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);
    await z.triggerConnected();
    await vi.advanceTimersByTimeAsync(0);
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
      await z.triggerError(ErrorKind.Internal, 'boom');
      await z.waitForConnectionStatus(ConnectionStatus.Error);
      await vi.advanceTimersByTimeAsync(0);

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
      await vi.advanceTimersByTimeAsync(0);

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
  await vi.advanceTimersByTimeAsync(0);
  expect(z.online).toBe(true);
  expect(online1).toBe(1);
  expect(offline1).toBe(0);
  expect(online2).toBe(1);
  expect(offline2).toBe(0);

  // Unsubscribe the first listener and trigger an error to go offline.
  unsubscribe1();
  await z.triggerError(ErrorKind.InvalidMessage, 'oops');
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
  const r = zeroForTest();

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);

  await r.triggerConnected();
  await r.waitForConnectionStatus(ConnectionStatus.Connected);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  (await r.socket).messages.length = 0;

  // Wait PING_INTERVAL_MS which will trigger a ping
  // Pings timeout after PING_TIMEOUT_MS so reply before that.
  await tickAFewTimes(vi, PING_INTERVAL_MS);
  expect((await r.socket).messages).toEqual(['["ping",{}]']);

  await r.triggerPong();
  await tickAFewTimes(vi);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, watchdogInterval);
  await r.triggerPong();
  await tickAFewTimes(vi);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, watchdogInterval);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

  await tickAFewTimes(vi, pingTimeout);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
});

test('does not ping when ping timeout is aborted by inbound message', async () => {
  const r = zeroForTest();

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await r.triggerConnected();
  await r.waitForConnectionStatus(ConnectionStatus.Connected);

  await tickAFewTimes(vi);

  const socket = await r.socket;
  socket.messages.length = 0;

  await r.triggerPullResponse({
    cookie: 'cookie-1',
    requestID: 'req-1',
    lastMutationIDChanges: {},
  });
  await tickAFewTimes(vi);

  const pingCountAfterAbort = socket.messages.filter(message =>
    message.startsWith('["ping"'),
  ).length;
  expect(pingCountAfterAbort).toBe(0);

  await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);

  const pingMessages = socket.messages.filter(message =>
    message.startsWith('["ping"'),
  );
  expect(pingMessages).toHaveLength(1);

  await r.triggerPong();
  await tickAFewTimes(vi);
});

const mockRep = {
  query() {
    return Promise.resolve(new Map());
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
  const t = (
    socketURL: WSString,
    baseCookie: NullableVersion,
    clientID: string,
    userID: string,
    auth: string | undefined,
    lmid: number,
    debugPerf: boolean,
    now: number,
    expectedURL: string,
    additionalConnectParams?: Record<string, string>,
    activeClients = new Set([clientID]),
  ) => {
    const clientSchema: ClientSchema = {
      tables: {
        foo: {
          columns: {
            bar: {type: 'string'},
          },
        },
      },
    };

    test(expectedURL, async () => {
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
        new ZeroLogContext('error', undefined, new TestLogSink()),
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
        new ZeroLogContext('error', undefined, new TestLogSink()),
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
    });
  };

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    0,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx`,
  );
  t(
    'ws://example.com/prefix',
    null,
    'clientID',
    'userID',
    '',
    0,
    false,
    0,
    `ws://example.com/prefix/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx`,
  );
  t(
    'ws://example.com/prefix/',
    null,
    'clientID',
    'userID',
    '',
    0,
    false,
    0,
    `ws://example.com/prefix/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    '1234',
    'clientID',
    'userID',
    '',
    0,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    '1234',
    'clientID',
    'userID',
    '',
    0,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=1234&ts=0&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    123,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    undefined,
    123,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=123&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    0,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    0,
    false,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    'auth with []',
    0,
    true,
    0,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=0&lmid=0&wsid=wsidx&debugPerf=true`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    0,
    false,
    456,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx`,
  );

  t(
    'ws://example.com/',
    null,
    'clientID',
    'userID',
    '',
    0,
    false,
    456,
    `ws://example.com/sync/v${PROTOCOL_VERSION}/connect?clientID=clientID&clientGroupID=testClientGroupID&userID=userID&baseCookie=&ts=456&lmid=0&wsid=wsidx&reason=rehome&backoff=100&lastTask=foo%2Fbar%26baz`,
    {
      reason: 'rehome',
      backoff: '100',
      lastTask: 'foo/bar&baz',
      clientID: 'conflicting-parameter-ignored',
    },
  );
});

describe('initConnection', () => {
  test('not sent when connected message received but before ConnectionStatus.Connected', async () => {
    const r = zeroForTest();
    const mockSocket = await r.socket;

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    // upon receiving `connected` we do not sent `initConnection` since it is sent
    // when opening the connection.
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength', async () => {
    const r = zeroForTest({
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
    const mockSocket = await r.socket;
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
      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength, with deletedClients', async () => {
    const r = await zeroForTestWithDeletedClients({
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

    const mockSocket = await r.socket;
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
      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sent when connected message received but before ConnectionStatus.Connected desired queries > maxHeaderLength, with deletedClientGroups', async () => {
    const r = await zeroForTestWithDeletedClients({
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

    const mockSocket = await r.socket;
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
      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sends desired queries patch in sec-protocol header', async () => {
    const r = zeroForTest({
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const view = r.query.e.materialize();
    view.addListener(() => {});

    const mockSocket = await r.socket;

    expect(
      valita.parse(
        decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        activeClients: [r.clientID],
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
            ast: {
              orderBy: [['id', 'asc']],
              table: 'e',
            },
            hash: '29j3x0l4bxthp',
            op: 'put',
            ttl: 300000,
          },
        ],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(0);
  });

  async function zeroForTestWithDeletedClients<
    const S extends Schema,
    MD extends CustomMutatorDefs = CustomMutatorDefs,
  >(
    options: Partial<ZeroOptions<S, MD>> & {
      deletedClients?:
        | {clientGroupID?: ClientGroupID | undefined; clientID: ClientID}[]
        | undefined;
    },
  ): Promise<TestZero<S, MD>> {
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
    const r = await zeroForTestWithDeletedClients({
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
      deletedClients: [{clientID: 'a'}],
    });

    const view = r.query.e.materialize();
    view.addListener(() => {});

    const mockSocket = await r.socket;

    const initConnectionMessage = valita.parse(
      decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
      initConnectionMessageSchema,
    );

    expect(initConnectionMessage).toEqual([
      'initConnection',
      {
        activeClients: [r.clientID],
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
            ast: {
              orderBy: [['id', 'asc']],
              table: 'e',
            },
            hash: '29j3x0l4bxthp',
            op: 'put',
            ttl: 300000,
          },
        ],
      },
    ]);

    expect(mockSocket.messages.length).toEqual(0);
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(0);
    await r.close();
  });

  test('sends desired queries patch in `initConnectionMessage` when the patch is over maxHeaderLength', async () => {
    const r = zeroForTest({
      maxHeaderLength: 0,
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });
    const mockSocket = await r.socket;

    mockSocket.onUpstream(msg => {
      expect(valita.parse(JSON.parse(msg), initConnectionMessageSchema))
        .toMatchInlineSnapshot(`
              [
                "initConnection",
                {
                  "clientSchema": {
                    "tables": {
                      "e": {
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
                  "desiredQueriesPatch": [
                    {
                      "ast": {
                        "orderBy": [
                          [
                            "id",
                            "asc",
                          ],
                        ],
                        "table": "e",
                      },
                      "hash": "29j3x0l4bxthp",
                      "op": "put",
                      "ttl": 300000,
                    },
                  ],
                },
              ]
            `);

      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    const view = r.query.e.materialize();
    view.addListener(() => {});
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);

    await r.close();
  });

  test('sends desired queries patch in `initConnectionMessage` when the patch is over maxHeaderLength with deleted clients', async () => {
    const r = await zeroForTestWithDeletedClients({
      maxHeaderLength: 0,
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
      deletedClients: [{clientID: 'a'}],
    });
    const mockSocket = await r.socket;

    mockSocket.onUpstream(msg => {
      expect(valita.parse(JSON.parse(msg), initConnectionMessageSchema))
        .toMatchInlineSnapshot(`
                [
                  "initConnection",
                  {
                    "clientSchema": {
                      "tables": {
                        "e": {
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
                    "desiredQueriesPatch": [
                      {
                        "ast": {
                          "orderBy": [
                            [
                              "id",
                              "asc",
                            ],
                          ],
                          "table": "e",
                        },
                        "hash": "29j3x0l4bxthp",
                        "op": "put",
                        "ttl": 300000,
                      },
                    ],
                  },
                ]
              `);

      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(mockSocket.messages.length).toEqual(0);
    const view = r.query.e.materialize();
    view.addListener(() => {});
    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('sends changeDesiredQueries if new queries are added after initConnection but before connected', async () => {
    const r = zeroForTest({
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const mockSocket = await r.socket;
    mockSocket.onUpstream(msg => {
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
              hash: '29j3x0l4bxthp',
              op: 'put',
              ttl: 300000,
            },
          ],
        },
      ]);
      expect(r.connectionStatus).toEqual(ConnectionStatus.Connecting);
    });

    expect(
      valita.parse(
        decodeSecProtocols(mockSocket.protocol).initConnectionMessage,
        initConnectionMessageSchema,
      ),
    ).toEqual([
      'initConnection',
      {
        activeClients: [r.clientID],
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

    const view = r.query.e.materialize();
    view.addListener(() => {});

    await r.triggerConnected();
    expect(mockSocket.messages.length).toEqual(1);
  });

  test('changeDesiredQueries does not include queries sent with initConnection', async () => {
    const r = zeroForTest({
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const view1 = r.query.e.materialize();
    view1.addListener(() => {});

    const mockSocket = await r.socket;
    expect(mockSocket.messages.length).toEqual(0);

    const view2 = r.query.e.materialize();
    view2.addListener(() => {});
    await r.triggerConnected();
    // no `changeDesiredQueries` sent since the query was already included in `initConnection`
    expect(mockSocket.messages.length).toEqual(0);
  });

  test('changeDesiredQueries does include removal of a query sent with initConnection if it was removed before `connected`', async () => {
    const r = zeroForTest({
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: string(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const view1 = r.query.e.materialize();
    const removeListener = view1.addListener(() => {});

    const mockSocket = await r.socket;
    mockSocket.onUpstream(msg => {
      expect(
        valita.parse(JSON.parse(msg), changeDesiredQueriesMessageSchema),
      ).toEqual([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [
            {
              hash: '29j3x0l4bxthp',
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

      const pushReq: PushRequest = {
        profileID: 'p1',
        clientGroupID: clientGroupID ?? (await r.clientGroupID),
        pushVersion: 1,
        schemaVersion: '1',
        mutations,
      };

      mockSocket.messages.length = 0;

      await r.pusher(pushReq, requestID);

      expect(mockSocket.messages).toHaveLength(expectedPushMessages);
      for (let i = 1; i < mockSocket.messages.length; i++) {
        const raw = mockSocket.messages[i];
        const msg = valita.parse(JSON.parse(raw), pushMessageSchema);
        expect(msg[1].clientGroupID).toBe(
          clientGroupID ?? (await r.clientGroupID),
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
    const r = zeroForTest({
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

    await r.triggerConnected();

    const mockSocket = await r.socket;

    for (const push of pushes) {
      const {client, server} = push;

      const pushReq: PushRequest = {
        profileID: 'p1',
        clientGroupID: await r.clientGroupID,
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

      await r.pusher(pushReq, 'test-request-id');

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
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;
  vi.advanceTimersByTime(300); // performance.now is 500, system time is startTime + 300

  const mutations = [
    {clientID: 'c1', id: 1, name: 'mut1', args: [{d: 1}], timestamp: 100},
    {clientID: 'c2', id: 1, name: 'mut1', args: [{d: 2}], timestamp: 200},
  ];
  const requestID = 'test-request-id';

  const pushReq: PushRequest = {
    profileID: 'p1',
    clientGroupID: await r.clientGroupID,
    pushVersion: 1,
    schemaVersion: '1',
    mutations,
  };

  mockSocket.messages.length = 0;

  await r.pusher(pushReq, requestID);

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
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = r.puller(pullReq, 'test-request-id');

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

  await r.triggerPullResponse({
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
  const r = zeroForTest();
  await r.triggerConnected();

  const mockSocket = await r.socket;

  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: 'test-client-group-id',
    cookie: '1',
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };
  mockSocket.messages.length = 0;

  const resultPromise = r.puller(pullReq, 'test-request-id');

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
  const r = zeroForTest();
  const pullReq: PullRequest = {
    profileID: 'test-profile-id',
    clientGroupID: await r.clientGroupID,
    cookie: '1',
    pullVersion: 1,
    schemaVersion: r.schemaVersion,
  };

  const result = await r.puller(pullReq, 'test-request-id');
  expect(fetch).not.toBeCalled();
  expect(result).toEqual({
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
      schema: createSchema({
        tables: [
          table('issues')
            .columns({
              id: string(),
              value: number(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    const calls: Array<Array<unknown>> = [];
    const view = r.query.issues.materialize();
    const unsubscribe = view.addListener(c => {
      calls.push([...c]);
    });

    await r.mutate.issues.insert({id: 'a', value: 1});
    await r.mutate.issues.insert({id: 'b', value: 2});

    // we get called for initial hydration, even though there's no data.
    // plus once for the each transaction
    // we test multiple changes in a transactions below
    expect(calls.length).eq(3);
    expect(calls[0]).toEqual([]);
    expect(calls[1]).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
    expect(calls[2]).toEqual([
      {id: 'a', value: 1, [refCountSymbol]: 1},
      {id: 'b', value: 2, [refCountSymbol]: 1},
    ]);

    calls.length = 0;

    await r.mutate.issues.insert({id: 'a', value: 1});
    await r.mutate.issues.insert({id: 'b', value: 2});

    expect(calls.length).eq(0);

    await r.mutate.issues.upsert({id: 'a', value: 11});

    // Although the set() results in a remove and add flowing through the pipeline,
    // they are in same tx, so we only get one call coming out.
    expect(calls.length).eq(1);
    expect(calls[0]).toEqual([
      {id: 'a', value: 11, [refCountSymbol]: 1},
      {id: 'b', value: 2, [refCountSymbol]: 1},
    ]);

    calls.length = 0;
    await r.mutate.issues.delete({id: 'b'});
    expect(calls.length).eq(1);
    expect(calls[0]).toEqual([{id: 'a', value: 11, [refCountSymbol]: 1}]);

    unsubscribe();

    calls.length = 0;
    await r.mutate.issues.insert({id: 'c', value: 6});
    expect(calls.length).eq(0);
  }
});

test('passing server null allows queries without WS connection', async () => {
  const r = zeroForTest({
    server: null,
    schema: createSchema({
      tables: [
        table('tasks')
          .columns({
            id: string(),
            title: string(),
            completed: boolean(),
          })
          .primaryKey('id'),
      ],
    }),
  });

  // Queries should still work locally
  const view = r.query.tasks.materialize();
  const calls: Array<Array<unknown>> = [];
  const unsubscribe = view.addListener(c => {
    calls.push([...c]);
  });

  // Initial hydration call with empty data
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([]);

  // Mutations should work locally
  await r.mutate.tasks.insert({id: 't1', title: 'Task 1', completed: false});
  await r.mutate.tasks.insert({id: 't2', title: 'Task 2', completed: true});

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
  await r.mutate.tasks.update({id: 't1', completed: true});
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([
    {id: 't1', title: 'Task 1', completed: true, [refCountSymbol]: 1},
    {id: 't2', title: 'Task 2', completed: true, [refCountSymbol]: 1},
  ]);

  calls.length = 0;

  // Delete mutation should work
  await r.mutate.tasks.delete({id: 't2'});
  expect(calls.length).eq(1);
  expect(calls[0]).toEqual([
    {id: 't1', title: 'Task 1', completed: true, [refCountSymbol]: 1},
  ]);

  unsubscribe();

  // Verify connection state indicates no server connection
  // The connection status should be in Error state when server is null
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);
});

// TODO: Reenable metrics
// test('Metrics', async () => {
//   // This is just a smoke test -- it ensures that we send metrics once at startup.
//   // Ideally we would run Zero and put it into different error conditions and see
//   // that the metrics are reported appropriately.

//   const r = zeroForTest();
//   await r.waitForConnectionStatus(ConnectionStatus.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionStatus(ConnectionStatus.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).toBe(true);
// });

// test('Metrics not reported when enableAnalytics is false', async () => {
//   const r = zeroForTest({enableAnalytics: false});
//   await r.waitForConnectionStatus(ConnectionStatus.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionStatus(ConnectionStatus.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).toBe(false);
// });

// test('Metrics not reported when server indicates local development', async () => {
//   const r = zeroForTest({server: 'http://localhost:8000'});
//   await r.waitForConnectionStatus(ConnectionStatus.Connecting);
//   await r.triggerConnected();
//   await r.waitForConnectionStatus(ConnectionStatus.Connected);

//   for (let t = 0; t < REPORT_INTERVAL_MS; t += PING_INTERVAL_MS) {
//     await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
//     await r.triggerPong();
//   }

//   expect(
//     fetchStub.calledWithMatch(
//       sinon.match(new RegExp('^https://example.com/api/metrics/v0/report?.*')),
//     ),
//   ).toBe(false);
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
  let currentSocket = await r.socket;

  const emulateErrorWhenConnecting = async (
    tickMS: number,
    expectedAuthToken: string,
    expectedTimeOfCall: number,
  ) => {
    expect(decodeSecProtocols(currentSocket.protocol).authToken).toBe(
      expectedAuthToken,
    );
    await r.triggerError(ErrorKind.Unauthorized, 'auth error ' + authCounter);
    expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(tickMS);
    expect(log).length(1);
    expect(log[0]).toBe(expectedTimeOfCall);
    log.length = 0;
    currentSocket = await r.socket;
    expect(r.connectionStatus).equal(ConnectionStatus.Connecting);
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
    socket = await r.socket;
    expect(decodeSecProtocols(socket.protocol).authToken).toBe(
      'new-auth-token-8',
    );
    await r.triggerConnected();
    await r.waitForConnectionStatus(ConnectionStatus.Connected);
    // getAuth should not be called again.
    expect(log).toHaveLength(0);
  }

  {
    // Ping/pong should happen every 5 seconds.
    await tickAFewTimes(vi, PING_INTERVAL_MS);
    const socket = await r.socket;
    expect(socket.messages[0]).toEqual(JSON.stringify(['ping', {}]));
    expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
    await r.triggerPong();
    expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
    // getAuth should not be called again.
    expect(log).toHaveLength(0);
    // Socket is kept as long as we are connected.
    expect(await r.socket).toBe(socket);
  }

  {
    await r.triggerError(ErrorKind.Unauthorized, 'auth error ' + authCounter);
    currentSocket = await r.socket;

    // wait the full timeout period and we should be disconnected now
    await vi.advanceTimersByTimeAsync(5 * 60 * 1_000);
    await r.waitForConnectionStatus(ConnectionStatus.Disconnected);
  }
});

test('throttles reauth on rapid auth errors', async () => {
  const authCallTimes: number[] = [];
  let nextToken = 1;
  const auth = () => {
    const now = Date.now();
    authCallTimes.push(now);
    return `auth-token-${nextToken++}`;
  };

  const r = zeroForTest({auth});

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await r.triggerConnected();
  await r.waitForConnectionStatus(ConnectionStatus.Connected);

  await vi.advanceTimersByTimeAsync(0);
  expect(authCallTimes).length(1);
  const initialAuthTime = authCallTimes.shift();
  expect(initialAuthTime).toBe(startTime);

  await r.triggerError(ErrorKind.Unauthorized, 'first auth error');
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(0);
  expect(authCallTimes).length(1);
  expect(authCallTimes[0]).toBe(startTime);

  const reconnectingSocketAfterFirstError = r.socket;
  await reconnectingSocketAfterFirstError;
  await r.triggerConnected();
  await r.waitForConnectionStatus(ConnectionStatus.Connected);

  await r.triggerError(ErrorKind.Unauthorized, 'second auth error');
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(0);
  expect(authCallTimes).length(1);
  const reconnectingSocketAfterSecondError = r.socket;

  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS - 1);
  expect(authCallTimes).length(1);

  await vi.advanceTimersByTimeAsync(1);
  await reconnectingSocketAfterSecondError;
  expect(authCallTimes).length(2);
  expect(authCallTimes[1]).toBe(startTime + RUN_LOOP_INTERVAL_MS);
});

test(ErrorKind.AuthInvalidated, async () => {
  // In steady state we can get an AuthInvalidated error if the tokens expire on the server.
  // At this point we should disconnect and reconnect with a new auth token.

  let authCounter = 1;

  const r = zeroForTest({
    auth: () => `auth-token-${authCounter++}`,
  });

  await r.triggerConnected();
  expect(decodeSecProtocols((await r.socket).protocol).authToken).toBe(
    'auth-token-1',
  );

  await r.triggerError(ErrorKind.AuthInvalidated, 'auth error');
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);

  const reconnectingSocket = await r.socket;
  expect(decodeSecProtocols(reconnectingSocket.protocol).authToken).toBe(
    'auth-token-2',
  );
});

test('Disconnect on error', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);
});

test('No backoff on errors', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  let currentSocket = await r.socket;

  const step = async (delta: number, message: string) => {
    await r.triggerError(ErrorKind.InvalidMessage, message);
    expect(r.connectionStatus).toBe(ConnectionStatus.Error);

    const nextSocketPromise = r.socket;

    await vi.advanceTimersByTimeAsync(delta - 1);
    await r.connection.connect();
    expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
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

  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  currentSocket = await r.socket;

  await steps();
});

test('Ping pong', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  (await r.socket).messages.length = 0;

  await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS - 1);
  expect((await r.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);

  expect((await r.socket).messages).toEqual([JSON.stringify(['ping', {}])]);
  await vi.advanceTimersByTimeAsync(PING_TIMEOUT_MS - 1);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);

  expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
});

test('Ping timeout', async () => {
  const r = zeroForTest();
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  (await r.socket).messages.length = 0;

  await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS - 1);
  expect((await r.socket).messages).toHaveLength(0);
  await vi.advanceTimersByTimeAsync(1);
  expect((await r.socket).messages).toEqual([JSON.stringify(['ping', {}])]);
  await vi.advanceTimersByTimeAsync(PING_TIMEOUT_MS - 1);
  await r.triggerPong();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(1);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
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

  const connectionStates: ConnectionState[] = [];
  const connectionStatusCleanup = r.subscribeToConnectionStatus(state => {
    connectionStates.push(state);
  });

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  let currentSocket = await r.socket;

  expect(connectionStates).toEqual([
    {
      name: ConnectionStatus.Connecting,
      attempt: 1,
      disconnectAt: DEFAULT_DISCONNECT_TIMEOUT_MS + startTime,
      reason: undefined,
    },
  ]);

  const step = async (sleepMS: number) => {
    // Need to drain the microtask queue without changing the clock because we are
    // using the time below to check when the connect times out.
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(0);
    }

    expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
    await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS - 1);
    expect(r.connectionStatus).not.toBe(ConnectionStatus.Connected);
    await vi.advanceTimersByTimeAsync(1);
    expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
    expectLogMessages(r).contain(connectTimeoutMessage);
    const nextSocketPromise = r.socket;

    // We stay in connecting state and sleep for RUN_LOOP_INTERVAL_MS before trying again

    await vi.advanceTimersByTimeAsync(sleepMS - 1);
    expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
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
    ConnectionStatus.Connecting,
  ]);

  // And success after this...
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  expect([...new Set(connectionStates.map(s => s.name))]).toEqual([
    ConnectionStatus.Connecting,
    ConnectionStatus.Connected,
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
    const r = zeroForTest(c.socketEnabled ? {} : {server: null});

    await tickAFewTimes(vi);

    expect(r.connectionStatus, c.name).toBe(
      c.socketEnabled ? ConnectionStatus.Connecting : ConnectionStatus.Error,
    );
  }
});

test('Logs errors in connect', async () => {
  const r = zeroForTest({});
  await r.triggerError(ErrorKind.InvalidMessage, 'bad-message');
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);
  await vi.advanceTimersByTimeAsync(0);

  const index = r.testLogSink.messages.findIndex(
    ([level, _context, args]) =>
      level === 'error' && args.find(arg => /bad-message/.test(String(arg))),
  );

  expect(index).not.toBe(-1);
});

test('New connection logs', async () => {
  vi.setSystemTime(1000);
  const r = zeroForTest({logLevel: 'info'});
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(500);
  await r.triggerConnected();
  expect(r.connectionStatus).toBe(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(500);
  await r.triggerPong();
  await r.triggerClose();
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  expect(r.connectionStatus).toBe(ConnectionStatus.Connecting);
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
  expect(connectIndex).not.toBe(-1);
  expect(disconnectIndex).not.toBe(-1);
});

async function testWaitsForConnection(
  fn: (r: TestZero<Schema>) => Promise<unknown>,
) {
  const r = zeroForTest();

  const log: ('resolved' | 'rejected')[] = [];
  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

  fn(r).then(
    () => log.push('resolved'),
    () => log.push('rejected'),
  );

  await tickAFewTimes(vi);

  // Rejections that happened in previous connect should not reject pusher.
  expect(log).toEqual([]);

  // Error state requires manual connect() to resume
  await r.connection.connect();
  const reconnectPromise = r.socket;
  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);
  await reconnectPromise;

  await r.triggerError(ErrorKind.InvalidMessage, 'Bad message');
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
  const r = zeroForTest(undefined, false);
  r.reload = fake;

  await r.triggerError(ErrorKind.VersionNotSupported, 'server test message');
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

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
  const r = zeroForTest({onUpdateNeeded: fake});

  await r.triggerError(ErrorKind.VersionNotSupported, 'server test message');
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

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
  const r = zeroForTest(undefined, false);
  r.reload = fake;

  await r.triggerError(
    ErrorKind.SchemaVersionNotSupported,
    'server test message',
  );
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

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
  const r = zeroForTest({onUpdateNeeded: fake});

  await r.triggerError(
    ErrorKind.SchemaVersionNotSupported,
    'server test message',
  );
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

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
  const r = zeroForTest(undefined, false);
  r.reload = fake;

  await r.triggerError(ErrorKind.ClientNotFound, 'server test message');
  await vi.advanceTimersToNextTimerAsync();
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

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
  const r = zeroForTest({onClientStateNotFound: fake});
  await r.triggerError(ErrorKind.ClientNotFound, 'server test message');
  await promise;
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);

  expect(fake).toBeCalledTimes(1);
});

test('server ahead', async () => {
  const {promise, resolve} = resolver();
  const storage: Record<string, string> = {};
  vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() =>
    storageMock(storage),
  );
  const r = zeroForTest();
  r.reload = resolve;

  await r.triggerError(
    ErrorKind.InvalidConnectionRequestBaseCookie,
    'unexpected BaseCookie',
  );

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
        expect(PING_INTERVAL_MS).lessThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        expect(PING_INTERVAL_MS * 2).greaterThanOrEqual(
          DEFAULT_DISCONNECT_HIDDEN_DELAY_MS,
        );
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS;
        await r.triggerPong();
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
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
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS / 2);
        let timeTillHiddenDisconnect = DEFAULT_DISCONNECT_HIDDEN_DELAY_MS;
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS / 2); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS / 2;
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
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 6.3),
      test: async (r, changeVisibilityState) => {
        let timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 6.3);
        changeVisibilityState('hidden');
        while (timeTillHiddenDisconnect > PING_INTERVAL_MS) {
          await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS); // sends ping
          timeTillHiddenDisconnect -= PING_INTERVAL_MS;
          await r.triggerPong();
        }
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
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
          await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
          timeTillHiddenDisconnect -= PING_INTERVAL_MS;
          await r.triggerPong();
        }
        expect(timeTillHiddenDisconnect).lessThan(
          PING_INTERVAL_MS + PING_TIMEOUT_MS,
        );
        expect(timeTillHiddenDisconnect).greaterThan(PING_INTERVAL_MS);
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS); // sends ping
        timeTillHiddenDisconnect -= PING_INTERVAL_MS;
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
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
        await r.triggerPong();
        const timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 0.3);
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(timeTillHiddenDisconnect);
      },
    },
    {
      name: 'custom delay shorter than ping interval during ping',
      hiddenTabDisconnectDelay: Math.floor(PING_INTERVAL_MS * 0.3),
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
        const timeTillHiddenDisconnect = Math.floor(PING_INTERVAL_MS * 0.3);
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
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
        await r.triggerPong();
        changeVisibilityState('hidden');
        await vi.advanceTimersByTimeAsync(0);
      },
    },
    {
      name: 'custom delay 0, during ping',
      hiddenTabDisconnectDelay: 0,
      test: async (r, changeVisibilityState) => {
        await vi.advanceTimersByTimeAsync(PING_INTERVAL_MS);
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

    const reconnectingSocket = z.socket;
    await tickAFewTimes(vi, RUN_LOOP_INTERVAL_MS);
    await reconnectingSocket;
    await z.triggerConnected();
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
    expect(await onOnlineChangeP).toBe(true);
    expect(z.online).toBe(true);

    await z.close();
  });
});

test(ErrorKind.InvalidConnectionRequest, async () => {
  const r = zeroForTest({});
  await r.triggerError(ErrorKind.InvalidConnectionRequest, 'test');
  expect(r.connectionStatus).toBe(ConnectionStatus.Error);
  await vi.advanceTimersByTimeAsync(0);
  const msg = r.testLogSink.messages.at(-1);
  assert(msg);

  expect(msg[0]).toBe('error');

  const err = msg[2][1];
  assert(isServerError(err));

  expect(err.kind).toEqual(ErrorKind.InvalidConnectionRequest);
});

describe('Invalid Downstream message', () => {
  afterEach(() => vi.resetAllMocks());

  test.each([
    {name: 'no ping', duringPing: false},
    {name: 'during ping', duringPing: true},
  ])('$name', async c => {
    const r = zeroForTest({
      logLevel: 'debug',
    });
    await r.triggerConnected();
    expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

    if (c.duringPing) {
      await waitForUpstreamMessage(r, 'ping', vi);
    }

    await r.triggerPokeStart({
      // @ts-expect-error - invalid field
      pokeIDXX: '1',
      baseCookie: null,
      cookie: '1',
      timestamp: 123456,
    });
    await vi.advanceTimersByTimeAsync(0);

    // if (c.duringPing) { // TODO(0xcadams): do we need to trigger pong here?
    //   await r.triggerPong();
    // }

    // Invalid downstream messages trigger error state
    expect(r.online).toEqual(false);
    expect(r.connectionStatus).toEqual(ConnectionStatus.Error);
  });
});

describe('Downstream message with unknown fields', () => {
  afterEach(() => vi.resetAllMocks());

  test('unknown fields do not result in a parse error', async () => {
    const r = zeroForTest({
      logLevel: 'debug',
    });
    await r.triggerConnected();
    expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

    await r.triggerPokeStart({
      pokeID: '1',
      // @ts-expect-error - invalid field
      pokeIDXX: '1',
      baseCookie: null,
      cookie: '1',
      timestamp: 123456,
    });
    await vi.advanceTimersByTimeAsync(0);

    expect(r.online).eq(true);
    expect(r.connectionStatus).eq(ConnectionStatus.Connected);

    expect(
      r.testLogSink.messages.some(m =>
        m[2].some(
          v =>
            v instanceof Error &&
            v.message.includes('Invalid message received from server'),
        ),
      ),
    ).toBe(false);
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
    });
    const r = zeroForTest({
      logLevel: 'debug',
      schema,
      mutators: {
        issues: {
          foo: (tx: Transaction<typeof schema>, {foo}: {foo: number}) =>
            tx.mutate.issues.insert({id: foo.toString(), value: foo}),
        },
      } as const,
    });
    await r.triggerConnected();
    expect(r.connectionStatus).toBe(ConnectionStatus.Connected);

    const mutation = r.mutate.issues.foo({foo: 1});
    const mutation2 = r.mutate.issues.foo({foo: 2});
    await mutation.client;
    await mutation2.client;

    await r.triggerPoke(null, '1', {
      lastMutationIDChanges: {
        [r.clientID]: 5,
      },
      mutationsPatch: [
        {
          mutation: {
            id: {
              clientID: r.clientID,
              id: 1,
            },
            result: {
              error: 'app',
              details: '...test ',
            },
          },
          op: 'put',
        },
      ],
    });

    await vi.advanceTimersByTimeAsync(100);
    let caught: unknown = undefined;
    try {
      await mutation.server;
    } catch (e) {
      caught = e;
    }

    expect(caught).toMatchInlineSnapshot(`
      {
        "details": "...test ",
        "error": "app",
      }
    `);
    await r.close();
  });
});

test('kvStore option', async () => {
  const spy = vi.spyOn(IDBFactory.prototype, 'open');

  type E = {
    id: string;
    value: number;
    [refCountSymbol]: number;
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
      schema: createSchema({
        tables: [
          table('e')
            .columns({
              id: string(),
              value: number(),
            })
            .primaryKey('id'),
        ],
      }),
    });

    // Use persist as a way to ensure we have read the data out of IDB.
    await r.persist();

    const idIsAView = r.query.e.where('id', '=', 'a').materialize();
    const allDataView = r.query.e.materialize();
    expect(allDataView.data).toEqual(expectedValue);

    await r.mutate.e.insert({id: 'a', value: 1});

    expect(idIsAView.data).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
    // Wait for persist to finish
    await r.persist();

    await r.close();
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
  const r = zeroForTest({
    logLevel: 'debug',
  });

  await r.triggerConnected();

  await r.waitForConnectionStatus(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(0);
  expect(r.online).toBe(true);

  (await r.socket).close();
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  const reconnectAfterFirstClose = r.socket;
  await reconnectAfterFirstClose;

  (await r.socket).close();
  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(0);
  expect(r.online).toBe(false);
  const hasSleeping = r.testLogSink.messages.some(m =>
    m[2].some(v => v === 'Sleeping'),
  );
  expect(hasSleeping).toBe(true);

  await vi.advanceTimersByTimeAsync(RUN_LOOP_INTERVAL_MS);

  const reconnectAfterSleep = r.socket;
  await reconnectAfterSleep;
  await r.triggerConnected();
  await r.waitForConnectionStatus(ConnectionStatus.Connected);
  await vi.advanceTimersByTimeAsync(0);
  expect(r.online).toBe(true);
});

test('Zero close should stop timeout', async () => {
  const r = zeroForTest({
    logLevel: 'debug',
  });

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await r.close();
  await r.waitForConnectionStatus(ConnectionStatus.Closed);
  expect(r.closed).toBe(true);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS);
  expectLogMessages(r).not.contain(connectTimeoutMessage);
});

test('Zero close should stop timeout, close delayed', async () => {
  const r = zeroForTest({
    logLevel: 'debug',
  });

  await r.waitForConnectionStatus(ConnectionStatus.Connecting);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS / 2);
  await r.close();
  await r.waitForConnectionStatus(ConnectionStatus.Closed);
  expect(r.closed).toBe(true);
  await vi.advanceTimersByTimeAsync(CONNECT_TIMEOUT_MS / 2);
  expectLogMessages(r).not.contain(connectTimeoutMessage);
});

test('ensure we get the same query object back', () => {
  const z = zeroForTest({
    schema: createSchema({
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
    }),
  });
  const issueQuery1 = z.query.issue;
  const issueQuery2 = z.query.issue;
  expect(issueQuery1).toBe(issueQuery2);

  const commentQuery1 = z.query.comment;
  const commentQuery2 = z.query.comment;
  expect(commentQuery1).toBe(commentQuery2);

  expect(issueQuery1).not.toBe(commentQuery1);
});

test('the type of collection should be inferred from options with parse', () => {
  const r = zeroForTest({
    schema: createSchema({
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
    }),
  });

  const c = r.query;
  expect(c).not.toBeUndefined();

  const issueQ = r.query.issue;
  const commentQ = r.query.comment;
  expect(issueQ).not.toBeUndefined();
  expect(commentQ).not.toBeUndefined();
});

describe('CRUD', () => {
  const makeZero = () =>
    zeroForTest({
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
      }),
    });

  test('create', async () => {
    const z = makeZero();

    const createIssue = z.mutate.issue.insert;
    const view = z.query.issue.materialize();
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

    const view = z.query.comment.materialize();
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
    const view = z.query.comment.materialize();
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
    const view = z.query.compoundPKTest.materialize();
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
  const makeZero = () =>
    zeroForTest({
      schema: createSchema({
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
      }),
    });

  test('create', async () => {
    const z = makeZero();

    const createIssue: (issue: Issue) => Promise<void> = z.mutate.issue.insert;
    const view = z.query.issue.materialize();
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

    const view = z.query.comment.materialize();
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
    const view = z.query.comment.materialize();
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
  const z = zeroForTest({
    schema: createSchema({
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
    }),
  });
  const issueView = z.query.issue.materialize();
  const commentView = z.query.comment.materialize();

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
  const z = zeroForTest({
    schema: createSchema({
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
    }),
  });
  const issueView = z.query.issue.materialize();

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
    await vi.advanceTimersByTimeAsync(0);
    expect(z.online).toBe(true);
    expect(pushSpy).toBeCalledTimes(1);

    // disconnect and reconnect and check that we sent a push
    await z.triggerClose();
    await z.waitForConnectionStatus(ConnectionStatus.Connecting);
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    await vi.advanceTimersByTimeAsync(0);
    expect(pushSpy).toBeCalledTimes(2);
  }
});

test('onError is called on error', async () => {
  const onErrorSpy = vi.fn();
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
    onError: onErrorSpy,
  });

  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

  await z.triggerError(ErrorKind.MutationRateLimited, 'test');

  expect(onErrorSpy).toBeCalledTimes(1);
  expect(onErrorSpy.mock.calls).toMatchInlineSnapshot(`
    [
      [
        "MutationRateLimited",
        "Mutation rate limited",
        {
          "message": "test",
        },
      ],
    ]
  `);
});

test('onError includes server error reason', async () => {
  const onErrorSpy = vi.fn();
  const z = zeroForTest({onError: onErrorSpy});

  await z.triggerConnected();
  expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

  const serverMessage = 'table missing on remote';
  await z.triggerError(ErrorKind.VersionNotSupported, serverMessage);

  expect(onErrorSpy).toBeCalledTimes(1);
  const [errorMessage, errorObject] = onErrorSpy.mock.calls[0];
  expect(errorMessage).toContain('VersionNotSupported');
  expect(errorMessage).toContain(serverMessage);
  expect(errorObject).toBeInstanceOf(ServerError);
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
