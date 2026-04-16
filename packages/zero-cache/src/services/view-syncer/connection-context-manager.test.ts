import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Auth} from '../../auth/auth.ts';
import type {ConnectParams} from '../../workers/connect-params.ts';
import {
  type ConnectionContextManager,
  type ConnectionSelector,
  ConnectionContextManagerImpl,
} from './connection-context-manager.ts';

const lc = createSilentLogContext();

function selector(clientID: string, wsID: string): ConnectionSelector {
  return {clientID, wsID};
}

function makeConnectParams(
  clientID: string,
  wsID: string,
  userID = `user-${clientID}`,
): ConnectParams {
  return {
    httpCookie: `cookie-${wsID}`,
    origin: `origin-${wsID}`,
    userID,
    profileID: null,
    baseCookie: null,
    protocolVersion: 0,
    clientID,
    wsID,
    clientGroupID: `group-${clientID}`,
    timestamp: 1234,
    lmID: 0,
    debugPerf: false,
    auth: `token-${wsID}`,
    initConnectionMsg: undefined,
  };
}

function register(
  manager: ConnectionContextManager,
  clientID: string,
  wsID: string,
  userID = `user-${clientID}`,
  auth: Auth | undefined = {type: 'opaque', raw: `token-${wsID}`},
) {
  return manager.registerConnection(
    selector(clientID, wsID),
    makeConnectParams(clientID, wsID, userID),
    auth,
  );
}

function registerLoggedOut(
  manager: ConnectionContextManager,
  clientID: string,
  wsID: string,
) {
  return manager.registerConnection(
    selector(clientID, wsID),
    {
      ...makeConnectParams(clientID, wsID),
      userID: undefined,
      auth: undefined,
    },
    undefined,
  );
}

function initConnection(
  manager: ConnectionContextManager,
  clientID: string,
  wsID: string,
  body: Partial<ConnectInitBody> = {},
) {
  return manager.initConnection(selector(clientID, wsID), {
    desiredQueriesPatch: [],
    ...body,
  });
}

type ConnectInitBody = {
  desiredQueriesPatch: [];
  userQueryURL?: string | undefined;
  userQueryHeaders?: Record<string, string> | undefined;
  userPushURL?: string | undefined;
  userPushHeaders?: Record<string, string> | undefined;
};

function validate(
  manager: ConnectionContextManager,
  clientID: string,
  wsID: string,
  validatedUserID: string | null | undefined,
  revision = manager.mustGetConnectionContext(selector(clientID, wsID))
    .revision,
) {
  return manager.validateConnection(
    selector(clientID, wsID),
    revision,
    validatedUserID,
  );
}

describe('ConnectionContextManager', () => {
  test('registers provisional connections, applies init metadata, and replaces prior sockets for a client', () => {
    const manager = new ConnectionContextManagerImpl(lc);

    expect(register(manager, 'c1', 'ws1')).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      revision: 0,
      state: 'provisional',
      userID: 'user-c1',
      auth: {type: 'opaque', raw: 'token-ws1'},
      revalidateAt: undefined,
      queryContext: {
        url: undefined,
        headerOptions: {
          token: 'token-ws1',
          cookie: undefined,
          origin: 'origin-ws1',
          userID: 'user-c1',
        },
      },
      pushContext: {
        url: undefined,
        headerOptions: {
          token: 'token-ws1',
          cookie: undefined,
          origin: 'origin-ws1',
          userID: 'user-c1',
        },
      },
    });

    expect(
      initConnection(manager, 'c1', 'ws1', {
        userQueryURL: 'https://api.example/query',
        userQueryHeaders: {foo: 'bar'},
        userPushURL: 'https://api.example/push',
        userPushHeaders: {baz: 'qux'},
      }),
    ).toMatchObject({
      revision: 1,
      state: 'provisional',
      queryContext: {
        url: 'https://api.example/query',
        headerOptions: {
          customHeaders: {foo: 'bar'},
        },
      },
      pushContext: {
        url: 'https://api.example/push',
        headerOptions: {
          customHeaders: {baz: 'qux'},
        },
      },
    });

    expect(register(manager, 'c1', 'ws2')).toMatchObject({
      clientID: 'c1',
      wsID: 'ws2',
      state: 'provisional',
      auth: {type: 'opaque', raw: 'token-ws2'},
      queryContext: {url: undefined},
      pushContext: {url: undefined},
    });
    expect(manager.closeConnection(selector('c1', 'ws1'))).toBeUndefined();
    expect(manager.getConnectionContext(selector('c1', 'ws2'))).toMatchObject({
      clientID: 'c1',
      wsID: 'ws2',
      state: 'provisional',
    });
  });

  test('binds the first validated userID from client', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');

    expect(validate(manager, 'c1', 'ws1', undefined)).toEqual({
      connection: expect.objectContaining({
        clientID: 'c1',
        wsID: 'ws1',
        state: 'validated',
        userID: 'user-1',
        revalidateAt: undefined,
      }),
      group: {
        userID: 'user-1',
        backgroundConnection: {clientID: 'c1', wsID: 'ws1'},
        maintenanceNotBeforeAt: undefined,
        retransformAt: undefined,
        validated: true,
      },
    });
  });

  test('pins a logged-out client group to an undefined userID', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    registerLoggedOut(manager, 'c1', 'ws1');
    register(manager, 'c2', 'ws2', 'user-2');

    expect(validate(manager, 'c1', 'ws1', null)).toEqual({
      connection: expect.objectContaining({
        clientID: 'c1',
        wsID: 'ws1',
        state: 'validated',
        userID: undefined,
      }),
      group: {
        userID: undefined,
        validated: true,
        backgroundConnection: {clientID: 'c1', wsID: 'ws1'},
        maintenanceNotBeforeAt: undefined,
        retransformAt: undefined,
      },
    });

    expect(() =>
      validate(manager, 'c2', 'ws2', undefined),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Client groups are pinned to a single userID. Connection userID does not match existing client group userID.]`,
    );
  });

  test('rejects mismatched validated userIDs and keeps the connection provisional', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');

    expect(() =>
      validate(manager, 'c1', 'ws1', 'user-2'),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Connection userID does not match validated server userID.]`,
    );

    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      state: 'provisional',
      userID: 'user-1',
    });
    expect(manager.getGroupState()).toEqual({
      userID: undefined,
      validated: false,
      backgroundConnection: undefined,
      maintenanceNotBeforeAt: undefined,
      retransformAt: undefined,
    });
  });

  test('rejects mismatched userIDs and keeps the connection provisional', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-2');
    validate(manager, 'c1', 'ws1', undefined);

    expect(() =>
      validate(manager, 'c2', 'ws2', undefined),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Client groups are pinned to a single userID. Connection userID does not match existing client group userID.]`,
    );

    expect(manager.getConnectionContext(selector('c2', 'ws2'))).toMatchObject({
      clientID: 'c2',
      wsID: 'ws2',
      state: 'provisional',
    });
    expect(manager.getGroupState()).toMatchObject({
      userID: 'user-1',
      validated: true,
    });
  });

  test('keeps the group userID pinned after all live connections are removed', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);

    manager.closeConnection(selector('c1', 'ws1'));
    register(manager, 'c2', 'ws2', 'user-2');

    expect(manager.getGroupState()).toMatchObject({
      userID: 'user-1',
      backgroundConnection: undefined,
      validated: true,
    });
    expect(() =>
      validate(manager, 'c2', 'ws2', undefined),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Client groups are pinned to a single userID. Connection userID does not match existing client group userID.]`,
    );
  });

  test('allows multiple validated connections when stored userIDs match', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-1');
    register(manager, 'c3', 'ws3', 'user-1');

    validate(manager, 'c1', 'ws1', undefined);
    validate(manager, 'c2', 'ws2', undefined);
    validate(manager, 'c3', 'ws3', undefined);

    expect(manager.getGroupState()).toMatchObject({
      userID: 'user-1',
      validated: true,
    });
    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      state: 'validated',
    });
    expect(manager.getConnectionContext(selector('c2', 'ws2'))).toMatchObject({
      state: 'validated',
    });
    expect(manager.getConnectionContext(selector('c3', 'ws3'))).toMatchObject({
      state: 'validated',
    });
  });

  test('does not demote a validated connection when auth is unchanged by value', async () => {
    const manager = new ConnectionContextManagerImpl(
      lc,
      5,
      10,
      undefined,
      undefined,
      undefined,
      () => 1_000,
    );
    register(manager, 'c1', 'ws1', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);
    const previousAuth = manager.mustGetConnectionContext(
      selector('c1', 'ws1'),
    ).auth;

    await expect(
      manager.updateAuth(selector('c1', 'ws1'), {auth: 'token-ws1'}),
    ).resolves.toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      state: 'validated',
      userID: 'user-1',
      revalidateAt: 6_000,
    });
    expect(manager.getBackgroundConnectionContext()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });
    expect(manager.getGroupState().retransformAt).toBe(11_000);
    expect(manager.mustGetConnectionContext(selector('c1', 'ws1')).auth).toBe(
      previousAuth,
    );
  });

  test('demotes only the connection whose auth materially changes', async () => {
    const manager = new ConnectionContextManagerImpl(
      lc,
      5,
      10,
      undefined,
      undefined,
      undefined,
      () => 1_000,
    );
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);
    validate(manager, 'c2', 'ws2', undefined);

    await expect(
      manager.updateAuth(selector('c2', 'ws2'), {auth: 'token-ws2-new'}),
    ).resolves.toMatchObject({
      clientID: 'c2',
      wsID: 'ws2',
      state: 'provisional',
      revalidateAt: undefined,
      revision: 1,
    });
    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      state: 'validated',
      userID: 'user-1',
      revalidateAt: 6_000,
    });
    expect(manager.getBackgroundConnectionContext()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });
    expect(manager.getGroupState().retransformAt).toBe(11_000);
  });

  test('keeps the current validated background sticky until it disappears, then promotes the newest validated connection', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-1');
    register(manager, 'c3', 'ws3', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);
    validate(manager, 'c2', 'ws2', undefined);
    validate(manager, 'c3', 'ws3', undefined);

    expect(manager.getBackgroundConnectionContext()).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
    });

    manager.closeConnection(selector('c1', 'ws1'));

    expect(manager.getBackgroundConnectionContext()).toMatchObject({
      clientID: 'c3',
      wsID: 'ws3',
    });
  });

  test('treats stale websocket operations as no-ops or invalid requests', async () => {
    const manager = new ConnectionContextManagerImpl(lc);
    register(manager, 'c1', 'ws1');
    register(manager, 'c1', 'ws2');

    expect(
      manager.validateConnection(selector('c1', 'ws1'), 0, undefined),
    ).toBeUndefined();
    expect(manager.closeConnection(selector('c1', 'ws1'))).toBeUndefined();
    expect(() =>
      manager.mustGetConnectionContext(selector('c1', 'ws1')),
    ).toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Connection auth state was not available for this websocket.]`,
    );
    await expect(
      manager.updateAuth(selector('c1', 'ws1'), {auth: ''}),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[ProtocolError: Connection auth state was not available for this websocket.]`,
    );
    expect(manager.getConnectionContext(selector('c1', 'ws2'))).toMatchObject({
      clientID: 'c1',
      wsID: 'ws2',
      state: 'provisional',
    });
  });

  test('stores normalized fetch context on the connection state', () => {
    const manager = new ConnectionContextManagerImpl(
      lc,
      undefined,
      undefined,
      {
        url: ['https://default.example/query'],
        apiKey: 'query-api-key',
        allowedClientHeaders: ['x-query-header'],
        forwardCookies: true,
      },
      {
        url: ['https://default.example/push'],
        apiKey: 'push-api-key',
        allowedClientHeaders: ['x-push-header'],
        forwardCookies: false,
      },
    );
    register(manager, 'c1', 'ws1', 'user-1', {
      type: 'opaque',
      raw: 'token-1',
    });

    expect(
      initConnection(manager, 'c1', 'ws1', {
        userQueryURL: 'https://user.example/query',
        userQueryHeaders: {'x-query-header': 'query-value'},
        userPushURL: 'https://user.example/push',
        userPushHeaders: {'x-push-header': 'push-value'},
      }),
    ).toEqual(
      expect.objectContaining({
        clientID: 'c1',
        wsID: 'ws1',
        revision: 1,
        queryContext: expect.objectContaining({
          url: 'https://user.example/query',
          allowedUrlPatterns: expect.arrayContaining([
            new URLPattern('https://user.example/query'),
          ]),
          headerOptions: expect.objectContaining({
            apiKey: 'query-api-key',
            customHeaders: {'x-query-header': 'query-value'},
            allowedClientHeaders: ['x-query-header'],
            token: 'token-1',
            cookie: 'cookie-ws1',
            origin: 'origin-ws1',
            userID: 'user-1',
          }),
        }),
        pushContext: expect.objectContaining({
          url: 'https://user.example/push',
          allowedUrlPatterns: expect.arrayContaining([
            new URLPattern('https://user.example/push'),
          ]),
          headerOptions: expect.objectContaining({
            apiKey: 'push-api-key',
            customHeaders: {'x-push-header': 'push-value'},
            allowedClientHeaders: ['x-push-header'],
            token: 'token-1',
            cookie: undefined,
            origin: 'origin-ws1',
            userID: 'user-1',
          }),
        }),
      }),
    );
  });

  test('ignores stale revision-scoped validation and failure updates', () => {
    const manager = new ConnectionContextManagerImpl(lc);
    const registered = register(manager, 'c1', 'ws1');
    const revised = initConnection(manager, 'c1', 'ws1', {
      userQueryURL: 'https://api.example/query',
      userQueryHeaders: {foo: 'bar'},
    });

    expect(
      manager.validateConnection(
        selector('c1', 'ws1'),
        registered.revision,
        undefined,
      ),
    ).toBeUndefined();
    expect(
      manager.failConnection(selector('c1', 'ws1'), registered.revision),
    ).toBeUndefined();
    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      clientID: 'c1',
      wsID: 'ws1',
      revision: revised.revision,
      state: 'provisional',
    });
  });

  test('plans maintenance with per-connection revalidation and shared retransform deadlines', () => {
    let now = 1_000;
    const manager = new ConnectionContextManagerImpl(
      lc,
      5,
      2,
      undefined,
      undefined,
      undefined,
      () => now,
    );
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-1');
    register(manager, 'c3', 'ws3', 'user-1');
    validate(manager, 'c2', 'ws2', undefined);
    validate(manager, 'c1', 'ws1', undefined);

    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 3_000,
    });

    now = 3_000;
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: true,
      earliestDeadlineAt: 3_000,
    });

    const background = manager.mustGetBackgroundConnectionContext();
    manager.markBackgroundRetransformSuccess(
      selector(background.clientID, background.wsID),
      background.revision,
    );
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 5_000,
    });

    now = 5_000;
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: true,
      earliestDeadlineAt: 5_000,
    });

    const backgroundAgain = manager.mustGetBackgroundConnectionContext();
    manager.markBackgroundRetransformSuccess(
      selector(backgroundAgain.clientID, backgroundAgain.wsID),
      backgroundAgain.revision,
    );
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 6_000,
    });

    now = 6_000;
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [
        expect.objectContaining({clientID: 'c1', wsID: 'ws1'}),
        expect.objectContaining({clientID: 'c2', wsID: 'ws2'}),
      ],
      dueRetransform: false,
      earliestDeadlineAt: 6_000,
    });

    validate(manager, 'c2', 'ws2', undefined);
    validate(manager, 'c1', 'ws1', undefined);

    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 7_000,
    });
  });

  test('revalidation does not reset shared retransform cadence, but retransform success does', () => {
    let now = 1_000;
    const manager = new ConnectionContextManagerImpl(
      lc,
      5,
      2,
      undefined,
      undefined,
      undefined,
      () => now,
    );
    register(manager, 'c1', 'ws1', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);

    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      revalidateAt: 6_000,
    });
    expect(manager.getGroupState().retransformAt).toBe(3_000);

    now = 1_500;
    validate(manager, 'c1', 'ws1', undefined);

    expect(manager.getConnectionContext(selector('c1', 'ws1'))).toMatchObject({
      revalidateAt: 6_500,
    });
    expect(manager.getGroupState().retransformAt).toBe(3_000);

    now = 2_000;
    const background = manager.mustGetBackgroundConnectionContext();
    manager.markBackgroundRetransformSuccess(
      selector(background.clientID, background.wsID),
      background.revision,
    );

    expect(manager.getGroupState().retransformAt).toBe(4_000);
  });

  test('defers all scheduled maintenance until the group not-before deadline', () => {
    let now = 1_000;
    const manager = new ConnectionContextManagerImpl(
      lc,
      5,
      2,
      undefined,
      undefined,
      undefined,
      () => now,
    );
    register(manager, 'c1', 'ws1', 'user-1');
    register(manager, 'c2', 'ws2', 'user-1');
    validate(manager, 'c1', 'ws1', undefined);
    validate(manager, 'c2', 'ws2', undefined);

    now = 3_000;
    manager.deferMaintenance('retransform');

    expect(manager.getGroupState().maintenanceNotBeforeAt).toBe(5_000);
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 5_000,
    });

    now = 6_000;
    manager.deferMaintenance('revalidate');

    expect(manager.getGroupState().maintenanceNotBeforeAt).toBe(11_000);
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [],
      dueRetransform: false,
      earliestDeadlineAt: 11_000,
    });

    now = 11_000;
    expect(manager.planMaintenance()).toEqual({
      dueRevalidations: [
        expect.objectContaining({clientID: 'c1', wsID: 'ws1'}),
        expect.objectContaining({clientID: 'c2', wsID: 'ws2'}),
      ],
      dueRetransform: true,
      earliestDeadlineAt: 3_000,
    });
  });
});
