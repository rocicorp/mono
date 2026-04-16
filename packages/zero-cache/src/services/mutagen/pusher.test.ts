import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import type {PushFailedBody} from '../../../../zero-protocol/src/error.ts';
import type {
  APIMutateResponse,
  Mutation,
  PushBody,
} from '../../../../zero-protocol/src/push.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import {
  type ConnectionContext,
  ConnectionContextManagerImpl,
  type ConnectionSelector,
} from '../view-syncer/connection-context-manager.ts';
import {combinePushes, PusherService} from './pusher.ts';

const config = {
  app: {
    id: 'zero',
    publications: [],
  },
  shard: {
    id: 'zero',
    num: 0,
  },
};

const clientID = 'test-cid';
const wsID = 'test-wsid';
const lc = createSilentLogContext();

type TestConnectionOptions = {
  clientID?: string | undefined;
  wsID?: string | undefined;
  auth?: string | undefined;
  httpCookie?: string | undefined;
  origin?: string | undefined;
  userID?: string | undefined;
  userPushURL?: string | undefined;
  userPushHeaders?: Record<string, string> | undefined;
};

const contextManagers = new WeakMap<
  PusherService,
  ConnectionContextManagerImpl
>();

function newPusherService(pushConfig: {
  url: string[];
  apiKey?: string | undefined;
  forwardCookies: boolean;
  allowedClientHeaders?: string[] | undefined;
}): PusherService {
  const contextManager = new ConnectionContextManagerImpl(
    lc,
    undefined,
    undefined,
    {
      url: undefined,
      apiKey: undefined,
      allowedClientHeaders: undefined,
      forwardCookies: false,
    },
    {
      url: pushConfig.url,
      apiKey: pushConfig.apiKey,
      allowedClientHeaders: pushConfig.allowedClientHeaders,
      forwardCookies: pushConfig.forwardCookies,
    },
  );
  const pusher = new PusherService(config, lc, 'cgid', contextManager);
  contextManagers.set(pusher, contextManager);
  return pusher;
}

function getContextManager(
  pusher: PusherService,
): ConnectionContextManagerImpl {
  const contextManager = contextManagers.get(pusher);
  if (!contextManager) {
    throw new Error('Missing context manager for test pusher');
  }
  return contextManager;
}

function registerConnection(
  pusher: PusherService,
  options: TestConnectionOptions = {},
): ConnectionSelector {
  const contextManager = getContextManager(pusher);
  const resolvedClientID = options.clientID ?? clientID;
  const selector = {
    clientID: resolvedClientID,
    wsID: options.wsID ?? `ws-${resolvedClientID}`,
  };

  contextManager.registerConnection(
    selector,
    {
      protocolVersion: 0,
      clientID: resolvedClientID,
      clientGroupID: 'cgid',
      profileID: null,
      baseCookie: null,
      timestamp: Date.now(),
      lmID: 0,
      wsID: selector.wsID,
      debugPerf: false,
      auth: options.auth,
      userID: options.userID,
      initConnectionMsg: undefined,
      httpCookie: options.httpCookie,
      origin: options.origin,
    },
    getAuth(options.auth),
  );
  contextManager.initConnection(selector, {
    desiredQueriesPatch: [],
    userPushURL: options.userPushURL,
    userPushHeaders: options.userPushHeaders,
  });

  return selector;
}

function openConnection(
  pusher: PusherService,
  options: TestConnectionOptions = {},
) {
  const selector = registerConnection(pusher, options);
  return {
    selector,
    stream: pusher.initConnection(selector),
  };
}

const authCache = new Map<string, NonNullable<ConnectionContext['auth']>>();

function getAuth(raw: string | undefined) {
  if (raw === undefined) {
    return undefined;
  }
  let auth = authCache.get(raw);
  if (!auth) {
    auth = {type: 'opaque', raw};
    authCache.set(raw, auth);
  }
  return auth;
}

function makeEntry(
  push: PushBody,
  options: {
    clientID?: string | undefined;
    wsID?: string | undefined;
    revision?: number | undefined;
    auth?: string | undefined;
    httpCookie?: string | undefined;
    origin?: string | undefined;
    userID?: string | undefined;
    userPushURL?: string | undefined;
  } = {},
) {
  const resolvedClientID =
    options.clientID ?? push.mutations[0]?.clientID ?? clientID;
  return {
    push,
    context: {
      state: 'provisional',
      clientID: resolvedClientID,
      wsID: options.wsID ?? `ws-${resolvedClientID}`,
      userID: options.userID,
      auth: getAuth(options.auth),
      profileID: null,
      baseCookie: null,
      protocolVersion: 0,
      revision: options.revision ?? 0,
      revalidateAt: undefined,
      insertionOrder: 0,
      queryContext: {
        url: undefined,
        allowedUrlPatterns: [],
        headerOptions: {
          customHeaders: undefined,
          cookie: options.httpCookie,
          origin: options.origin,
          apiKey: undefined,
          allowedClientHeaders: undefined,
        },
      },
      pushContext: {
        url: options.userPushURL,
        allowedUrlPatterns: [],
        headerOptions: {
          customHeaders: undefined,
          cookie: options.httpCookie,
          origin: options.origin,
          apiKey: undefined,
          allowedClientHeaders: undefined,
        },
      },
    } satisfies ConnectionContext,
  };
}

describe('combine pushes', () => {
  test('empty array', () => {
    const [pushes, terminate] = combinePushes([]);
    expect(pushes).toEqual([]);
    expect(terminate).toBe(false);
  });

  test('stop', () => {
    const [pushes, terminate] = combinePushes([undefined]);
    expect(pushes).toEqual([]);
    expect(terminate).toBe(true);
  });

  test('stop after pushes', () => {
    const [pushes, terminate] = combinePushes([
      makeEntry(makePush(1), {auth: 'a'}),
      makeEntry(makePush(1), {auth: 'a'}),
      undefined,
    ]);
    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(true);
  });

  test('stop in the middle', () => {
    const [pushes, terminate] = combinePushes([
      makeEntry(makePush(1), {auth: 'a'}),
      undefined,
      makeEntry(makePush(1), {auth: 'a'}),
    ]);
    expect(pushes).toHaveLength(1);
    expect(pushes[0].push.mutations).toHaveLength(1);
    expect(pushes[0].push.mutations[0].id).toBe(1);
    expect(terminate).toBe(true);
  });

  test('combines pushes for same clientID', () => {
    const [pushes, terminate] = combinePushes([
      makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
      makeEntry(makePush(2, 'client1'), {clientID: 'client1', auth: 'a'}),
      makeEntry(makePush(1, 'client2'), {clientID: 'client2', auth: 'b'}),
    ]);

    expect(pushes).toHaveLength(2);
    expect(terminate).toBe(false);

    const client1Push = pushes.find(p => p.context.clientID === 'client1');
    expect(client1Push).toBeDefined();
    expect(client1Push?.push.mutations).toHaveLength(3);

    const client2Push = pushes.find(p => p.context.clientID === 'client2');
    expect(client2Push).toBeDefined();
    expect(client2Push?.push.mutations).toHaveLength(1);
  });

  test('throws on jwt mismatch for same client', () => {
    expect(() =>
      combinePushes([
        makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
        makeEntry(makePush(2, 'client1'), {clientID: 'client1', auth: 'b'}),
      ]),
    ).toThrow('auth must be the same for all pushes with the same clientID');
  });

  test('throws on userID mismatch for same client', () => {
    expect(() =>
      combinePushes([
        makeEntry(makePush(1, 'client1'), {
          clientID: 'client1',
          auth: 'a',
          userID: 'user-1',
        }),
        makeEntry(makePush(2, 'client1'), {
          clientID: 'client1',
          auth: 'a',
          userID: 'user-2',
        }),
      ]),
    ).toThrow('userID must be the same for all pushes with the same clientID');
  });

  test('throws on schema version mismatch for same client', () => {
    expect(() =>
      combinePushes([
        makeEntry(
          {
            ...makePush(1, 'client1'),
            schemaVersion: 1,
          },
          {clientID: 'client1', auth: 'a'},
        ),
        makeEntry(
          {
            ...makePush(2, 'client1'),
            schemaVersion: 2,
          },
          {clientID: 'client1', auth: 'a'},
        ),
      ]),
    ).toThrow(
      'schemaVersion must be the same for all pushes with the same clientID',
    );
  });

  test('throws on push version mismatch for same client', () => {
    expect(() =>
      combinePushes([
        makeEntry(
          {
            ...makePush(1, 'client1'),
            pushVersion: 1,
          },
          {clientID: 'client1', auth: 'a'},
        ),
        makeEntry(
          {
            ...makePush(2, 'client1'),
            pushVersion: 2,
          },
          {clientID: 'client1', auth: 'a'},
        ),
      ]),
    ).toThrow(
      'pushVersion must be the same for all pushes with the same clientID',
    );
  });

  test('combines compatible pushes with same schema version and push version', () => {
    const [pushes, terminate] = combinePushes([
      makeEntry(
        {
          ...makePush(1, 'client1'),
          schemaVersion: 1,
          pushVersion: 1,
        },
        {clientID: 'client1', auth: 'a'},
      ),
      makeEntry(
        {
          ...makePush(2, 'client1'),
          schemaVersion: 1,
          pushVersion: 1,
        },
        {clientID: 'client1', auth: 'a'},
      ),
    ]);

    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(false);
    expect(pushes[0].push.mutations).toHaveLength(3);
  });

  test('combines pushes when auth matches by value across snapshots', () => {
    const first = makeEntry(makePush(1, 'client1'), {
      clientID: 'client1',
      wsID: 'ws1',
      revision: 1,
      auth: 'a',
    });
    const second = makeEntry(makePush(1, 'client1'), {
      clientID: 'client1',
      wsID: 'ws1',
      revision: 1,
      auth: 'a',
    });

    second.context.auth = {type: 'opaque', raw: 'a'};

    const [pushes, terminate] = combinePushes([first, second]);

    expect(pushes).toHaveLength(1);
    expect(terminate).toBe(false);
    expect(pushes[0].push.mutations).toHaveLength(2);
  });

  test('handles multiple clients with multiple pushes', () => {
    const [pushes, terminate] = combinePushes([
      makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
      makeEntry(makePush(2, 'client2'), {clientID: 'client2', auth: 'b'}),
      makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
      makeEntry(makePush(3, 'client2'), {clientID: 'client2', auth: 'b'}),
    ]);

    expect(pushes).toHaveLength(2);
    expect(terminate).toBe(false);

    const client1Push = pushes.find(p => p.context.clientID === 'client1');
    expect(client1Push?.push.mutations).toHaveLength(2);

    const client2Push = pushes.find(p => p.context.clientID === 'client2');
    expect(client2Push?.push.mutations).toHaveLength(5);
  });

  test('preserves mutation order within client', () => {
    const [pushes] = combinePushes([
      makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
      makeEntry(makePush(1, 'client2'), {clientID: 'client2', auth: 'b'}),
      makeEntry(makePush(1, 'client1'), {clientID: 'client1', auth: 'a'}),
    ]);

    const client1Push = pushes.find(p => p.context.clientID === 'client1');
    expect(client1Push?.push.mutations[0].id).toBeLessThan(
      client1Push?.push.mutations[1].id || 0,
    );
  });
});
describe('pusher service', () => {
  test('the service can be stopped', async () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      forwardCookies: false,
    });
    let shutDown = false;
    void pusher.run().then(() => {
      shutDown = true;
    });
    await pusher.stop();
    expect(shutDown).toBe(true);
  });

  test('the service sets authorization headers', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userID: 'user-123',
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      'Authorization': 'Bearer jwt',
    });

    fetch.mockReset();
  });

  test('the service sends custom headers from initConnection when allowed', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
      allowedClientHeaders: [
        'x-vercel-automation-bypass-secret',
        'x-custom-header',
      ],
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userPushHeaders: {
        'x-vercel-automation-bypass-secret': 'my-secret',
        'x-custom-header': 'custom-value',
      },
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      'x-vercel-automation-bypass-secret': 'my-secret',
      'x-custom-header': 'custom-value',
      'Authorization': 'Bearer jwt',
    });

    fetch.mockReset();
  });

  test('the service filters custom headers when not in allowedClientHeaders', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
      // allowedClientHeaders not set - secure by default
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userPushHeaders: {
        'x-vercel-automation-bypass-secret': 'my-secret',
        'x-custom-header': 'custom-value',
      },
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toEqual({
      'Content-Type': 'application/json',
      'X-Api-Key': 'api-key',
      'Authorization': 'Bearer jwt',
    });

    fetch.mockReset();
  });

  test('the service sends the app id and schema over the query params', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][0]).toMatchInlineSnapshot(
      '"http://example.com/?schema=zero_0&appID=zero"',
    );

    fetch.mockReset();
  });

  test('the service correctly batches pushes when the API server is delayed', async () => {
    const fetch = (global.fetch = vi.fn());
    const apiServerReturn = resolver();
    fetch.mockImplementation(async (_url: string, _options: RequestInit) => {
      await apiServerReturn.promise;
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });

    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1));
    await Promise.resolve();

    expect(fetch.mock.calls).toHaveLength(1);
    expect(JSON.parse(fetch.mock.calls[0][1].body).mutations).toHaveLength(1);

    pusher.enqueuePush(selector, makePush(1));
    await Promise.resolve();
    pusher.enqueuePush(selector, makePush(1));
    await Promise.resolve();
    pusher.enqueuePush(selector, makePush(1));
    await Promise.resolve();

    expect(fetch.mock.calls).toHaveLength(1);

    apiServerReturn.resolve();
    await new Promise(resolve => {
      setTimeout(resolve, 0);
    });

    expect(JSON.parse(fetch.mock.calls[1][1].body).mutations).toHaveLength(3);
    expect(fetch.mock.calls).toHaveLength(2);
  });

  test('the service does not forward cookies if forwardCookies is false', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      httpCookie: 'my-cookie',
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).not.toHaveProperty('Cookie');
  });

  test('the service forwards cookies if forwardCookies is true', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: true,
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      httpCookie: 'my-cookie',
    });

    pusher.enqueuePush(selector, makePush(1));

    await pusher.stop();

    expect(fetch.mock.calls[0][1]?.headers).toHaveProperty(
      'Cookie',
      'my-cookie',
    );
  });

  test('successful pushes validate the live connection', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: 'user-123',
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userID: 'user-123',
    });

    expect(
      getContextManager(pusher).getConnectionContext(selector),
    ).toMatchObject({
      clientID,
      wsID,
      state: 'provisional',
      revision: 1,
    });

    pusher.enqueuePush(selector, makePush(1, clientID));

    await vi.waitFor(() =>
      expect(
        getContextManager(pusher).getConnectionContext(selector),
      ).toMatchObject({
        clientID,
        wsID,
        userID: 'user-123',
        state: 'validated',
        revision: 1,
      }),
    );
    expect(getContextManager(pusher).getGroupState()).toMatchObject({
      userID: 'user-123',
      validated: true,
      backgroundConnection: selector,
    });

    await pusher.stop();
  });

  test('push auth failure responses remove the live connection', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason: ErrorReason.HTTP,
          status: 403,
          bodyPreview: 'Forbidden',
          message: 'Fetch from API server returned non-OK status 403',
          mutationIDs: [{clientID, id: 1}],
        }),
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userID: 'user-123',
    });

    pusher.enqueuePush(selector, makePush(1, clientID));

    await expect(stream[Symbol.asyncIterator]().next()).rejects.toMatchObject({
      errorBody: expect.objectContaining({
        kind: ErrorKind.PushFailed,
        status: 403,
      }),
    });
    expect(
      getContextManager(pusher).getConnectionContext(selector),
    ).toBeUndefined();

    await pusher.stop();
  });

  test('non-OK 401 push failures remove the live connection', async () => {
    const fetch = (global.fetch = vi.fn());
    const response = {
      ok: false,
      status: 401,
      text: () => Promise.resolve('Unauthorized access'),
      clone() {
        return response;
      },
    };
    fetch.mockResolvedValue(response);

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userID: 'user-123',
    });

    pusher.enqueuePush(selector, makePush(1, clientID));

    await expect(stream[Symbol.asyncIterator]().next()).rejects.toMatchObject({
      errorBody: expect.objectContaining({
        kind: ErrorKind.PushFailed,
        status: 401,
      }),
    });
    expect(
      getContextManager(pusher).getConnectionContext(selector),
    ).toBeUndefined();

    await pusher.stop();
  });

  test('ack mutation responses sends cleanup mutation via HTTP', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: true,
    });
    void pusher.run();

    const requester = registerConnection(pusher, {clientID: 'test-client'});

    await pusher.ackMutationResponses(requester, {
      clientID: 'test-client',
      id: 42,
    });

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, options] = fetch.mock.calls[0];
    expect(url).toMatch(/^http:\/\/example\.com\/?\?schema=zero_0&appID=zero$/);
    expect(options.method).toBe('POST');
    expect(options.headers['X-Api-Key']).toBe('api-key');

    const body = JSON.parse(options.body);
    expect(body.clientGroupID).toBe('cgid');
    expect(body.mutations).toHaveLength(1);
    expect(body.mutations[0].name).toBe('_zero_cleanupResults');
    expect(body.mutations[0].args[0]).toEqual({
      type: 'single',
      clientGroupID: 'cgid',
      clientID: 'test-client',
      upToMutationID: 42,
    });
  });

  test('ack mutation responses handles network errors gracefully', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockRejectedValue(new Error('Network error'));

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: true,
    });
    void pusher.run();

    const requester = registerConnection(pusher, {clientID: 'test-client'});

    await pusher.ackMutationResponses(requester, {
      clientID: 'test-client',
      id: 42,
    });

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
  });

  test('ack mutation responses skips cleanup when no push URL configured', async () => {
    const fetch = (global.fetch = vi.fn());

    const pusher = newPusherService({
      url: [],
      forwardCookies: true,
    });
    void pusher.run();

    const requester = registerConnection(pusher, {clientID: 'test-client'});

    await pusher.ackMutationResponses(requester, {
      clientID: 'test-client',
      id: 42,
    });

    await pusher.stop();

    expect(fetch).not.toHaveBeenCalled();
  });

  test('ack mutation responses uses custom configs from initConnection', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://default.com', 'http://custom.com/push'],
      apiKey: 'api-key',
      forwardCookies: false,
      allowedClientHeaders: ['x-custom-header'],
    });
    void pusher.run();

    const requester = registerConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      httpCookie: 'my-cookie',
      origin: 'https://app.example',
      userID: 'user-123',
      userPushHeaders: {
        'x-custom-header': 'custom-value',
      },
    });

    await pusher.ackMutationResponses(requester, {
      clientID,
      id: 42,
    });

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][1]?.headers).toMatchObject({
      'X-Api-Key': 'api-key',
      'x-custom-header': 'custom-value',
      'Authorization': 'Bearer jwt',
      'Origin': 'https://app.example',
    });
    expect(fetch.mock.calls[0][1]?.headers).not.toHaveProperty('Cookie');
  });

  test('ack mutation responses passes auth headers', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://default.com', 'http://custom.com/push'],
      apiKey: 'api-key',
      forwardCookies: false,
      allowedClientHeaders: ['x-custom-header'],
    });
    void pusher.run();

    const requester = registerConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      httpCookie: 'my-cookie',
      origin: 'https://app.example',
      userPushHeaders: {
        'x-custom-header': 'custom-value',
      },
    });

    await pusher.ackMutationResponses(requester, {
      clientID,
      id: 42,
    });

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][1]?.headers).toMatchObject({
      'X-Api-Key': 'api-key',
      'x-custom-header': 'custom-value',
      'Authorization': 'Bearer jwt',
      'Origin': 'https://app.example',
    });
    expect(fetch.mock.calls[0][1]?.headers).not.toHaveProperty('Cookie');
  });

  test('delete client mutations use the requester connection context for all cleanup requests', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: [
        'http://default.com',
        'http://requester.com/push',
        'http://custom-a.com/push',
        'http://custom-b.com/push',
      ],
      apiKey: 'api-key',
      forwardCookies: false,
      allowedClientHeaders: ['x-custom-header'],
    });
    void pusher.run();

    const requester = registerConnection(pusher, {
      clientID: 'requester',
      wsID: 'ws-requester',
      auth: 'jwt',
      httpCookie: 'my-cookie',
      origin: 'https://app.example',
      userPushURL: 'http://requester.com/push',
      userPushHeaders: {
        'x-custom-header': 'requester',
      },
    });
    registerConnection(pusher, {
      clientID: 'client-a',
      wsID: 'ws-a',
      userPushURL: 'http://custom-a.com/push',
      userPushHeaders: {
        'x-custom-header': 'a',
      },
    });
    registerConnection(pusher, {
      clientID: 'client-b',
      wsID: 'ws-b',
      userPushURL: 'http://custom-b.com/push',
      userPushHeaders: {
        'x-custom-header': 'b',
      },
    });

    await pusher.deleteClientMutations(requester, ['client-a', 'client-b']);

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);

    expect(fetch.mock.calls[0][0]).toBe(
      'http://requester.com/push?schema=zero_0&appID=zero',
    );
    expect(fetch.mock.calls[0][1]?.headers).toMatchObject({
      'X-Api-Key': 'api-key',
      'x-custom-header': 'requester',
      'Authorization': 'Bearer jwt',
      'Origin': 'https://app.example',
    });
    expect(fetch.mock.calls[0][1]?.headers).not.toHaveProperty('Cookie');
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toMatchObject({
      clientGroupID: 'cgid',
      mutations: [
        {
          clientID: 'client-a',
          name: '_zero_cleanupResults',
          args: [
            {
              type: 'bulk',
              clientGroupID: 'cgid',
              clientIDs: ['client-a', 'client-b'],
            },
          ],
        },
      ],
    });
  });

  test('delete client mutations skip cleanup when the requester connection is missing', async () => {
    const fetch = (global.fetch = vi.fn());

    const pusher = newPusherService({
      url: ['http://default.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    await pusher.deleteClientMutations(
      {clientID: 'requester', wsID: 'ws-requester'},
      ['client-a', 'client-b'],
    );

    await pusher.stop();

    expect(fetch).not.toHaveBeenCalled();
  });

  test('delete client mutations forwards cookies when configured', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: 'user-123',
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://default.com'],
      apiKey: 'api-key',
      forwardCookies: true,
    });
    void pusher.run();

    const requester = registerConnection(pusher, {
      clientID: 'requester',
      wsID: 'ws-requester',
      auth: 'jwt',
      httpCookie: 'my-cookie',
      origin: 'https://app.example',
      userID: 'user-123',
    });

    await pusher.deleteClientMutations(requester, ['client-a']);

    await pusher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][1]?.headers).toMatchObject({
      Authorization: 'Bearer jwt',
      Cookie: 'my-cookie',
      Origin: 'https://app.example',
    });
  });
});

describe('initConnection', () => {
  test('initConnection returns a stream', () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const selector = registerConnection(pusher, {clientID, wsID});
    const stream = pusher.initConnection(selector);

    expect(stream[Symbol.asyncIterator]).toBeTypeOf('function');
  });

  test('initConnection throws if it was already called for the same clientID and wsID', () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const selector = registerConnection(pusher, {
      clientID: 'c1',
      wsID: 'ws1',
    });
    pusher.initConnection(selector);
    expect(() => pusher.initConnection(selector)).toThrow(
      'Connection was already initialized',
    );
  });

  test('initConnection destroys prior stream for same client when wsID changes', async () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {stream: stream1} = openConnection(pusher, {
      clientID: 'c1',
      wsID: 'ws1',
    });
    openConnection(pusher, {
      clientID: 'c1',
      wsID: 'ws2',
    });
    const iterator = stream1[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });

  test('uses client custom URL when userParams.url is provided', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://default.com', 'http://custom.com/push'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userPushURL: 'http://custom.com/push',
    });
    pusher.enqueuePush(selector, makePush(1));

    await new Promise(resolve => setTimeout(resolve, 0));

    expect(fetch.mock.calls[0][0]).toEqual(
      'http://custom.com/push?schema=zero_0&appID=zero',
    );

    await pusher.stop();
  });

  test('falls back to default URL when userParams.url is not provided', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: ['http://default.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1));

    await new Promise(resolve => setTimeout(resolve, 0));

    expect(fetch.mock.calls[0][0]).toEqual(
      'http://default.com/?schema=zero_0&appID=zero',
    );

    await pusher.stop();
  });

  test('routes custom push URL and headers per client connection', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          kind: 'MutateResponse',
          userID: null,
          mutations: [],
        }),
    });

    const pusher = newPusherService({
      url: [
        'http://default.com',
        'http://custom-a.com/push',
        'http://custom-b.com/push',
      ],
      apiKey: 'api-key',
      forwardCookies: false,
      allowedClientHeaders: ['x-custom-header'],
    });
    void pusher.run();

    const {selector: selectorA} = openConnection(pusher, {
      clientID: 'client-a',
      wsID: 'ws-a',
      auth: 'jwt-a',
      userPushURL: 'http://custom-a.com/push',
      userPushHeaders: {
        'x-custom-header': 'a',
      },
    });
    const {selector: selectorB} = openConnection(pusher, {
      clientID: 'client-b',
      wsID: 'ws-b',
      auth: 'jwt-b',
      userPushURL: 'http://custom-b.com/push',
      userPushHeaders: {
        'x-custom-header': 'b',
      },
    });

    pusher.enqueuePush(selectorA, makePush(1, 'client-a'));
    pusher.enqueuePush(selectorB, makePush(1, 'client-b'));

    await pusher.stop();

    expect(fetch.mock.calls[0][0]).toBe(
      'http://custom-a.com/push?schema=zero_0&appID=zero',
    );
    expect(fetch.mock.calls[0][1]?.headers).toMatchObject({
      'x-custom-header': 'a',
      'Authorization': 'Bearer jwt-a',
    });
    expect(fetch.mock.calls[1][0]).toBe(
      'http://custom-b.com/push?schema=zero_0&appID=zero',
    );
    expect(fetch.mock.calls[1][1]?.headers).toMatchObject({
      'x-custom-header': 'b',
      'Authorization': 'Bearer jwt-b',
    });
  });
});

describe('pusher streaming', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  test('returns ok for subsequent pushes from same client', () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1));
    const result = pusher.enqueuePush(selector, makePush(1));
    expect(result.type).toBe('ok');
  });

  test('cleanup removes client subscription', () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector, stream: stream1} = openConnection(pusher, {
      clientID,
      wsID: 'ws1',
      auth: 'jwt',
    });

    pusher.enqueuePush(selector, makePush(1, clientID));

    stream1.cancel();

    const replacement = registerConnection(pusher, {
      clientID,
      wsID: 'ws1',
    });
    expect(() => pusher.initConnection(replacement)).not.toThrow();
  });

  test('new websocket for same client creates new downstream', async () => {
    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {stream: stream1} = openConnection(pusher, {
      clientID,
      wsID: 'ws1',
    });
    openConnection(pusher, {
      clientID,
      wsID: 'ws2',
    });

    const iterator = stream1[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });
});

describe('pusher errors', () => {
  async function expectPushErrorResponse(
    errorResponse: APIMutateResponse,
    expectedError: PushFailedBody,
  ) {
    const fetch = (global.fetch = vi.fn());
    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(errorResponse),
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID: 'ws1',
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1, clientID));

    const iterator = stream[Symbol.asyncIterator]();
    const failure = iterator.next();
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: expectedError,
      logLevel: 'warn',
    });
  }

  test('emits error message on ooo mutations', async () => {
    await expectPushErrorResponse(
      {
        kind: 'MutateResponse',
        userID: null,
        mutations: [
          {id: {clientID, id: 3}, result: {}},
          {id: {clientID, id: 1}, result: {error: 'oooMutation'}},
        ],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.OutOfOrderMutation,
        message: 'mutation was out of order',
        details: undefined,
        mutationIDs: [
          {clientID: 'test-cid', id: 3},
          {clientID: 'test-cid', id: 1},
        ],
      },
    );
  });

  test('emits error message on unsupported schema version or push version', async () => {
    await expectPushErrorResponse(
      {
        error: 'unsupportedSchemaVersion',
        mutationIDs: [{clientID, id: 1}],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.Internal,
        message: 'Unsupported schema version',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
    );
  });

  test('emits error message with PushFailed error on 401 response', async () => {
    await expectPushErrorResponse(
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 401,
        bodyPreview: 'Unauthorized access',
        message: 'Fetch from API server returned non-OK status 401',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 401,
        bodyPreview: 'Unauthorized access',
        message: 'Fetch from API server returned non-OK status 401',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
    );
  });

  test('emits error message with legacy http error format', async () => {
    await expectPushErrorResponse(
      {
        error: 'http',
        status: 503,
        details: 'Service Unavailable',
        mutationIDs: [{clientID, id: 1}],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 503,
        bodyPreview: 'Service Unavailable',
        message: 'Fetch from API server returned non-OK status 503',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
    );
  });

  test('emits error message with legacy unsupportedPushVersion format', async () => {
    await expectPushErrorResponse(
      {
        error: 'unsupportedPushVersion',
        mutationIDs: [{clientID, id: 1}],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.UnsupportedPushVersion,
        message: 'Unsupported push version',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
    );
  });

  test('emits error message with legacy zeroPusher error format', async () => {
    await expectPushErrorResponse(
      {
        error: 'zeroPusher',
        details: 'Zero pusher internal error',
        mutationIDs: [{clientID, id: 1}],
      },
      {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.Internal,
        message: 'Zero pusher internal error',
        mutationIDs: [{clientID: 'test-cid', id: 1}],
      },
    );
  });

  test('handles non-Error object thrown in catch block', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockRejectedValue('string error');

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1, clientID));

    const iterator = stream[Symbol.asyncIterator]();
    const failure = iterator.next();
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: 'Fetch from API server threw error: string error',
        mutationIDs: [
          {
            clientID: 'test-cid',
            id: 1,
          },
        ],
      },
      logLevel: 'warn',
    });
  });

  test('handles ooo mutation with subsequent mutations', async () => {
    const fetch = (global.fetch = vi.fn());
    const oooResponse: APIMutateResponse = {
      kind: 'MutateResponse',
      userID: null,
      mutations: [
        {
          id: {clientID, id: 1},
          result: {error: 'oooMutation', details: 'out of order'},
        },
        {
          id: {clientID, id: 2},
          result: {},
        },
        {
          id: {clientID, id: 3},
          result: {},
        },
      ],
    };

    fetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(oooResponse),
    });

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID: 'ws1',
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(3, clientID));

    const iterator = stream[Symbol.asyncIterator]();
    const failure = iterator.next();
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.Server,
        reason: ErrorReason.OutOfOrderMutation,
        message: 'mutation was out of order',
        details: 'out of order',
        mutationIDs: [
          {
            clientID: 'test-cid',
            id: 1,
          },
          {
            clientID: 'test-cid',
            id: 2,
          },
          {
            clientID: 'test-cid',
            id: 3,
          },
        ],
      },
      logLevel: 'warn',
    });
  });

  test('streams error response to affected clients', async () => {
    const fetch = (global.fetch = vi.fn());
    const mockResponse = {
      ok: false,
      status: 500,
      text: () => Promise.resolve('Internal Server Error'),
      clone() {
        return mockResponse;
      },
    };
    fetch.mockResolvedValue(mockResponse);

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector: selector1, stream: stream1} = openConnection(pusher, {
      clientID: 'client1',
      wsID: 'ws1',
      auth: 'jwt',
    });
    const {selector: selector2, stream: stream2} = openConnection(pusher, {
      clientID: 'client2',
      wsID: 'ws2',
      auth: 'jwt',
    });

    pusher.enqueuePush(selector1, makePush(1, 'client1'));
    pusher.enqueuePush(selector2, makePush(1, 'client2'));

    const iterator1 = stream1[Symbol.asyncIterator]();
    const iterator2 = stream2[Symbol.asyncIterator]();
    const failure1 = iterator1.next();
    const failure2 = iterator2.next();

    await expect(failure1).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure1).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 500,
        bodyPreview: 'Internal Server Error',
        message: 'Fetch from API server returned non-OK status 500',
        mutationIDs: [
          {
            clientID: 'client1',
            id: 1,
          },
        ],
      },
      logLevel: 'warn',
    });

    await expect(failure2).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure2).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 500,
        bodyPreview: 'Internal Server Error',
        message: 'Fetch from API server returned non-OK status 500',
        mutationIDs: [
          {
            clientID: 'client2',
            id: 2,
          },
        ],
      },
      logLevel: 'warn',
    });
  });

  test('handles network errors', async () => {
    const fetch = (global.fetch = vi.fn());
    fetch.mockRejectedValue(new Error('Network error'));

    const pusher = newPusherService({
      url: ['http://example.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();

    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
    });
    pusher.enqueuePush(selector, makePush(1, clientID));

    const iterator = stream[Symbol.asyncIterator]();
    const failure = iterator.next();
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: 'Fetch from API server threw error: Network error',
        mutationIDs: [
          {
            clientID: 'test-cid',
            id: 1,
          },
        ],
      },
      logLevel: 'warn',
    });
  });

  test('rejects disallowed custom URL', async () => {
    const pusher = newPusherService({
      url: ['http://allowed.com'],
      apiKey: 'api-key',
      forwardCookies: false,
    });
    void pusher.run();
    const {selector, stream} = openConnection(pusher, {
      clientID,
      wsID,
      auth: 'jwt',
      userPushURL: 'http://malicious.com/endpoint',
    });

    pusher.enqueuePush(selector, makePush(1, clientID));

    const iterator = stream[Symbol.asyncIterator]();
    const failure = iterator.next();
    await expect(failure).rejects.toBeInstanceOf(ProtocolErrorWithLevel);
    await expect(failure).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: expect.stringContaining(
          'URL "http://malicious.com/endpoint" is not allowed by the ZERO_MUTATE_URL configuration',
        ),
        mutationIDs: [
          {
            clientID: 'test-cid',
            id: 1,
          },
        ],
      },
      logLevel: 'warn',
    });

    await pusher.stop();
  });
});

let timestamp = 0;
let id = 0;

beforeEach(() => {
  timestamp = 0;
  id = 0;
});

function makePush(numMutations: number, clientID?: string): PushBody {
  return {
    clientGroupID: 'cgid',
    mutations: Array.from({length: numMutations}, () => makeMutation(clientID)),
    pushVersion: 1,
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: ++timestamp,
  };
}

function makeMutation(clientID?: string): Mutation {
  return {
    type: 'custom',
    args: [],
    clientID: clientID ?? 'cid',
    id: ++id,
    name: 'n',
    timestamp: ++timestamp,
  } as const;
}
