import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import * as MutationType from '../../../zero-protocol/src/mutation-type-enum.ts';
import {CRUD_MUTATION_NAME} from '../../../zero-protocol/src/push.ts';
import type {Auth, ValidateLegacyJWT} from '../auth/auth.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import {
  type ConnectionContextManager,
  ConnectionContextManagerImpl,
} from '../services/view-syncer/connection-context-manager.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {ConnectParams} from './connect-params.ts';
import {SyncerWsMessageHandler} from './syncer-ws-message-handler.ts';

const lc = createSilentLogContext();

function createMockPusher() {
  return {
    enqueuePush: vi.fn().mockReturnValue({type: 'ok'}),
    initConnection: vi.fn(),
    ackMutationResponses: vi.fn().mockResolvedValue(undefined),
    deleteClientMutations: vi.fn().mockResolvedValue(undefined),
  };
}

type MockPusher = ReturnType<typeof createMockPusher>;

function createMockMutagen() {
  return {
    processMutation: vi.fn().mockResolvedValue(undefined),
  };
}

type MockMutagen = ReturnType<typeof createMockMutagen>;

type MockViewSyncer = ViewSyncer & {
  deleteClients: ReturnType<typeof vi.fn>;
  updateAuth: ReturnType<typeof vi.fn>;
};

function createMockViewSyncer(
  contextManager: ConnectionContextManager,
): MockViewSyncer {
  return {
    contextManager,
    updateAuth: vi.fn().mockResolvedValue(undefined),
    changeDesiredQueries: vi.fn().mockResolvedValue(undefined),
    deleteClients: vi.fn().mockResolvedValue([]),
    initConnection: vi.fn(),
    inspect: vi.fn().mockResolvedValue(undefined),
  } as unknown as MockViewSyncer;
}

function createConnectParams(
  overrides: Partial<ConnectParams> = {},
): ConnectParams {
  return {
    clientGroupID: 'test-client-group',
    clientID: 'test-client',
    profileID: 'test-profile',
    wsID: 'test-ws',
    baseCookie: null,
    protocolVersion: 48,
    timestamp: Date.now(),
    lmID: 0,
    debugPerf: false,
    auth: undefined,
    userID: 'test-user',
    initConnectionMsg: undefined,
    httpCookie: undefined,
    origin: undefined,
    ...overrides,
  };
}

function createHandler(
  viewSyncer: ViewSyncer,
  mutagen: MockMutagen,
  pusher: MockPusher,
  initialAuth: Auth | undefined,
  connectParamsOverrides: Partial<ConnectParams> = {},
  contextManager = new ConnectionContextManagerImpl(lc),
) {
  const connectParams = createConnectParams(connectParamsOverrides);
  contextManager.registerConnection(
    {clientID: connectParams.clientID, wsID: connectParams.wsID},
    connectParams,
    initialAuth,
  );
  return new SyncerWsMessageHandler(
    lc,
    connectParams,
    contextManager,
    viewSyncer,
    mutagen as unknown as Mutagen,
    pusher as unknown as Pusher,
  );
}

describe('SyncerWsMessageHandler auth handling', () => {
  test('ignores push auth and uses the connection auth snapshot', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'connection-token',
      },
      {},
      contextManager,
    );

    await handler.handleMessage([
      'push',
      {
        clientGroupID: 'test-client-group',
        mutations: [
          {
            type: 'custom',
            id: 1,
            clientID: 'test-client',
            name: 'testMutation',
            args: [],
            timestamp: Date.now(),
          },
        ],
        pushVersion: 1,
        schemaVersion: 1,
        timestamp: Date.now(),
        requestID: 'req-1',
        auth: 'ignored-token',
      },
    ]);

    expect(viewSyncer.updateAuth).not.toHaveBeenCalled();
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      expect.any(Object),
    );
  });

  test('updateAuth updates auth used by later pushes', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'old-token',
      },
      {},
      contextManager,
    );

    await handler.handleMessage(['updateAuth', {auth: 'new-token'}]);

    expect(viewSyncer.updateAuth).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      ['updateAuth', {auth: 'new-token'}],
      true,
    );

    await handler.handleMessage([
      'push',
      {
        clientGroupID: 'test-client-group',
        mutations: [
          {
            type: 'custom',
            id: 1,
            clientID: 'test-client',
            name: 'testMutation',
            args: [],
            timestamp: Date.now(),
          },
        ],
        pushVersion: 1,
        schemaVersion: 1,
        timestamp: Date.now(),
        requestID: 'req-1',
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      expect.any(Object),
    );

    await handler.handleMessage(['updateAuth', {auth: 'new-token'}]);
    expect(viewSyncer.updateAuth).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      ['updateAuth', {auth: 'new-token'}],
      // the second call should not have an auth revision
      false,
    );
  });

  test('ackMutationResponses forwards connection auth context to cleanup', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'connection-token',
      },
      {
        httpCookie: 'my-cookie',
        origin: 'https://app.example',
      },
      contextManager,
    );

    await handler.handleMessage([
      'ackMutationResponses',
      {
        clientID: 'test-client',
        id: 42,
      },
    ]);

    expect(pusher.ackMutationResponses).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      {
        clientID: 'test-client',
        id: 42,
      },
    );
  });

  test('deleteClients forwards connection auth context to cleanup', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    viewSyncer.deleteClients.mockResolvedValue(['client-a']);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'connection-token',
      },
      {
        httpCookie: 'my-cookie',
        origin: 'https://app.example',
      },
      contextManager,
    );

    await handler.handleMessage(['deleteClients', {clientIDs: ['client-a']}]);

    expect(pusher.deleteClientMutations).toHaveBeenCalledWith(
      {
        clientID: 'test-client',
        wsID: 'test-ws',
      },
      ['client-a'],
    );
  });

  test('rejects clearing auth on an authenticated connection', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'old-token',
      },
      {},
      contextManager,
    );

    await expect(
      handler.handleMessage(['updateAuth', {auth: ''}]),
    ).rejects.toMatchObject({
      errorBody: expect.objectContaining({kind: 'Unauthorized'}),
    });
    expect(viewSyncer.updateAuth).not.toHaveBeenCalled();
  });

  test('surfaces validator failures before updating the connection auth', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const validateLegacyJWT: ValidateLegacyJWT = () =>
      Promise.reject(new Error('bad token'));
    const contextManager = new ConnectionContextManagerImpl(
      lc,
      undefined,
      undefined,
      undefined,
      undefined,
      validateLegacyJWT,
    );
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      undefined,
      {},
      contextManager,
    );

    await expect(
      handler.handleMessage(['updateAuth', {auth: 'jwt-token'}]),
    ).rejects.toMatchObject({
      errorBody: expect.objectContaining({kind: 'AuthInvalidated'}),
    });
    expect(viewSyncer.updateAuth).not.toHaveBeenCalled();
  });

  test('uses handler-local JWT auth for CRUD mutations', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'jwt',
        raw: 'jwt-token',
        decoded: {sub: 'test-user', iat: 1},
      },
      {},
      contextManager,
    );

    await handler.handleMessage([
      'push',
      {
        clientGroupID: 'test-client-group',
        mutations: [
          {
            type: MutationType.CRUD,
            id: 1,
            clientID: 'test-client',
            name: CRUD_MUTATION_NAME,
            args: [{ops: []}],
            timestamp: Date.now(),
          },
        ],
        pushVersion: 1,
        schemaVersion: 1,
        timestamp: Date.now(),
        requestID: 'req-1',
      },
    ]);

    expect(mutagen.processMutation).toHaveBeenCalledWith(
      expect.objectContaining({type: MutationType.CRUD}),
      {sub: 'test-user', iat: 1},
      true,
    );
  });

  test('rejects CRUD mutations when auth is opaque', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const contextManager = new ConnectionContextManagerImpl(lc);
    const viewSyncer = createMockViewSyncer(contextManager);
    const handler = createHandler(
      viewSyncer,
      mutagen,
      pusher,
      {
        type: 'opaque',
        raw: 'opaque-token',
      },
      {},
      contextManager,
    );

    await expect(
      handler.handleMessage([
        'push',
        {
          clientGroupID: 'test-client-group',
          mutations: [
            {
              type: MutationType.CRUD,
              id: 1,
              clientID: 'test-client',
              name: CRUD_MUTATION_NAME,
              args: [{ops: []}],
              timestamp: Date.now(),
            },
          ],
          pushVersion: 1,
          schemaVersion: 1,
          timestamp: Date.now(),
          requestID: 'req-1',
        },
      ]),
    ).rejects.toThrow('Only JWT auth is supported for CRUD mutations');
  });
});
