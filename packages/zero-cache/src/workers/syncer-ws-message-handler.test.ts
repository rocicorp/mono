import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import * as MutationType from '../../../zero-protocol/src/mutation-type-enum.ts';
import {CRUD_MUTATION_NAME} from '../../../zero-protocol/src/push.ts';
import type {Auth} from '../auth/auth.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {ConnectParams} from './connect-params.ts';
import {SyncerWsMessageHandler} from './syncer-ws-message-handler.ts';

const lc = createSilentLogContext();

function createMockPusher() {
  return {
    enqueuePush: vi.fn().mockReturnValue({type: 'ok'}),
    initConnection: vi.fn(),
  } as unknown as Pusher;
}

function createMockMutagen() {
  return {
    processMutation: vi.fn().mockResolvedValue(undefined),
  } as unknown as Mutagen;
}

type MockViewSyncer = ViewSyncer & {
  clearAuth: ReturnType<typeof vi.fn>;
  updateAuth: ReturnType<typeof vi.fn>;
};

function createMockViewSyncer(initialAuth: Auth | undefined): MockViewSyncer {
  let auth = initialAuth;

  return {
    get auth() {
      return auth;
    },
    clearAuth: vi.fn(() => {
      auth = undefined;
    }),
    updateAuth: vi.fn((...args: Parameters<ViewSyncer['updateAuth']>) => {
      const [, msg] = args;
      if (!msg[1].auth) {
        if (auth) {
          throw new Error(
            'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
          );
        }
        auth = undefined;
        return;
      }

      auth = {
        type: 'opaque',
        raw: msg[1].auth,
      };
    }),
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
  mutagen: Mutagen,
  pusher: Pusher,
) {
  return new SyncerWsMessageHandler(
    lc,
    createConnectParams(),
    viewSyncer,
    mutagen,
    pusher,
  );
}

describe('SyncerWsMessageHandler auth handling', () => {
  test('uses auth from push message when provided', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'old-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

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
        auth: 'fresh-token',
      },
    ]);

    expect(viewSyncer.updateAuth).toHaveBeenCalledTimes(1);
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      'fresh-token',
      undefined,
      undefined,
    );
  });

  test('falls back to existing auth when push auth is missing', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'connection-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

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
      'test-client',
      expect.any(Object),
      'connection-token',
      undefined,
      undefined,
    );
  });

  test('updateAuth updates auth used by later push', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'old-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

    await handler.handleMessage([
      'updateAuth',
      {
        auth: 'new-token',
      },
    ]);

    expect(viewSyncer.updateAuth).toHaveBeenCalledTimes(1);

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

    expect(viewSyncer.auth?.raw).toBe('new-token');
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      'new-token',
      undefined,
      undefined,
    );
  });

  test('updateAuth with empty auth is rejected for authenticated client group', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'old-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

    await expect(
      handler.handleMessage([
        'updateAuth',
        {
          auth: '',
        },
      ]),
    ).rejects.toThrow(
      'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
    );

    expect(viewSyncer.auth?.raw).toBe('old-token');
  });

  test('invalid push auth stops processing before enqueueing or mutating', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer(undefined);
    const updateAuthSpy = vi
      .spyOn(viewSyncer, 'updateAuth')
      .mockRejectedValue(new Error('bad token'));
    const handler = createHandler(viewSyncer, mutagen, pusher);

    await expect(
      handler.handleMessage([
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
          auth: 'bad-token',
        },
      ]),
    ).rejects.toThrow('bad token');

    expect(updateAuthSpy).toHaveBeenCalledTimes(1);
    expect(pusher.enqueuePush).not.toHaveBeenCalled();
    expect(mutagen.processMutation).not.toHaveBeenCalled();
  });

  test('push auth null does not clear auth used by pusher', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'existing-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

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
      'test-client',
      expect.any(Object),
      'existing-token',
      undefined,
      undefined,
    );
  });

  test('rejects CRUD mutations when auth is opaque', async () => {
    const pusher = createMockPusher();
    const mutagen = createMockMutagen();
    const viewSyncer = createMockViewSyncer({
      type: 'opaque',
      raw: 'opaque-token',
    });
    const handler = createHandler(viewSyncer, mutagen, pusher);

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
