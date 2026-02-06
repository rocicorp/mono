import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import * as MutationType from '../../../zero-protocol/src/mutation-type-enum.ts';
import {CRUD_MUTATION_NAME} from '../../../zero-protocol/src/push.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import type {
  SyncContext,
  ViewSyncer,
} from '../services/view-syncer/view-syncer.ts';
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

function createMockViewSyncer() {
  return {} as unknown as ViewSyncer;
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
    protocolVersion: 30,
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

describe('SyncerWsMessageHandler push auth handling', () => {
  let pusher: ReturnType<typeof createMockPusher>;
  let mutagen: ReturnType<typeof createMockMutagen>;
  let viewSyncer: ReturnType<typeof createMockViewSyncer>;

  beforeEach(() => {
    pusher = createMockPusher();
    mutagen = createMockMutagen();
    viewSyncer = createMockViewSyncer();
  });

  test('uses auth from push message when provided', async () => {
    const connectionToken = 'old-connection-token';
    const freshPushToken = 'fresh-push-token';

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {type: 'opaque', raw: connectionToken},
      viewSyncer,
      mutagen,
      pusher,
      undefined,
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
        auth: freshPushToken,
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      freshPushToken,
      undefined,
      undefined,
    );
  });

  test('falls back to connection token when push auth is not provided', async () => {
    const connectionToken = 'connection-token';

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {type: 'opaque', raw: connectionToken},
      viewSyncer,
      mutagen,
      pusher,
      undefined,
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
      'test-client',
      expect.any(Object),
      connectionToken,
      undefined,
      undefined,
    );
  });

  test('handles undefined connection token with push auth', async () => {
    const freshPushToken = 'fresh-push-token';

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      undefined, // no token data at connection time
      viewSyncer,
      mutagen,
      pusher,
      undefined,
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
        auth: freshPushToken,
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      freshPushToken,
      undefined,
      undefined,
    );
  });

  test('handles both undefined - no auth forwarded', async () => {
    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      undefined, // no token data
      viewSyncer,
      mutagen,
      pusher,
      undefined,
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
        // no auth field
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      undefined,
      undefined,
      undefined,
    );
  });

  test('changeDesiredQueries refresh updates opaque auth fallback token', async () => {
    const connectionToken = 'opaque-connection-token';
    const freshToken = 'opaque-refresh-token';
    const viewSyncerWithChange = {
      changeDesiredQueries: vi.fn().mockResolvedValue(undefined),
    } as unknown as ViewSyncer;

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {type: 'opaque', raw: connectionToken},
      viewSyncerWithChange,
      mutagen,
      pusher,
      undefined,
    );

    await handler.handleMessage([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [],
        auth: freshToken,
      },
    ]);

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

    expect(viewSyncerWithChange.changeDesiredQueries).toHaveBeenCalledTimes(1);
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      freshToken,
      undefined,
      undefined,
    );
  });

  test('changeDesiredQueries sets opaque auth when unauthenticated', async () => {
    const freshToken = 'opaque-refresh-token';
    const viewSyncerWithChange = {
      changeDesiredQueries: vi.fn().mockResolvedValue(undefined),
    } as unknown as ViewSyncer;

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      undefined,
      viewSyncerWithChange,
      mutagen,
      pusher,
      undefined,
    );

    await handler.handleMessage([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [],
        auth: freshToken,
      },
    ]);

    const [ctx] = (
      viewSyncerWithChange.changeDesiredQueries as unknown as {
        mock: {calls: [SyncContext][]};
      }
    ).mock.calls[0];
    expect(ctx.auth).toEqual({type: 'opaque', raw: freshToken});
  });

  test('rejects CRUD mutations when auth is opaque', async () => {
    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {type: 'opaque', raw: 'opaque-token'},
      viewSyncer,
      mutagen,
      pusher,
      undefined,
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
    ).rejects.toThrow('Only JWT auth is supported for mutations');
  });

  test('changeDesiredQueries auth refresh updates push fallback token', async () => {
    const connectionToken = 'old-connection-token';
    const freshToken = 'fresh-change-token';
    const viewSyncerWithChange = {
      changeDesiredQueries: vi.fn().mockResolvedValue(undefined),
    } as unknown as ViewSyncer;

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {type: 'jwt', raw: connectionToken, decoded: {sub: 'user-1', iat: 1}},
      viewSyncerWithChange,
      mutagen,
      pusher,
      auth =>
        Promise.resolve({
          type: 'jwt',
          raw: auth,
          decoded: {sub: 'user-1', iat: 2},
        }),
    );

    await handler.handleMessage([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [],
        auth: freshToken,
      },
    ]);

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

    expect(viewSyncerWithChange.changeDesiredQueries).toHaveBeenCalledTimes(1);
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      freshToken,
      undefined,
      undefined,
    );
  });
});
