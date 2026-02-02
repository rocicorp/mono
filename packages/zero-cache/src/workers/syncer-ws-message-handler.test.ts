import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
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
      {raw: connectionToken, decoded: {sub: 'user-1'}},
      viewSyncer,
      mutagen,
      pusher,
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
        auth: freshPushToken, // fresh auth sent with push
      },
    ]);

    // should use the fresh token from push, not the stale connection token
    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      freshPushToken, // this is the key assertion
      undefined,
      undefined,
    );
  });

  test('falls back to connection token when push auth is not provided', async () => {
    const connectionToken = 'connection-token';

    const handler = new SyncerWsMessageHandler(
      lc,
      createConnectParams(),
      {raw: connectionToken, decoded: {sub: 'user-1'}},
      viewSyncer,
      mutagen,
      pusher,
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
        // no auth field - should fall back to connection token
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      connectionToken, // falls back to connection token
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
        // no auth field either
      },
    ]);

    expect(pusher.enqueuePush).toHaveBeenCalledWith(
      'test-client',
      expect.any(Object),
      undefined, // both undefined = undefined
      undefined,
      undefined,
    );
  });
});
