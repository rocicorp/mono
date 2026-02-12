/* oxlint-disable @typescript-eslint/no-explicit-any */
import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';

let receiver: WebSocketReceiver<any>;
vi.mock('../types/websocket-handoff.ts', () => ({
  installWebSocketReceiver: vi
    .fn()
    .mockImplementation((_lc, _server, receive, _sender) => {
      receiver = receive;
    }),
}));

// Mock the anonymous telemetry functions
vi.mock('../server/anonymous-otel-start.ts', () => ({
  recordConnectionSuccess: vi.fn(),
  recordConnectionAttempted: vi.fn(),
  setActiveClientGroupsGetter: vi.fn(),
}));

import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {type WebSocket} from 'ws';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {AuthSessionImpl, type ValidateLegacyJWT} from '../auth/auth.ts';
import * as jwt from '../auth/jwt.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {
  recordConnectionAttempted,
  recordConnectionSuccess,
} from '../server/anonymous-otel-start.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import type {ActivityBasedService} from '../services/service.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {WebSocketReceiver} from '../types/websocket-handoff.ts';
import {Syncer} from './syncer.ts';

const lc = createSilentLogContext();
const tempDir = await fs.mkdtemp(
  path.join(os.tmpdir(), 'zero-cache-syncer-test'),
);
const tempFile = path.join(tempDir, `syncer.test.db`);
const sqlite = new Database(lc, tempFile);

sqlite.exec(`
CREATE TABLE "test-app.permissions" (permissions, hash);
INSERT INTO "test-app.permissions" (permissions, hash) VALUES (null, 'test-hash');
`);

// ------------------------------
// Test helpers
// ------------------------------

const TEST_PARENT: any = {
  onMessageType: () => {},
  send: () => {},
};

function makeFactories(
  lc: LogContext,
  mutagensOut: MutagenService[],
  pushersOut: PusherService[],
) {
  const storageDb = new Database(lc, ':memory:');
  storageDb.prepare(CREATE_STORAGE_TABLE).run();
  const writeAuthzStorage = new DatabaseStorage(storageDb);

  return {
    viewSyncerFactory: (
      id: string,
      _sub: unknown,
      _drainCoordinator: unknown,
      validateLegacyJWT: ValidateLegacyJWT | undefined,
    ) =>
      (() => {
        const stopped = resolver<void>();
        const authSession = new AuthSessionImpl(lc, id, validateLegacyJWT);
        return {
          id,
          get auth() {
            return authSession.auth;
          },
          clearAuth: vi.fn(() => authSession.clear()),
          initAuthSession: vi.fn(
            (userID: string, wireAuth: string | undefined) =>
              authSession.update(userID, wireAuth),
          ),
          initConnection: vi.fn(),
          changeDesiredQueries: vi.fn(),
          deleteClients: vi.fn(),
          inspect: vi.fn(),
          updateAuth: vi.fn(),
          keepalive: () => true,
          stop() {
            stopped.resolve();
            return stopped.promise;
          },
          run() {
            return stopped.promise;
          },
        } as ViewSyncer & ActivityBasedService;
      })(),
    mutagenFactory: (id: string) => {
      const ret = new MutagenService(
        lc,
        {appID: 'test-app', shardNum: 0},
        id,
        {} as any,
        {
          replica: {file: tempFile},
          perUserMutationLimit: {},
        } as ZeroConfig,
        writeAuthzStorage,
      );
      mutagensOut.push(ret);
      return ret;
    },
    pusherFactory: (id: string) => {
      const ret = new PusherService(
        {} as ZeroConfig,
        {url: ['http://example.com'], forwardCookies: false},
        lc,
        id,
      );
      pushersOut.push(ret);
      return ret;
    },
  } as const;
}

function setupSyncer(lc: LogContext, config: ZeroConfig) {
  const mutagens: MutagenService[] = [];
  const pushers: PusherService[] = [];
  const {viewSyncerFactory, mutagenFactory, pusherFactory} = makeFactories(
    lc,
    mutagens,
    pushers,
  );
  const syncer = new Syncer(
    lc,
    config,
    viewSyncerFactory,
    mutagenFactory,
    pusherFactory,
    TEST_PARENT,
  );
  return {syncer, mutagens, pushers};
}

const baseParams = {
  clientGroupID: '1',
  userID: 'anon',
  wsID: '1',
  protocolVersion: 30,
};

function makeParams(clientID: number, params: any = {}) {
  return {
    ...baseParams,
    clientID: `${clientID}`,
    ...params,
  };
}

async function openConnection(clientID: number, params: any = {}) {
  const ws = new MockWebSocket() as unknown as WebSocket;
  await receiver(ws, makeParams(clientID, params), {} as any);
  return ws;
}

describe('cleanup', () => {
  let syncer: Syncer;
  let mutagens: MutagenService[];
  let pushers: PusherService[];
  beforeEach(() => {
    const env = setupSyncer(lc, {
      auth: {
        secret: 'test-secret',
      },
    } as ZeroConfig);
    syncer = env.syncer;
    mutagens = env.mutagens;
    pushers = env.pushers;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  const newConnection = (clientID: number) => openConnection(clientID);

  test('bumps ref count when getting same service over and over', async () => {
    const connections: WebSocket[] = [];
    function check() {
      expect(mutagens.length).toBe(1);
      expect(pushers.length).toBe(1);
      expect(mutagens[0].hasRefs()).toBe(true);
      expect(pushers[0].hasRefs()).toBe(true);
    }

    for (let i = 0; i < 10; i++) {
      connections.push(await newConnection(i));
      check();
    }

    // now close all the connections
    for (const ws of connections) {
      ws.close();
    }
    expect(mutagens[0].hasRefs()).toBe(false);
    expect(pushers[0].hasRefs()).toBe(false);
    expect(mutagens.length).toBe(1);
    expect(pushers.length).toBe(1);
  });

  test('decrements ref count on connection close, returns new instance on next connection if ref count is 0', async () => {
    function check(iteration: number) {
      expect(mutagens.length).toBe(iteration + 1);
      expect(pushers.length).toBe(iteration + 1);
      // the current service has no refs since the connection was closed immediately.
      expect(mutagens[iteration].hasRefs()).toBe(false);
      expect(pushers[iteration].hasRefs()).toBe(false);
    }

    for (let i = 0; i < 10; i++) {
      const ws = await newConnection(i);
      ws.close();
      check(i);
    }
  });

  test('handles same client coming back on different connections', async () => {
    function check(iteration: number) {
      expect(mutagens.length).toBe(iteration + 1);
      expect(pushers.length).toBe(iteration + 1);

      expect(mutagens[iteration].hasRefs()).toBe(true);
      expect(pushers[iteration].hasRefs()).toBe(true);

      // prior service has no refs since it only had one connection and that
      // connection was closed and swapped to a new one.
      if (iteration > 0) {
        expect(mutagens[iteration - 1].hasRefs()).toBe(false);
        expect(pushers[iteration - 1].hasRefs()).toBe(false);
      }
    }

    for (let i = 0; i < 10; i++) {
      await newConnection(1);
      check(i);
    }
  });
});

describe('connection telemetry', () => {
  let syncer: Syncer;

  beforeEach(() => {
    vi.clearAllMocks();
    const env = setupSyncer(lc, {
      auth: {
        secret: 'test-secret',
      },
    } as ZeroConfig);
    syncer = env.syncer;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  const newConnection = (clientID: number, params: any = {}) =>
    openConnection(clientID, params);

  test('should record connection success for valid protocol version', async () => {
    // Create a connection with valid protocol version
    await newConnection(1);

    // Should record connection success
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(1);
  });

  test('should record multiple successful connections', async () => {
    // Create multiple connections with valid protocol version
    await newConnection(1);
    await newConnection(2);
    await newConnection(3);

    // Should record multiple connection successes
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(3);
  });

  test('should record connection attempted for each connection', async () => {
    // Create connections - both should record attempts
    // supported protocol version
    await newConnection(1);
    // unsupported protocol version
    await newConnection(2, {protocolVersion: 21});

    // Should record connection attempts
    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(2);
  });
});

describe('jwt auth validation', () => {
  let syncer: Syncer;
  let mutagens: MutagenService[];
  let pushers: PusherService[];

  beforeEach(() => {
    vi.clearAllMocks();
    const env = setupSyncer(lc, {
      auth: {
        // Intentionally set multiple options to trigger the validation error
        jwk: '{}',
        secret: 'super-secret',
      },
    } as ZeroConfig);
    syncer = env.syncer;
    mutagens = env.mutagens;
    pushers = env.pushers;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  test('fails when too many JWT options are set', async () => {
    const ws = new MockWebSocket() as unknown as WebSocket;
    await expect(
      receiver(
        ws,
        {
          clientGroupID: '1',
          clientID: `1`,
          userID: 'anon',
          wsID: '1',
          protocolVersion: 21,
          auth: 'dummy-token',
        },
        {} as any,
      ),
    ).rejects.toThrow(/Exactly one of jwk, secret, or jwksUrl must be set/);

    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(1);
    expect(vi.mocked(recordConnectionSuccess)).not.toHaveBeenCalled();

    // No services should be instantiated when auth validation fails early
    expect(mutagens.length).toBe(0);
    expect(pushers.length).toBe(0);
  });
});

describe('jwt auth without options', () => {
  let syncer: Syncer;
  let logSink: TestLogSink;
  let mutagens: MutagenService[];
  let pushers: PusherService[];
  let verifySpy: any;

  beforeEach(() => {
    vi.clearAllMocks();
    verifySpy = vi.spyOn(jwt, 'verifyToken');
    mutagens = [];
    pushers = [];
    logSink = new TestLogSink();
    const lc = new LogContext('debug', {}, logSink);
    const env = setupSyncer(lc, {
      // No auth options set; should not verify token
      auth: {},
      // set custom mutations & queries to avoid token verification
      mutate: {url: ['http://mutate.example.com']},
      query: {url: ['http://queries.example.com']},
    } as ZeroConfig);
    syncer = env.syncer;
    mutagens = env.mutagens;
    pushers = env.pushers;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  test('succeeds when using mutations & queries and skips verification', async () => {
    const ws = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      ws,
      {
        clientGroupID: '1',
        clientID: '1',
        userID: 'anon',
        wsID: '1',
        protocolVersion: 30,
        auth: 'dummy-token',
      },
      {} as any,
    );

    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(1);
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(1);
    expect(verifySpy).not.toHaveBeenCalled();

    // Connection stays open and sends 'connected'
    expect((ws as any).readyState).toBe(MockWebSocket.OPEN);
    const messages = (ws as any).messages as string[];
    expect(messages.length).toBeGreaterThan(0);
    const first = JSON.parse(messages[0]);
    expect(first[0]).toBe('connected');

    // Services should be instantiated for successful connection
    expect(mutagens.length).toBe(1);
    expect(pushers.length).toBe(1);
  });
});

describe('jwt auth missing options and missing endpoints', () => {
  let syncer: Syncer;
  let mutagens: MutagenService[];
  let pushers: PusherService[];

  beforeEach(() => {
    vi.clearAllMocks();
    const env = setupSyncer(lc, {
      // No auth and no mutate/queries set; should assert on receiving auth
      auth: {},
    } as ZeroConfig);
    syncer = env.syncer;
    mutagens = env.mutagens;
    pushers = env.pushers;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  test('succeeds when no auth token is provided', async () => {
    const ws = await openConnection(1);

    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(1);
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(1);

    expect((ws as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );
    expect(mutagens.length).toBe(1);
    expect(pushers.length).toBe(1);
  });

  test('fails when no JWT options and no custom endpoints are set', async () => {
    const ws = new MockWebSocket() as unknown as WebSocket;
    await expect(
      receiver(
        ws,
        {
          clientGroupID: '1',
          clientID: `1`,
          userID: 'anon',
          wsID: '1',
          auth: 'dummy-token',
        },
        {} as any,
      ),
    ).rejects.toThrow(/Exactly one of jwk, secret, or jwksUrl must be set/);

    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(1);
    expect(vi.mocked(recordConnectionSuccess)).not.toHaveBeenCalled();

    expect(mutagens.length).toBe(0);
    expect(pushers.length).toBe(0);
  });
});

describe('connection hijacking prevention', () => {
  let syncer: Syncer;
  let verifySpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.clearAllMocks();
    verifySpy = vi.spyOn(jwt, 'verifyToken');
    verifySpy.mockReset();
    const env = setupSyncer(lc, {
      auth: {
        secret: 'test-secret',
      },
    } as ZeroConfig);
    syncer = env.syncer;
  });

  afterEach(async () => {
    await syncer.stop();
  });

  test('invalid auth does not close existing connection', async () => {
    // First, establish a legitimate unauthenticated connection
    const existingWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      existingWs,
      {
        clientGroupID: '1',
        clientID: 'target-client',
        userID: 'legit-user',
        wsID: 'ws-1',
        protocolVersion: 30,
        // No auth token - existing unauthenticated connection
      },
      {} as any,
    );
    expect((existingWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );

    // Now, attacker tries to connect with same clientID but invalid token
    verifySpy.mockRejectedValueOnce(new Error('Invalid token'));
    const attackerWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      attackerWs,
      {
        clientGroupID: '1',
        clientID: 'target-client', // Same clientID as existing connection
        userID: 'attacker',
        wsID: 'ws-2',
        protocolVersion: 30,
        auth: 'invalid-token',
      },
      {} as any,
    );

    // The existing connection should NOT have been closed
    expect((existingWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );

    // The attacker's connection should be closed due to invalid auth
    expect((attackerWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.CLOSED,
    );
  });

  test('valid auth can replace existing connection', async () => {
    // First, establish an existing connection
    const existingWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      existingWs,
      {
        clientGroupID: '1',
        clientID: 'client-1',
        userID: 'user-1',
        wsID: 'ws-1',
        protocolVersion: 30,
      },
      {} as any,
    );
    expect((existingWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );

    // Same user reconnects with valid auth
    verifySpy.mockResolvedValueOnce({sub: 'user-1'});
    const newWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      newWs,
      {
        clientGroupID: '1',
        clientID: 'client-1', // Same clientID
        userID: 'user-1',
        wsID: 'ws-2',
        protocolVersion: 30,
        auth: 'valid-token',
      },
      {} as any,
    );

    // The existing connection should be closed (replaced)
    expect((existingWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.CLOSED,
    );

    // The new connection should be open
    expect((newWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );
  });

  test('rejects missing auth on reconnect for authenticated client group', async () => {
    verifySpy.mockResolvedValueOnce({sub: 'user-1'});
    const firstWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      firstWs,
      {
        clientGroupID: '1',
        clientID: 'client-1',
        userID: 'user-1',
        wsID: 'ws-1',
        protocolVersion: 30,
        auth: 'valid-token',
      },
      {} as any,
    );
    expect((firstWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );

    const reconnectWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      reconnectWs,
      {
        clientGroupID: '1',
        clientID: 'client-2',
        userID: 'user-1',
        wsID: 'ws-2',
        protocolVersion: 30,
      },
      {} as any,
    );

    expect((reconnectWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.CLOSED,
    );
    expect((firstWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.OPEN,
    );
  });

  test('retains user binding until view syncer expires', async () => {
    verifySpy.mockResolvedValueOnce({sub: 'user-1'});
    const firstWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      firstWs,
      {
        clientGroupID: '1',
        clientID: 'client-1',
        userID: 'user-1',
        wsID: 'ws-1',
        protocolVersion: 30,
        auth: 'valid-token',
      },
      {} as any,
    );
    firstWs.close();

    const reconnectWs = new MockWebSocket() as unknown as WebSocket;
    await receiver(
      reconnectWs,
      {
        clientGroupID: '1',
        clientID: 'client-2',
        userID: 'user-2',
        wsID: 'ws-2',
        protocolVersion: 30,
      },
      {} as any,
    );

    expect((reconnectWs as unknown as MockWebSocket).readyState).toBe(
      MockWebSocket.CLOSED,
    );
  });
});

afterAll(async () => {
  try {
    await fs.rm(tempDir, {recursive: true, force: true});
  } catch (e) {
    // oxlint-disable-next-line no-console
    console.error(`Failed to clean up temp directory ${tempDir}:`, e);
  }

  sqlite.close();
});

class MockWebSocket {
  static readonly OPEN = 1;
  static readonly CLOSED = 3;

  #listeners: Map<string, ((event: any) => void)[]> = new Map();
  addEventListener(type: string, fn: (event: any) => void) {
    if (!this.#listeners.has(type)) {
      this.#listeners.set(type, []);
    }
    this.#listeners.get(type)!.push(fn);
  }
  removeEventListener(type: string, fn: (event: any) => void) {
    const listeners = this.#listeners.get(type);
    if (listeners) {
      this.#listeners.set(
        type,
        listeners.filter(listener => listener !== fn),
      );
    }
  }

  readyState = 1; // OPEN
  close() {
    this.readyState = 3; // CLOSED
    const listeners = this.#listeners.get('close') || [];
    for (const listener of listeners) {
      listener({code: 1000, reason: 'Test close', wasClean: true});
    }
  }
  // recorded outbound messages (stringified JSON)
  messages: string[] = [];
  send(data: string) {
    this.messages.push(data);
  }
  on(event: string, fn: (event: any) => void) {
    this.addEventListener(event, fn);
  }
  once(event: string, fn: (event: any) => void) {
    this.addEventListener(event, fn);
    const listeners = this.#listeners.get(event) || [];
    this.#listeners.set(
      event,
      listeners.filter(l => l !== fn),
    );
  }
}
