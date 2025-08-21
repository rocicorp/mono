/* eslint-disable @typescript-eslint/no-explicit-any */
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
  addClientGroup: vi.fn(),
  removeClientGroup: vi.fn(),
  recordConnectionSuccess: vi.fn(),
  recordConnectionAttempted: vi.fn(),
}));
const mockDB = (() => {}) as unknown as PostgresDB;

import {Syncer} from './syncer.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  recordConnectionSuccess,
  recordConnectionAttempted,
} from '../server/anonymous-otel-start.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import type {ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {ActivityBasedService} from '../services/service.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import type {WebSocketReceiver} from '../types/websocket-handoff.ts';
import {type WebSocket} from 'ws';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs/promises';
import {Database} from '../../../zqlite/src/db.ts';
import type {PostgresDB} from '../types/pg.ts';

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

describe('cleanup', () => {
  let syncer: Syncer;
  let mutagens: MutagenService[];
  let pushers: PusherService[];
  beforeEach(() => {
    mutagens = [];
    pushers = [];
    syncer = new Syncer(
      lc,
      {} as ZeroConfig,
      id =>
        ({
          id,
          keepalive: () => true,
          stop() {
            return Promise.resolve();
          },
          run() {
            return Promise.resolve();
          },
        }) as ViewSyncer & ActivityBasedService,
      id => {
        const ret = new MutagenService(
          lc,
          {
            appID: 'test-app',
            shardNum: 0,
          },
          id,
          {} as any,
          {
            replica: {
              file: tempFile,
            },
            perUserMutationLimit: {},
          } as ZeroConfig,
        );
        mutagens.push(ret);
        return ret;
      },
      id => {
        const ret = new PusherService(
          mockDB,
          {} as ZeroConfig,
          {
            url: ['http://example.com'],
            forwardCookies: false,
          },
          lc,
          id,
        );
        pushers.push(ret);
        return ret;
      },
      {
        onMessageType: () => {},
        send: () => {},
      } as any,
    );
  });

  afterEach(async () => {
    await syncer.stop();
  });

  function newConnection(clientID: number) {
    const ws = new MockWebSocket() as unknown as WebSocket;
    receiver(
      ws,
      {
        clientGroupID: '1',
        clientID: `${clientID}`,
        userID: 'anon',
        wsID: '1',
        protocolVersion: 21, // Valid protocol version (current PROTOCOL_VERSION)
      },
      {} as any,
    );
    return ws;
  }

  test('bumps ref count when getting same service over and over', () => {
    const connections: WebSocket[] = [];
    function check() {
      expect(mutagens.length).toBe(1);
      expect(pushers.length).toBe(1);
      expect(mutagens[0].hasRefs()).toBe(true);
      expect(pushers[0].hasRefs()).toBe(true);
    }

    for (let i = 0; i < 10; i++) {
      connections.push(newConnection(i));
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

  test('decrements ref count on connection close, returns new instance on next connection if ref count is 0', () => {
    function check(iteration: number) {
      expect(mutagens.length).toBe(iteration + 1);
      expect(pushers.length).toBe(iteration + 1);
      // the current service has no refs since the connection was closed immediately.
      expect(mutagens[iteration].hasRefs()).toBe(false);
      expect(pushers[iteration].hasRefs()).toBe(false);
    }

    for (let i = 0; i < 10; i++) {
      const ws = newConnection(i);
      ws.close();
      check(i);
    }
  });

  test('handles same client coming back on different connections', () => {
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
      newConnection(1);
      check(i);
    }
  });
});

describe('connection telemetry', () => {
  let syncer: Syncer;
  let mutagens: MutagenService[];
  let pushers: PusherService[];

  beforeEach(() => {
    vi.clearAllMocks();
    mutagens = [];
    pushers = [];
    syncer = new Syncer(
      lc,
      {} as ZeroConfig,
      id =>
        ({
          id,
          keepalive: () => true,
          stop() {
            return Promise.resolve();
          },
          run() {
            return Promise.resolve();
          },
        }) as ViewSyncer & ActivityBasedService,
      id => {
        const ret = new MutagenService(
          lc,
          {
            appID: 'test-app',
            shardNum: 0,
          },
          id,
          {} as any,
          {
            replica: {
              file: tempFile,
            },
            perUserMutationLimit: {},
          } as ZeroConfig,
        );
        mutagens.push(ret);
        return ret;
      },
      id => {
        const ret = new PusherService(
          mockDB,
          {} as ZeroConfig,
          {
            url: ['http://example.com'],
            forwardCookies: false,
          },
          lc,
          id,
        );
        pushers.push(ret);
        return ret;
      },
      {
        onMessageType: () => {},
        send: () => {},
      } as any,
    );
  });

  afterEach(async () => {
    await syncer.stop();
  });

  function newConnection(clientID: number, params: any = {}) {
    const ws = new MockWebSocket() as unknown as WebSocket;
    receiver(
      ws,
      {
        clientGroupID: '1',
        clientID: `${clientID}`,
        userID: 'anon',
        wsID: '1',
        protocolVersion: 21, // Valid protocol version (current PROTOCOL_VERSION)
        ...params,
      },
      {} as any,
    );
    return ws;
  }

  test('should record connection success for valid protocol version', () => {
    // Create a connection with valid protocol version
    newConnection(1, {protocolVersion: 21});

    // Should record connection success
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(1);
  });

  test('should record multiple successful connections', () => {
    // Create multiple connections with valid protocol version
    newConnection(1, {protocolVersion: 21});
    newConnection(2, {protocolVersion: 21});
    newConnection(3, {protocolVersion: 21});

    // Should record multiple connection successes
    expect(vi.mocked(recordConnectionSuccess)).toHaveBeenCalledTimes(3);
  });

  test('should record connection attempted for each connection', () => {
    // Create connections - both should record attempts
    newConnection(1, {protocolVersion: 21});
    newConnection(2, {protocolVersion: 21});

    // Should record connection attempts
    expect(vi.mocked(recordConnectionAttempted)).toHaveBeenCalledTimes(2);
  });
});

afterAll(async () => {
  try {
    await fs.rm(tempDir, {recursive: true, force: true});
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(`Failed to clean up temp directory ${tempDir}:`, e);
  }

  sqlite.close();
});

class MockWebSocket {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  static readonly OPEN = 1;
  // eslint-disable-next-line @typescript-eslint/naming-convention
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
  send() {}
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
