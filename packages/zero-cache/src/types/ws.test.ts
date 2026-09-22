import {EventEmitter} from 'node:events';
import {resolver} from '@rocicorp/resolver';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';
import WebSocket, {WebSocketServer} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {closeWithError, expectPingsForLiveness, PROTOCOL_ERROR} from './ws.ts';

describe('types/ws', () => {
  let port: number;
  let wss: WebSocketServer;

  beforeAll(() => {
    port = randInt(10000, 20000);
    wss = new WebSocketServer({port});
  });

  afterAll(() => {
    wss.close();
  });

  test('close with protocol error', async () => {
    wss.on('connection', ws =>
      closeWithError(
        createSilentLogContext(),
        ws,
        'こんにちは' + 'あ'.repeat(150),
        PROTOCOL_ERROR,
      ),
    );

    const ws = new WebSocket(`ws://localhost:${port}/`);
    const {promise, resolve} = resolver<{code: number; reason: string}>();
    ws.on('close', (code, reason) =>
      resolve({code, reason: reason.toString('utf-8')}),
    );

    const error = await promise;
    expect(error).toMatchInlineSnapshot(`
      {
        "code": 1002,
        "reason": "こんにちはあああああああああああああああああああああああああああああああああああ...",
      }
    `);
    // close messages must be less than or equal to 123 bytes:
    // https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
    expect(new TextEncoder().encode(error.reason).length).toBeLessThanOrEqual(
      123,
    );
  });
});

describe('expectPingsForLiveness', () => {
  const INTERVAL_MS = 1000;
  const BUFFER_MS = 100;
  const TIMEOUT_MS = INTERVAL_MS + BUFFER_MS;

  class FakeWebSocket extends EventEmitter {
    readonly CONNECTING = WebSocket.CONNECTING;
    readonly OPEN = WebSocket.OPEN;
    readonly url = 'ws://fake/';
    readyState: number;
    readonly terminate = vi.fn(() => {
      this.readyState = WebSocket.CLOSED;
      this.emit('close');
    });

    constructor(readyState: number) {
      super();
      this.readyState = readyState;
    }

    open() {
      this.readyState = WebSocket.OPEN;
      this.emit('open');
    }
  }

  function watch(readyState: number) {
    const ws = new FakeWebSocket(readyState);
    expectPingsForLiveness(
      createSilentLogContext(),
      ws as unknown as WebSocket,
      INTERVAL_MS,
      BUFFER_MS,
    );
    return ws;
  }

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test('terminates an open socket that does not send heartbeats', () => {
    const ws = watch(WebSocket.OPEN);
    vi.advanceTimersByTime(TIMEOUT_MS - 1);
    expect(ws.terminate).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(ws.terminate).toHaveBeenCalledOnce();
  });

  test('keeps an open socket that sends heartbeats', () => {
    const ws = watch(WebSocket.OPEN);
    for (let i = 0; i < 5; i++) {
      ws.emit(i % 2 ? 'ping' : 'message');
      vi.advanceTimersByTime(TIMEOUT_MS);
    }
    expect(ws.terminate).not.toHaveBeenCalled();
  });

  test('terminates a socket that does not connect in time', () => {
    const ws = watch(WebSocket.CONNECTING);
    vi.advanceTimersByTime(TIMEOUT_MS - 1);
    expect(ws.terminate).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(ws.terminate).toHaveBeenCalledOnce();

    // No further timers once closed.
    vi.advanceTimersByTime(TIMEOUT_MS * 10);
    expect(ws.terminate).toHaveBeenCalledOnce();
  });

  test('starts heartbeat checks when a slow handshake completes', () => {
    const ws = watch(WebSocket.CONNECTING);
    vi.advanceTimersByTime(TIMEOUT_MS - 10);
    ws.open();

    // The connect timeout no longer applies, and the first heartbeat
    // interval starts from when the socket opened.
    vi.advanceTimersByTime(TIMEOUT_MS - 1);
    expect(ws.terminate).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(ws.terminate).toHaveBeenCalledOnce();
  });

  test('clears timers when a connecting socket closes', () => {
    const ws = watch(WebSocket.CONNECTING);
    ws.readyState = WebSocket.CLOSED;
    ws.emit('close');
    expect(vi.getTimerCount()).toBe(0);
  });

  test('clears timers when an open socket closes', () => {
    const ws = watch(WebSocket.CONNECTING);
    ws.open();
    ws.readyState = WebSocket.CLOSED;
    ws.emit('close');
    expect(vi.getTimerCount()).toBe(0);
  });
});
