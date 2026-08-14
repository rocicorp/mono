import {LogContext, type LogLevel} from '@rocicorp/logger';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import WebSocket from 'ws';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  LAST_POKE_PART_PROTOCOL_VERSION,
  POKE_CHUNK_MESSAGE_TYPE,
  POKE_CHUNK_PROTOCOL_VERSION,
  type PokeChunk,
  type PokeEndMessage,
  type PokePartMessage,
  type PokeStartMessage,
} from '../../../zero-protocol/src/poke.ts';
import {MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} from '../../../zero-protocol/src/protocol-version.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {
  DownstreamSender,
  send,
  sendError,
  WEBSOCKET_SEND_TIMEOUT_MS,
  type WebSocketLike,
} from './connection.ts';

class MockSocket implements WebSocketLike {
  readyState: WebSocket['readyState'] = WebSocket.OPEN;
  readonly sent: (string | PokeChunk)[] = [];
  readonly autoComplete: boolean;

  constructor(autoComplete = false) {
    this.autoComplete = autoComplete;
  }

  send(data: string | PokeChunk, cb?: (err?: Error) => void) {
    this.sent.push(typeof data === 'string' ? data : data.slice());
    if (this.autoComplete) {
      cb?.();
    }
  }
}

describe('send', () => {
  const lc = createSilentLogContext();
  let ws: MockSocket;
  const data: Downstream = ['pong', {}];

  beforeEach(() => {
    ws = new MockSocket();
  });

  test('invokes callback immediately when socket already closed', () => {
    const callback = vi.fn();
    ws.readyState = WebSocket.CLOSED;
    send(lc, ws, data, callback);
    expect(callback).toHaveBeenCalledTimes(1);
    const [errorArg] = callback.mock.calls[0]!;
    expect(errorArg).toBeInstanceOf(ProtocolErrorWithLevel);
    const typedError = errorArg as ProtocolErrorWithLevel;
    expect(typedError.errorBody).toEqual({
      kind: ErrorKind.Internal,
      message: 'WebSocket closed',
      origin: ErrorOrigin.ZeroCache,
    });
    expect(typedError.logLevel).toBe('info');
  });

  test('passes callback to websocket when open', () => {
    using sendSpy = vi.spyOn(ws, 'send');
    const callback = () => {};
    ws.readyState = WebSocket.OPEN;
    send(lc, ws, data, callback);
    expect(sendSpy).toHaveBeenCalledWith(
      JSON.stringify(data),
      expect.any(Function),
    );
  });

  test('fails a stalled websocket send after the timeout', async () => {
    vi.useFakeTimers();
    try {
      const callback = vi.fn();
      send(lc, ws, data, callback);

      await vi.advanceTimersByTimeAsync(WEBSOCKET_SEND_TIMEOUT_MS);

      expect(callback).toHaveBeenCalledTimes(1);
      const error = callback.mock.calls[0]![0] as ProtocolErrorWithLevel;
      expect(error).toBeInstanceOf(ProtocolErrorWithLevel);
      expect(error.errorBody).toEqual({
        kind: ErrorKind.Internal,
        message: `WebSocket send timed out after ${WEBSOCKET_SEND_TIMEOUT_MS} ms`,
        origin: ErrorOrigin.ZeroCache,
      });
      expect(error.logLevel).toBe('info');
    } finally {
      vi.useRealTimers();
    }
  });

  test('invokes the callback only once when a timed-out send later completes', async () => {
    vi.useFakeTimers();
    try {
      let sendCallback: ((err?: Error) => void) | undefined;
      ws.send = (_data, callback) => {
        sendCallback = callback;
      };
      const callback = vi.fn();
      send(lc, ws, data, callback);

      await vi.advanceTimersByTimeAsync(WEBSOCKET_SEND_TIMEOUT_MS);
      sendCallback?.();

      expect(callback).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  test('reuses an already serialized downstream message', () => {
    using sendSpy = vi.spyOn(ws, 'send');
    const callback = () => {};
    const serialized = '["pong", {"cached": true}]';

    send(lc, ws, data, callback, serialized);

    expect(sendSpy).toHaveBeenCalledWith(serialized, expect.any(Function));
  });
});

describe('DownstreamSender poke compatibility', () => {
  const lc = createSilentLogContext();
  const pokeStart = [
    'pokeStart',
    {pokeID: '01', baseCookie: null},
  ] satisfies PokeStartMessage;
  const pokePart = [
    'pokePart',
    {pokeID: '01', rowsPatch: [{op: 'clear'}]},
  ] satisfies PokePartMessage;
  const pokeEnd = [
    'pokeEnd',
    {pokeID: '01', cookie: '01'},
  ] satisfies PokeEndMessage;

  test.each([
    MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
    LAST_POKE_PART_PROTOCOL_VERSION,
  ])(
    'keeps sending JSON pokePart messages to protocol version %i',
    async protocolVersion => {
      const ws = new MockSocket(true);
      const sender = new DownstreamSender(lc, ws, protocolVersion);

      await sendWithBackpressure(sender, pokeStart);
      await sendWithBackpressure(sender, pokePart);
      await sendWithBackpressure(sender, pokeEnd);

      expect(ws.sent).toEqual([
        JSON.stringify(pokeStart),
        JSON.stringify(pokePart),
        JSON.stringify(pokeEnd),
      ]);
    },
  );

  test('sends binary poke chunks only to clients at the cutoff', async () => {
    const ws = new MockSocket(true);
    const sender = new DownstreamSender(lc, ws, POKE_CHUNK_PROTOCOL_VERSION);

    await sendWithBackpressure(sender, pokeStart);
    await sendWithBackpressure(sender, pokePart);
    await sendWithBackpressure(sender, pokeEnd);

    expect(ws.sent).toHaveLength(3);
    expect(ws.sent[0]).toBe(JSON.stringify(pokeStart));
    expect(ws.sent[1]).toBeInstanceOf(Uint8Array);
    const chunk = ws.sent[1] as PokeChunk;
    expect(chunk[0]).toBe(POKE_CHUNK_MESSAGE_TYPE);
    expect(new TextDecoder().decode(chunk.subarray(1))).toBe(
      '[{"pokeID":"01","rowsPatch":[{"op":"clear"}]}]',
    );
    expect(ws.sent[2]).toBe(JSON.stringify(pokeEnd));
  });

  test('fails a stalled binary send after the websocket timeout', async () => {
    vi.useFakeTimers();
    try {
      const ws = new MockSocket();
      const sender = new DownstreamSender(lc, ws, POKE_CHUNK_PROTOCOL_VERSION);

      sender.send(pokeStart, 'ignore-backpressure');
      await sendWithBackpressure(sender, pokePart);
      const pokeEndPromise = sendWithBackpressure(sender, pokeEnd);
      const rejection = expect(pokeEndPromise).rejects.toMatchObject({
        errorBody: {
          kind: ErrorKind.Internal,
          message: `WebSocket send timed out after ${WEBSOCKET_SEND_TIMEOUT_MS} ms`,
          origin: ErrorOrigin.ZeroCache,
        },
        logLevel: 'info',
      });

      await vi.advanceTimersByTimeAsync(WEBSOCKET_SEND_TIMEOUT_MS);
      await rejection;
    } finally {
      vi.useRealTimers();
    }
  });
});

function sendWithBackpressure(
  sender: DownstreamSender,
  downstream: Downstream,
): Promise<void> {
  return new Promise((resolve, reject) => {
    sender.send(downstream, error => (error ? reject(error) : resolve()));
  });
}

describe('sendError', () => {
  let sink: TestLogSink;
  let lc: LogContext;
  let ws: WebSocket;

  beforeEach(() => {
    sink = new TestLogSink();
    lc = new LogContext('debug', {worker: 'test'}, sink);
    ws = {
      readyState: WebSocket.OPEN,
      send: vi.fn(),
    } as unknown as WebSocket;
  });

  const lastLogLevel = (): LogLevel | undefined => sink.messages.at(-1)?.[0];

  test('ClientNotFound errors are logged as warnings', () => {
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.ClientNotFound,
        message: 'Client not found',
        origin: ErrorOrigin.ZeroCache,
      },
      undefined,
    );
    expect(lastLogLevel()).toBe('warn');
  });

  test('TransformFailed errors are logged as warnings', () => {
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.TransformFailed,
        message: 'bad transform config',
        origin: ErrorOrigin.ZeroCache,
        queryIDs: ['query1'],
        reason: 'internal',
      },
      undefined,
    );
    expect(lastLogLevel()).toBe('warn');
  });

  test('socket write errno errors are logged as warnings', () => {
    const err = Object.assign(new Error('write EPIPE'), {
      errno: -32,
      code: 'EPIPE',
    });
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'write EPIPE',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    expect(lastLogLevel()).toBe('warn');
  });

  test('ECANCELED error code is logged as warning', () => {
    const err = Object.assign(new Error('write ECANCELED'), {
      code: 'ECANCELED',
    });
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'write ECANCELED',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    expect(lastLogLevel()).toBe('warn');
  });

  test('other protocol errors remain at their default level', () => {
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'unexpected failure',
        origin: ErrorOrigin.ZeroCache,
      },
      undefined,
    );
    expect(lastLogLevel()).toBe('info');
  });

  test('ProtocolErrorWithLevel uses its logLevel', () => {
    const err = new ProtocolErrorWithLevel(
      {
        kind: ErrorKind.Internal,
        message: 'protocol error',
        origin: ErrorOrigin.ZeroCache,
      },
      'debug',
    );
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'wrapper message',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    expect(lastLogLevel()).toBe('debug');
  });

  test('ProtocolErrorWithLevel takes precedence over errorBody kind', () => {
    // ProtocolErrorWithLevel's logLevel takes precedence, even if errorBody.kind would classify it differently
    const err = new ProtocolErrorWithLevel(
      {
        kind: ErrorKind.ClientNotFound,
        message: 'client not found',
        origin: ErrorOrigin.ZeroCache,
      },
      'error',
    );
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.ClientNotFound,
        message: 'client not found',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    // ProtocolErrorWithLevel specifies 'error', so that takes precedence
    expect(lastLogLevel()).toBe('error');
  });

  test('ECONNRESET error code is logged as warning', () => {
    const err = Object.assign(new Error('read ECONNRESET'), {
      code: 'ECONNRESET',
    });
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'read ECONNRESET',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    expect(lastLogLevel()).toBe('warn');
  });

  test('socket closed while compressing is logged as warning', () => {
    const err = new Error(
      'The socket was closed while data was being compressed',
    );
    sendError(
      lc,
      ws,
      {
        kind: ErrorKind.Internal,
        message: 'The socket was closed while data was being compressed',
        origin: ErrorOrigin.ZeroCache,
      },
      err,
    );
    expect(lastLogLevel()).toBe('warn');
  });
});
