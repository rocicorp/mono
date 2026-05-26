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
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {send, sendError, type WebSocketLike} from './connection.ts';

class MockSocket implements WebSocketLike {
  readyState: WebSocket['readyState'] = WebSocket.OPEN;
  send(_data: string, _cb?: (err?: Error) => void) {}
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
    expect(sendSpy).toHaveBeenCalledWith(JSON.stringify(data), callback);
  });
});

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
