import {beforeEach, describe, expect, test, vi} from 'vitest';
import WebSocket, {type RawData} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {send, sendError} from './connection.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';

class MockSocket implements Pick<WebSocket, 'readyState' | 'send'> {
  readyState: WebSocket['readyState'] = WebSocket.OPEN;

  send(data: RawData, cb?: (err?: Error) => void): void;
  send(
    data: RawData,
    options: {
      mask?: boolean | undefined;
      binary?: boolean | undefined;
      compress?: boolean | undefined;
      fin?: boolean | undefined;
    },
    cb?: (err?: Error) => void,
  ): void;
  send(
    _data: RawData,
    _optionsOrCb?: unknown,
    _maybeCb?: (err?: Error) => void,
  ) {}
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
  const lc = createSilentLogContext();
  const errorBody: ErrorBody = {
    kind: ErrorKind.Internal,
    message: 'boom',
    origin: ErrorOrigin.ZeroCache,
  };

  test('waits for websocket callback before resolving', async () => {
    const ws = new MockSocket();
    let storedCallback: ((err?: Error) => void) | undefined;
    const sendSpy = vi.spyOn(ws, 'send').mockImplementation(((
      _message: RawData,
      optionsOrCb?:
        | {
            mask?: boolean | undefined;
            binary?: boolean | undefined;
            compress?: boolean | undefined;
            fin?: boolean | undefined;
          }
        | ((err?: Error) => void),
      maybeCb?: (err?: Error) => void,
    ) => {
      storedCallback =
        typeof optionsOrCb === 'function' ? optionsOrCb : maybeCb;
    }) as typeof ws.send);

    let resolved = false;
    const promise = sendError(lc, ws as unknown as WebSocket, errorBody).then(
      () => {
        resolved = true;
      },
    );

    await Promise.resolve();
    expect(resolved).toBe(false);
    expect(sendSpy).toHaveBeenCalledTimes(1);
    expect(storedCallback).toBeTypeOf('function');

    storedCallback?.();
    await promise;
    expect(resolved).toBe(true);
  });

  test('resolves even if socket already closed', async () => {
    const ws = new MockSocket();
    ws.readyState = WebSocket.CLOSED;
    await expect(
      sendError(lc, ws as unknown as WebSocket, errorBody),
    ).resolves.toBeUndefined();
  });
});
