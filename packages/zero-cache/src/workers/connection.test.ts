import {beforeEach, describe, expect, test, vi} from 'vitest';
import WebSocket, {type RawData} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {send} from './connection.ts';

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
