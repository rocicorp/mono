import {describe, expect, test, vi} from 'vitest';
import WebSocket from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorWithLevel} from '../types/error-for-client.ts';
import {send} from './connection.ts';

class MockSocket implements Pick<WebSocket, 'readyState' | 'send'> {
  readyState: WebSocket['readyState'] = WebSocket.OPEN;
  send(_message: string) {}
}

describe('send', () => {
  const lc = createSilentLogContext();
  const ws = new MockSocket();
  const data: Downstream = ['pong', {}];

  test('CLOSED', () => {
    const callback = vi.fn();
    ws.readyState = WebSocket.CLOSED;
    send(lc, ws, data, callback);
    expect(callback).toHaveBeenCalledWith(
      new ErrorWithLevel('websocket closed', 'info'),
    );
  });

  test('OPEN', () => {
    using sendSpy = vi.spyOn(ws, 'send');
    const callback = () => {};
    ws.readyState = WebSocket.OPEN;
    send(lc, ws, data, callback);
    expect(sendSpy).toHaveBeenCalledWith(JSON.stringify(data), callback);
  });
});
