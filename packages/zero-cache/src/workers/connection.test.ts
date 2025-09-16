/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
