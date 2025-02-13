import type {LogContext} from '@rocicorp/logger';
import type {WebSocket} from 'ws';
import {elide} from './strings.ts';

// https://github.com/Luka967/websocket-close-codes
export const PROTOCOL_ERROR = 1002;
export const INTERNAL_ERROR = 1011;

export type ErrorCode = typeof PROTOCOL_ERROR | typeof INTERNAL_ERROR;

export function closeWithError(
  lc: LogContext,
  ws: WebSocket,
  err: unknown,
  code: ErrorCode = INTERNAL_ERROR,
) {
  const endpoint = ws.url ?? 'client';
  const errMsg = String(err);
  lc.warn?.(`closing connection to ${endpoint} with error`, errMsg);

  // close messages must be less than or equal to 123 bytes:
  // https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
  ws.close(code, elide(errMsg, 123));
}
