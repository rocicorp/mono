import type {LogContext} from '@rocicorp/logger';
import type {WebSocket} from 'ws';

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
  ws.close(code, truncate(errMsg));
}

// close messages must be less than or equal to 123 bytes:
// https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
function truncate(val: string, maxBytes = 123) {
  const encoder = new TextEncoder();
  if (encoder.encode(val).length <= maxBytes) {
    return val;
  }
  val = val.substring(0, maxBytes - 3);
  while (encoder.encode(val + '...').length > maxBytes) {
    val = val.substring(0, val.length - 1);
  }
  return val + '...';
}
