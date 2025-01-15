import type {LogContext} from '@rocicorp/logger';
import type {WebSocket} from 'ws';

export function closeWithProtocolError(
  lc: LogContext,
  ws: WebSocket,
  err: unknown,
) {
  const errMsg = String(err);
  lc.warn?.('closing with protocol error', errMsg);
  ws.close(1002 /* "protocol error" */, truncate(errMsg));
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
