import type {LogContext} from '@rocicorp/logger';
import type {Downstream, ErrorKind, ErrorMessage} from 'reflect-protocol';
import type {
  TailErrorKind,
  TailErrorMessage,
} from 'reflect-protocol/src/tail.js';
import type {Socket} from '../types/client-state.js';

export function sendError(
  lc: LogContext,
  ws: Socket,
  kind: ErrorKind,
  message = '',
  logLevel: 'info' | 'error' = 'info',
) {
  const data: ErrorMessage = ['error', kind, message];
  sendErrorInternal(
    lc,
    'Sending error on socket',
    ws,
    data,
    kind,
    message,
    logLevel,
  );
}

/**
 * msg is optional and will be truncated to 123 bytes.
 */
export function closeWithError(
  lc: LogContext,
  ws: Socket,
  kind: ErrorKind,
  message = '',
  logLevel: 'info' | 'error' = 'info',
) {
  const data: ErrorMessage = ['error', kind, message];
  closeWithErrorInternal(lc, ws, data, kind, message, logLevel);
}

/**
 * msg is optional and will be truncated to 123 bytes.
 */
function closeWithErrorInternal<Data, Kind>(
  lc: LogContext,
  ws: Socket,
  data: Data,
  kind: Kind,
  message = '',
  logLevel: 'info' | 'error' = 'info',
) {
  sendErrorInternal(
    lc,
    'Closing socket with error',
    ws,
    data,
    kind,
    message,
    logLevel,
  );
  ws.close();
}

function sendErrorInternal<Data, Kind>(
  lc: LogContext,
  logMessage: string,
  ws: Socket,
  data: Data,
  kind: Kind,
  message = '',
  logLevel: 'info' | 'error' = 'info',
) {
  const log = (...args: unknown[]) =>
    logLevel === 'info' ? lc.info?.(...args) : lc.error?.(...args);
  log(logMessage, {
    kind,
    message,
  });
  ws.send(JSON.stringify(data));
}

export function send(ws: Socket, data: Downstream) {
  ws.send(JSON.stringify(data));
}

export function encodeReason(msg: string): string {
  // WebSocket close reason length must be less than 123 bytes UTF-8 (RFC 6455)
  // We replace all non ascii characters in msg with '?'. We then encode the
  // reason as "kind: msg" and truncate to 123 bytes.

  msg = msg
    // eslint-disable-next-line no-control-regex
    .replace(/[^\x00-\x7F]/gu, '?');

  if (msg.length > 123) {
    msg = msg.slice(0, 120) + '...';
  }

  return msg;
}

export const SEC_WEBSOCKET_PROTOCOL_HEADER = 'Sec-WebSocket-Protocol';

/**
 * Creates a WebSocketPair and immediately closes the server side with an error.
 *
 * The request headers needs to be passed so that we can copy the
 * Sec-WebSocket-Protocol header to the response as per the spec.
 */
export function createWSAndCloseWithError(
  lc: LogContext,
  request: Request,
  kind: ErrorKind,
  message: string,
) {
  const data: ErrorMessage = ['error', kind, message];
  return createWSAndCloseWithErrorInternal(lc, request, data, kind, message);
}

/**
 * Creates a WebSocketPair and immediately closes the server side with an error.
 *
 * The request headers needs to be passed so that we can copy the
 * Sec-WebSocket-Protocol header to the response as per the spec.
 */
export function createWSAndCloseWithTailError(
  lc: LogContext,
  request: Request,
  kind: TailErrorKind,
  message: string,
) {
  const data: TailErrorMessage = {type: 'error', kind, message};
  return createWSAndCloseWithErrorInternal(lc, request, data, kind, message);
}

function createWSAndCloseWithErrorInternal<Data, Kind>(
  lc: LogContext,
  request: Request,
  data: Data,
  kind: Kind,
  message: string,
) {
  const pair = new WebSocketPair();
  const ws = pair[1];
  lc.info?.('accepting connection to send error', request.url);
  ws.accept();

  // MDN tells me that the message will be delivered even if we call close
  // immediately after send:
  //   https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close
  // However the relevant section of the RFC says this behavior is non-normative?
  //   https://www.rfc-editor.org/rfc/rfc6455.html#section-1.4
  // In any case, it seems to work just fine to send the message and
  // close before even returning the response.
  closeWithErrorInternal(lc, ws, data, kind, message);

  const responseHeaders = new Headers();
  const protocolHeader = request.headers.get(SEC_WEBSOCKET_PROTOCOL_HEADER);
  if (protocolHeader !== null) {
    responseHeaders.set(SEC_WEBSOCKET_PROTOCOL_HEADER, protocolHeader);
  }
  return new Response(null, {
    status: 101,
    headers: responseHeaders,
    webSocket: pair[0],
  });
}
