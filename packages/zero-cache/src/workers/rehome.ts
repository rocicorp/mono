import type {LogContext} from '@rocicorp/logger';
import type {WebSocket} from 'ws';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {sendError} from './connection.ts';

/**
 * Returns the `Set-Cookie` header line that expires the configured load
 * balancer affinity cookie, or `undefined` if none is configured. It must be
 * part of the 101 response of a draining handshake so that the client's next
 * attempt is routed to a different server.
 */
export function expiredAffinityCookieHeader({
  affinityCookie,
}: Pick<ZeroConfig, 'affinityCookie'>): string | undefined {
  if (!affinityCookie?.name) {
    return undefined;
  }
  const {name, path, domain} = affinityCookie;
  return `Set-Cookie: ${name}=; Max-Age=0; Path=${path}${domain ? `; Domain=${domain}` : ''}; Secure; HttpOnly; SameSite=None`;
}

/**
 * Sends a Rehome error to a connection on a draining server and closes it.
 */
export function rehome(lc: LogContext, ws: WebSocket) {
  ws.on('error', error => {
    lc.warn?.('error while rehoming draining connection', error);
    ws.terminate();
  });
  const error = new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.Rehome,
      message: 'Server is draining',
      origin: ErrorOrigin.ZeroCache,
    },
    'info',
  );
  sendError(lc, ws, error.errorBody, error);
  ws.close(1001, 'Server is draining');
}
