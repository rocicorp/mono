import type {ClientMap} from '../types/client-state.js';
import type {LogContext} from '@rocicorp/logger';
import {closeWithError} from 'shared/src/cf/socket.js';

export function handleAuthInvalidate(
  lc: LogContext,
  clients: ClientMap,
  userID?: string,
): Response {
  let closedCount = 0;
  for (const clientState of clients.values()) {
    if (userID === undefined || userID === clientState.auth.userID) {
      closeWithError(lc, clientState.socket, 'AuthInvalidated');
      closedCount++;
    }
  }
  lc.debug?.('Closed', closedCount, 'connections.');
  return new Response('Success', {status: 200});
}
