import type {LogContext} from '@rocicorp/logger';
import type {ClientID, ClientMap} from '../types/client-state.js';
import type {Socket} from '../util/socket.js';

export function handleClose(
  lc: LogContext,
  clients: ClientMap,
  clientID: ClientID,
  ws: Socket,
) {
  const client = clients.get(clientID);
  if (client?.socket === ws) {
    lc.debug?.('on socket close deleting client map entry for', clientID);
    clients.delete(clientID);
  }
}
