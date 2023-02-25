import type {ClientMap, ClientState} from '../types/client-state.js';
import type {ConnectionsResponse} from 'reflect-protocol';

export function getConnections(clients: ClientMap): ConnectionsResponse {
  const connections: ConnectionsResponse = [];
  for (const [clientID, clientState] of clients) {
    connections.push({userID: clientState.userData.userID, clientID});
  }
  return connections;
}

export function closeConnections(
  clients: ClientMap,
  predicate: (clientState: ClientState) => boolean,
) {
  for (const clientState of clients.values()) {
    if (predicate(clientState)) {
      clientState.socket.close();
    }
  }
}
