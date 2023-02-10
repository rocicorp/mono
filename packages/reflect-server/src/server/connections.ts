import type { ClientMap, ClientState } from "../types/client-state";
import type { ConnectionsResponse } from "../protocol/api/auth";

export function getConnections(clients: ClientMap): ConnectionsResponse {
  const connections: ConnectionsResponse = [];
  for (const [clientID, clientState] of clients) {
    connections.push({ userID: clientState.userData.userID, clientID });
  }
  return connections;
}

export function closeConnections(
  clients: ClientMap,
  predicate: (clientState: ClientState) => boolean
) {
  for (const clientState of clients.values()) {
    if (predicate(clientState)) {
      clientState.socket.close();
    }
  }
}
