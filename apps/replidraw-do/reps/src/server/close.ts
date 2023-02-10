import { ClientID, ClientMap } from "../types/client-state";

export function handleClose(clients: ClientMap, clientID: ClientID) {
  clients.delete(clientID);
}
