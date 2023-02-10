import * as s from 'superstruct';
import type {ClientID} from './client-state.js';
import type {Storage} from '../storage/storage.js';

export const connectedClientSchema = s.array(s.string());
export const connectedClientsKey = 'connectedclients';

export async function getConnectedClients(
  storage: Storage,
): Promise<Set<ClientID>> {
  const connectedClients = await storage.get(
    connectedClientsKey,
    connectedClientSchema,
  );
  return new Set(connectedClients);
}

export async function putConnectedClients(
  clients: ReadonlySet<ClientID>,
  storage: Storage,
): Promise<void> {
  return await storage.put(connectedClientsKey, [...clients.values()]);
}

export async function addConnectedClient(
  clientID: ClientID,
  storage: Storage,
): Promise<void> {
  const connectedClients = await getConnectedClients(storage);
  if (!connectedClients.has(clientID)) {
    connectedClients.add(clientID);
    await putConnectedClients(connectedClients, storage);
  }
}
