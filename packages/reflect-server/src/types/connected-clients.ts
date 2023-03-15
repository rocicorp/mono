import * as valita from 'shared/valita.js';
import type {ClientID} from './client-state.js';
import type {Storage} from '../storage/storage.js';

export const connectedClientSchema = valita.array(valita.string());
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

export function putConnectedClients(
  clients: ReadonlySet<ClientID>,
  storage: Storage,
): Promise<void> {
  return storage.put(connectedClientsKey, [...clients.values()]);
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
