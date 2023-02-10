import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {putVersion, Version} from '../types/version.js';
import type {LogContext} from '@rocicorp/logger';
import {
  getConnectedClients,
  putConnectedClients,
} from '../types/connected-clients.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {ClientID} from '../types/client-state.js';

export async function processDisconnects(
  lc: LogContext,
  disconnectHandler: DisconnectHandler,
  connectedClients: ClientID[],
  storage: Storage,
  nextVersion: Version,
): Promise<void> {
  const currentlyConnectedClients = new Set(connectedClients);
  const storedConnectedClients = await getConnectedClients(storage);
  lc.debug?.(
    'Checking for disconnected clients.',
    'Currently connected',
    [...currentlyConnectedClients],
    'Stored connected',
    [...storedConnectedClients],
  );
  for (const clientID of storedConnectedClients) {
    if (!currentlyConnectedClients.has(clientID)) {
      lc.debug?.('Executing disconnectHandler for:', clientID);
      const cache = new EntryCache(storage);
      const tx = new ReplicacheTransaction(cache, clientID, nextVersion);
      try {
        await disconnectHandler(tx);
        await putVersion(nextVersion, cache);
        await cache.flush();
      } catch (e) {
        lc.info?.('Error executing disconnectHandler for:', clientID, e);
      }
    }
  }
  await putConnectedClients(currentlyConnectedClients, storage);
}
