import type {LogContext} from '@rocicorp/logger';
import type {Version} from 'reflect-protocol';
import type {DisconnectHandler} from '../server/disconnect.js';
import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientID} from '../types/client-state.js';
import {
  getConnectedClients,
  putConnectedClients,
} from '../types/connected-clients.js';
import {putVersion} from '../types/version.js';

const NOOP_MUTATION_ID = -1;

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
      const tx = new ReplicacheTransaction(
        cache,
        clientID,
        NOOP_MUTATION_ID,
        nextVersion,
        undefined,
      );
      try {
        await disconnectHandler(tx);
        // TODO only update version if disconnectHandler modifies state
        await putVersion(nextVersion, cache);
        await cache.flush();
      } catch (e) {
        lc.info?.('Error executing disconnectHandler for:', clientID, e);
      }
    }
  }
  await putConnectedClients(currentlyConnectedClients, storage);
}
