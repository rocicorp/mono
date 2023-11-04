import type {LogContext} from '@rocicorp/logger';
import type {Version} from 'reflect-protocol';
import type {Env} from 'reflect-shared';
import type {PendingMutation} from 'replicache';
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
  env: Env,
  disconnectHandler: DisconnectHandler,
  connectedClients: ClientID[],
  pendingMutations: PendingMutation[],
  numPendingMutationsProcessed: number,
  storage: Storage,
  nextVersion: Version,
): Promise<void> {
  const currentlyConnectedClients = new Set(connectedClients);
  const clientsWithPendingMutations = new Set();
  for (let i = numPendingMutationsProcessed; i < pendingMutations.length; i++) {
    clientsWithPendingMutations.add(pendingMutations[i].clientID);
  }
  const storedConnectedClients = await getConnectedClients(storage);
  const newStoredConnectedClients = new Set<ClientID>(
    currentlyConnectedClients,
  );
  lc.debug?.(
    'Checking for disconnected clients.',
    'Currently connected',
    [...currentlyConnectedClients],
    'Stored connected',
    [...storedConnectedClients],
  );
  for (const clientID of storedConnectedClients) {
    if (
      currentlyConnectedClients.has(clientID) ||
      clientsWithPendingMutations.has(clientID)
    ) {
      newStoredConnectedClients.add(clientID);
      if (!currentlyConnectedClients.has(clientID)) {
        lc.debug?.(
          'Not Executing disconnectHandler for disconnected:',
          clientID,
          'because it has pending mutations',
        );
      }
    } else {
      lc.debug?.('Executing disconnectHandler for:', clientID);
      const cache = new EntryCache(storage);
      const tx = new ReplicacheTransaction(
        cache,
        clientID,
        NOOP_MUTATION_ID,
        nextVersion,
        undefined,
        env,
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
  await putConnectedClients(newStoredConnectedClients, storage);
}
