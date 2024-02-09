import type {LogContext} from '@rocicorp/logger';
import type {Version} from 'reflect-protocol';
import type {Env} from 'reflect-shared/src/types.js';
import type {PendingMutation} from 'replicache';
import {equals as setEquals} from 'shared/src/set-utils.js';
import type {ClientDeleteHandler} from '../server/client-delete-handler.js';
import type {ClientDisconnectHandler} from '../server/client-disconnect-handler.js';
import {collectClientIfDeleted, updateLastSeen} from '../server/client-gc.js';
import {EntryCache} from '../storage/entry-cache.js';
import {
  NOOP_MUTATION_ID,
  ReplicacheTransaction,
} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientID} from '../types/client-state.js';
import {
  getConnectedClients,
  putConnectedClients,
} from '../types/connected-clients.js';
import {putVersion} from '../types/version.js';

export async function processDisconnects(
  lc: LogContext,
  env: Env,
  clientDisconnectHandler: ClientDisconnectHandler,
  clientDeleteHandler: ClientDeleteHandler,
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
          'Not Executing clientDisconnectHandler for disconnected:',
          clientID,
          'because it has pending mutations',
        );
      }
    } else {
      lc.debug?.('Executing clientDisconnectHandler for:', clientID);
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
        await clientDisconnectHandler(tx);

        // TODO only update version if clientDisconnectHandler modifies state
        await putVersion(nextVersion, cache);
        await cache.flush();
      } catch (e) {
        lc.info?.('Error executing clientDisconnectHandler for:', clientID, e);
      }

      await collectClientIfDeleted(
        lc,
        env,
        clientID,
        clientDeleteHandler,
        storage,
        nextVersion,
      );
    }
  }

  if (!setEquals(storedConnectedClients, newStoredConnectedClients)) {
    await Promise.all([
      putConnectedClients(newStoredConnectedClients, storage),
      updateLastSeen(
        lc,
        storedConnectedClients,
        newStoredConnectedClients,
        storage,
        Date.now(),
      ),
    ]);
  }
}
