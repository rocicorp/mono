import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.ts';
import type {Store} from '../dag/store.ts';
import {
  addDeletedClients,
  type WritableDeletedClients,
} from '../deleted-clients.ts';
import type {ClientID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import type {Client, OnClientsDeleted} from './clients.ts';
import {type ClientMap, getClients, setClients} from './clients.ts';

/**
 * The maximum time a client can be inactive before it is garbage collected.
 * This means that this is the maximum time a tab can be in the background
 * (frozen) and still be able to sync when it comes back to the foreground.
 */
export const CLIENT_MAX_INACTIVE_TIME = 24 * 60 * 60 * 1000; // 24 hours

/**
 * How frequently to try to garbage collect clients.
 */
export const GC_INTERVAL = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientMap> | undefined {
  return latestGCUpdate;
}

export function initClientGC(
  clientID: ClientID,
  dagStore: Store,
  clientMaxInactiveTime: number,
  gcInterval: number,
  onClientsDeleted: OnClientsDeleted,
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(
        clientID,
        dagStore,
        clientMaxInactiveTime,
        onClientsDeleted,
      );
      return latestGCUpdate;
    },
    () => gcInterval,
    lc,
    signal,
  );
}

function gcClients(
  clientID: ClientID,
  dagStore: Store,
  clientMaxInactiveTime: number,
  onClientsDeleted: OnClientsDeleted,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const now = Date.now();
    const clients = await getClients(dagWrite);
    const deletedClients: WritableDeletedClients = [];
    const newClients: Map<ClientID, Client> = new Map();
    for (const [id, client] of clients) {
      if (
        id === clientID /* never collect ourself */ ||
        now - client.heartbeatTimestampMs <= clientMaxInactiveTime
      ) {
        newClients.set(id, client);
      } else {
        deletedClients.push({
          clientGroupID: client.clientGroupID,
          clientID: id,
        });
      }
    }

    if (newClients.size === clients.size) {
      return clients;
    }
    await setClients(newClients, dagWrite);
    const normalizedDeletedClients = await addDeletedClients(
      dagWrite,
      deletedClients,
    );
    await onClientsDeleted(normalizedDeletedClients);
    return newClients;
  });
}
