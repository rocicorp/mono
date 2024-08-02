import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.js';
import type {Store} from '../dag/store.js';
import type {ClientID} from '../sync/ids.js';
import {withWrite} from '../with-transactions.js';
import {ClientMap, getClients, setClients} from './clients.js';

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
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(clientID, dagStore, clientMaxInactiveTime);
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
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const now = Date.now();
    const clients = await getClients(dagWrite);
    const clientsAfterGC = Array.from(clients).filter(
      ([id, client]) =>
        id === clientID /* never collect ourself */ ||
        now - client.heartbeatTimestampMs <= clientMaxInactiveTime,
    );
    if (clientsAfterGC.length === clients.size) {
      return clients;
    }
    const newClients = new Map(clientsAfterGC);
    await setClients(newClients, dagWrite);
    return newClients;
  });
}
