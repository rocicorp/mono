import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.js';
import type {Store} from '../dag/store.js';
import type {ClientID} from '../sync/ids.js';
import {withWrite} from '../with-transactions.js';
import {ClientMap, getClients, setClients} from './clients.js';

const CLIENT_MAX_INACTIVE_IN_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const GC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientMap> | undefined {
  return latestGCUpdate;
}
export function initClientGC(
  clientID: ClientID,
  dagStore: Store,
  onClientsRemoved: (clientIDs: Set<ClientID>) => void,
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(clientID, dagStore, onClientsRemoved);
      return latestGCUpdate;
    },
    () => GC_INTERVAL_MS,
    lc,
    signal,
  );
}

type Writable<M> = M extends ReadonlyMap<infer K, infer V> ? Map<K, V> : never;

function gcClients(
  clientID: ClientID,
  dagStore: Store,
  onClientsRemoved: (clientIDs: Set<ClientID>) => void,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const now = Date.now();
    const oldClients = await getClients(dagWrite);
    const newClients: Writable<ClientMap> = new Map();
    const removedClientIDs: Set<ClientID> = new Set();
    for (const [id, client] of oldClients) {
      // never collect ourself
      if (
        id === clientID ||
        now - client.heartbeatTimestampMs <= CLIENT_MAX_INACTIVE_IN_MS
      ) {
        newClients.set(id, client);
      } else {
        removedClientIDs.add(id);
      }
    }
    if (newClients.size === oldClients.size) {
      return oldClients;
    }
    if (removedClientIDs.size > 0) {
      onClientsRemoved(removedClientIDs);
    }
    await setClients(newClients, dagWrite);
    return newClients;
  });
}
