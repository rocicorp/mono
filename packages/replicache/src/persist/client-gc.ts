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
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(clientID, dagStore);
      return latestGCUpdate;
    },
    () => GC_INTERVAL_MS,
    lc,
    signal,
  );
}

function gcClients(clientID: ClientID, dagStore: Store): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const now = Date.now();
    const clients = await getClients(dagWrite);
    const clientsAfterGC = Array.from(clients).filter(
      ([id, client]) =>
        id === clientID /* never collect ourself */ ||
        now - client.heartbeatTimestampMs <= CLIENT_MAX_INACTIVE_IN_MS,
    );
    if (clientsAfterGC.length === clients.size) {
      return clients;
    }
    const newClients = new Map(clientsAfterGC);
    await setClients(newClients, dagWrite);
    await dagWrite.commit();
    return newClients;
  });
}
