import type {LogContext} from '@rocicorp/logger';
import type {ClientID} from '../sync/client-id';
import type * as dag from '../dag/mod';
import {ClientMap, noUpdates, updateClients} from './clients';
import {initBgIntervalProcess} from './bg-interval';

const CLIENT_MAX_INACTIVE_IN_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const GC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientMap> | undefined {
  return latestGCUpdate;
}

export function initClientGC(
  clientID: ClientID,
  dagStore: dag.Store,
  lc: LogContext,
): () => void {
  return initBgIntervalProcess(
    'ClientGC',
    () => {
      latestGCUpdate = gcClients(clientID, dagStore);
      return latestGCUpdate;
    },
    GC_INTERVAL_MS,
    lc,
  );
}

export async function gcClients(
  clientID: ClientID,
  dagStore: dag.Store,
): Promise<ClientMap> {
  return updateClients(clients => {
    const now = Date.now();
    const clientsAfterGC = Array.from(clients).filter(
      ([id, client]) =>
        id === clientID /* never collect ourself */ ||
        now - client.heartbeatTimestampMs <= CLIENT_MAX_INACTIVE_IN_MS,
    );
    if (clientsAfterGC.length === clients.size) {
      return noUpdates;
    }
    return {
      clients: new Map(clientsAfterGC),
    };
  }, dagStore);
}
