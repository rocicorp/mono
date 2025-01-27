import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.ts';
import type {Store} from '../dag/store.ts';
import type {ClientID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {
  type ClientMap,
  ClientStateNotFoundError,
  getClients,
  setClients,
} from './clients.ts';

export const HEARTBEAT_INTERVAL = 60 * 1000;

export let latestHeartbeatUpdate: Promise<ClientMap> | undefined;

export function startHeartbeats(
  clientID: ClientID,
  dagStore: Store,
  onClientStateNotFound: () => void,
  heartbeatIntervalMs: number,
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'Heartbeat',
    async () => {
      latestHeartbeatUpdate = writeHeartbeat(clientID, dagStore);
      try {
        return await latestHeartbeatUpdate;
      } catch (e) {
        if (e instanceof ClientStateNotFoundError) {
          onClientStateNotFound();
          return;
        }
        throw e;
      }
    },
    () => heartbeatIntervalMs,
    lc,
    signal,
  );
}

export function writeHeartbeat(
  clientID: ClientID,
  dagStore: Store,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const clients = await getClients(dagWrite);
    const client = clients.get(clientID);
    if (!client) {
      throw new ClientStateNotFoundError(clientID);
    }

    const newClient = {
      ...client,
      heartbeatTimestampMs: Date.now(),
    };
    const newClients = new Map(clients).set(clientID, newClient);

    await setClients(newClients, dagWrite);
    return newClients;
  });
}
