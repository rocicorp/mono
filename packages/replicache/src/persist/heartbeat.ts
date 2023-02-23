import type {LogContext} from '@rocicorp/logger';
import type {ClientID} from '../sync/ids.js';
import type * as dag from '../dag/mod.js';
import {
  ClientMap,
  ClientStateNotFoundError,
  getClients,
  isClientSDD,
  setClients,
} from './clients.js';
import {initBgIntervalProcess} from '../bg-interval.js';
import {withWrite} from '../with-transactions.js';

const HEARTBEAT_INTERVAL_MS = 60 * 1000;

export let latestHeartbeatUpdate: Promise<ClientMap> | undefined;

export function startHeartbeats(
  clientID: ClientID,
  dagStore: dag.Store,
  onClientStateNotFound: () => void,
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
    () => HEARTBEAT_INTERVAL_MS,
    lc,
    signal,
  );
}

export function writeHeartbeat(
  clientID: ClientID,
  dagStore: dag.Store,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    const clients = await getClients(dagWrite);
    const client = clients.get(clientID);
    if (!client) {
      throw new ClientStateNotFoundError(clientID);
    }

    const newClients = new Map(clients).set(
      clientID,
      isClientSDD(client)
        ? {
            heartbeatTimestampMs: Date.now(),
            headHash: client.headHash,
            mutationID: client.mutationID,
            lastServerAckdMutationID: client.lastServerAckdMutationID,
          }
        : {
            heartbeatTimestampMs: Date.now(),
            headHash: client.headHash,
            clientGroupID: client.clientGroupID,
            tempRefreshHash: client.tempRefreshHash,
          },
    );

    await setClients(newClients, dagWrite);
    await dagWrite.commit();
    return newClients;
  });
}
