import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod.js';
import {initBgIntervalProcess} from '../bg-interval.js';
import {
  ClientGroupMap,
  getClientGroups,
  setClientGroups,
  clientGroupHasPendingMutations,
} from './client-groups.js';
import {assertClientV5, getClients} from './clients.js';
import {withWrite} from '../with-transactions.js';

const GC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientGroupMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientGroupMap> | undefined {
  return latestGCUpdate;
}

export function initClientGroupGC(
  dagStore: dag.Store,
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGroupGC',
    () => {
      latestGCUpdate = gcClientGroups(dagStore);
      return latestGCUpdate;
    },
    () => GC_INTERVAL_MS,
    lc,
    signal,
  );
}

export function gcClientGroups(dagStore: dag.Store): Promise<ClientGroupMap> {
  return withWrite(dagStore, async tx => {
    const clients = await getClients(tx);
    const clientGroupIDs = new Set();
    for (const client of clients.values()) {
      assertClientV5(client);
      clientGroupIDs.add(client.clientGroupID);
    }
    const clientGroups = new Map();
    for (const [clientGroupID, clientGroup] of await getClientGroups(tx)) {
      if (
        clientGroupIDs.has(clientGroupID) ||
        clientGroupHasPendingMutations(clientGroup)
      ) {
        clientGroups.set(clientGroupID, clientGroup);
      }
    }
    await setClientGroups(clientGroups, tx);
    await tx.commit();
    return clientGroups;
  });
}
