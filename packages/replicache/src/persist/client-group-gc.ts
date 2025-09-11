import type {LogContext} from '@rocicorp/logger';
import {initBgIntervalProcess} from '../bg-interval.ts';
import type {Store} from '../dag/store.ts';
import type {ClientGroupID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {
  clientGroupHasPendingMutations,
  getClientGroups,
  setClientGroups,
  type ClientGroupMap,
} from './client-groups.ts';
import {getClients} from './clients.ts';

const GC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<ClientGroupMap> | undefined;
export function getLatestGCUpdate(): Promise<ClientGroupMap> | undefined {
  return latestGCUpdate;
}

export function initClientGroupGC(
  dagStore: Store,
  enableMutationRecovery: boolean,
  lc: LogContext,
  signal: AbortSignal,
): void {
  initBgIntervalProcess(
    'ClientGroupGC',
    () => {
      latestGCUpdate = gcClientGroups(dagStore, enableMutationRecovery);
      return latestGCUpdate;
    },
    () => GC_INTERVAL_MS,
    lc,
    signal,
  );
}

/**
 * This removes client groups that have no clients and no pending mutations.
 * If {@linkcode enableMutationRecovery} is true, it will keep client groups with
 * pending mutations. If it is false, it will remove client groups even when they
 * have pending mutations.
 */
export function gcClientGroups(
  dagStore: Store,
  enableMutationRecovery: boolean,
): Promise<ClientGroupMap> {
  return withWrite(dagStore, async tx => {
    const clients = await getClients(tx);
    const clientGroupIDs = new Set();
    for (const client of clients.values()) {
      clientGroupIDs.add(client.clientGroupID);
    }
    const clientGroups = new Map();
    const removeClientGroups: Set<ClientGroupID> = new Set();
    for (const [clientGroupID, clientGroup] of await getClientGroups(tx)) {
      if (
        clientGroupIDs.has(clientGroupID) ||
        (enableMutationRecovery && clientGroupHasPendingMutations(clientGroup))
      ) {
        clientGroups.set(clientGroupID, clientGroup);
      } else {
        removeClientGroups.add(clientGroupID);
      }
    }
    await setClientGroups(clientGroups, tx);
    // Client group GC doesn't delete individual clients, so we don't call onClientsDeleted
    return clientGroups;
  });
}
