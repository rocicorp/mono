import type {LogContext} from '@rocicorp/logger';
import {assert} from '../asserts';
import type * as dag from '../dag/mod';
import {initBgIntervalProcess} from '../bg-interval';
import {
  BranchMap,
  getBranches,
  setBranches,
  branchHasPendingMutations,
} from './branches';
import {assertClientDD31, getClients} from './clients';

const GC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let latestGCUpdate: Promise<BranchMap> | undefined;
export function getLatestGCUpdate(): Promise<BranchMap> | undefined {
  return latestGCUpdate;
}

export function initBranchGC(
  dagStore: dag.Store,
  lc: LogContext,
  signal: AbortSignal,
): void {
  assert(DD31);
  initBgIntervalProcess(
    'BranchGC',
    () => {
      latestGCUpdate = gcBranches(dagStore);
      return latestGCUpdate;
    },
    GC_INTERVAL_MS,
    lc,
    signal,
  );
}

export async function gcBranches(dagStore: dag.Store): Promise<BranchMap> {
  return await dagStore.withWrite(async tx => {
    const clients = await getClients(tx);
    const clientBranchIds = new Set();
    for (const client of clients.values()) {
      assertClientDD31(client);
      clientBranchIds.add(client.branchID);
    }
    const branches = new Map();
    for (const [branchID, branch] of await getBranches(tx)) {
      if (clientBranchIds.has(branchID) || branchHasPendingMutations(branch)) {
        branches.set(branchID, branch);
      }
    }
    await setBranches(branches, tx);
    await tx.commit();
    return branches;
  });
}
