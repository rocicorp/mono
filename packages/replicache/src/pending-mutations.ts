import type {Read} from './dag/store.js';
import {
  assertLocalMetaDD31,
  Commit,
  LocalMeta,
  localMutationsDD31,
} from './db/commit.js';
import type {Hash} from './hash.js';
import type {ReadonlyJSONValue} from './json.js';
import type {ClientID} from './sync/ids.js';

export type PendingMutation = {
  readonly name: string;
  readonly id: number;
  readonly args: ReadonlyJSONValue;
  readonly clientID: ClientID;
};

/**
 * This returns the pending changes with the oldest mutations first.
 */
export async function pendingMutationsForAPI(
  dagRead: Read,
  hash: Hash,
): Promise<readonly PendingMutation[]> {
  const pending = await localMutationsDD31(hash, dagRead);
  return pending
    .map(p => ({
      id: p.meta.mutationID,
      name: p.meta.mutatorName,
      args: p.meta.mutatorArgsJSON,
      clientID: p.meta.clientID,
    }))
    .reverse();
}

function convertLocalMetaCommitToPendingMutationAPI(
  commit: Commit<LocalMeta>,
): PendingMutation {
  const {meta} = commit;
  assertLocalMetaDD31(meta);
  return {
    id: meta.mutationID,
    name: meta.mutatorName,
    args: meta.mutatorArgsJSON,
    clientID: meta.clientID,
  };
}

export function convertLocalMetaCommitsToPendingMutationsAPI(
  commits: Commit<LocalMeta>[],
) {
  return commits.map(convertLocalMetaCommitToPendingMutationAPI).reverse();
}
