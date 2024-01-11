import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {mustGetHeadHash, Read} from './dag/store.js';
import {DEFAULT_HEAD_NAME, localMutationsDD31} from './db/commit.js';
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
): Promise<readonly PendingMutation[]> {
  const mainHeadHash = await mustGetHeadHash(DEFAULT_HEAD_NAME, dagRead);
  const pending = await localMutationsDD31(mainHeadHash, dagRead);
  return pending
    .map(p => ({
      id: p.meta.mutationID,
      name: p.meta.mutatorName,
      args: p.meta.mutatorArgsJSON,
      clientID: p.meta.clientID,
    }))
    .reverse();
}
