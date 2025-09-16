/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {mustGetHeadHash, type Read} from './dag/store.ts';
import {DEFAULT_HEAD_NAME, localMutationsDD31} from './db/commit.ts';
import type {ClientID} from './sync/ids.ts';

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
