import type {LogContext} from '@rocicorp/logger';
import type {Env, WriteTransaction} from 'reflect-shared/src/types.js';
import {EntryCache} from '../storage/entry-cache.js';
import {
  NOOP_MUTATION_ID,
  ReplicacheTransaction,
} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {putVersion} from '../types/version.js';

/**
 * A `ClientDeleteHandler` can modify room state in response to a client being
 * garbage collected from the room. A client gets "deleted" when it can no
 * longer reconnect. These changes will be synced to all clients of the room
 * just like mutator changes. `write.clientID` will be the id of the deleted
 * client. `write.mutationID` will be -1.
 *
 * This is run before the presence keys for `clientID` are deleted and before
 * other client state is removed.
 */
export type ClientDeleteHandler = (write: WriteTransaction) => Promise<void>;

export async function callClientDeleteHandler(
  lc: LogContext,
  clientID: string,
  env: Env,
  clientDeleteHandler: ClientDeleteHandler,
  nextVersion: number,
  storage: Storage,
): Promise<void> {
  lc.debug?.('Executing clientDeleteHandler for:', clientID);
  const cache = new EntryCache(storage);
  const tx = new ReplicacheTransaction(
    cache,
    clientID,
    NOOP_MUTATION_ID,
    nextVersion,
    undefined,
    env,
  );
  try {
    await clientDeleteHandler(tx);
    if (cache.isDirty()) {
      await putVersion(nextVersion, cache);
      await cache.flush();
    }
  } catch (e) {
    lc.error?.('Error executing clientDeleteHandler for:', clientID);
    // We let the caller continue...
  }
}
