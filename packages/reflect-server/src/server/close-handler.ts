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
 * A `CloseHandler` can modify room state in response to a client closing from
 * the room. A client gets "closed" when it can no longer reconnect. These
 * changes will be synced to all clients of the room just like mutator changes.
 * `write.clientID` will be the id of the disconnected client.
 * `write.mutationID` will be -1.
 */
export type CloseHandler = (write: WriteTransaction) => Promise<void>;

export async function callCloseHandler(
  lc: LogContext,
  clientID: string,
  env: Env,
  closeHandler: CloseHandler,
  nextVersion: number,
  storage: Storage,
): Promise<void> {
  lc.debug?.('Executing closeHandler for:', clientID);
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
    await closeHandler(tx);
    if (cache.isDirty()) {
      await putVersion(nextVersion, cache);
      await cache.flush();
    }
  } catch (e) {
    lc.error?.('Error executing closeHandler for:', clientID);
    // We let the caller continue...
  }
}
