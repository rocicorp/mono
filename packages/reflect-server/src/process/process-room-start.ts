import type {LogContext} from '@rocicorp/logger';
import type {RoomStartHandler} from '../server/room-start.js';
import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {getVersion, putVersion} from '../types/version.js';

// Processes the roomStartHandler. Errors in starting the room are logged
// and thrown for the caller to handle appropriately (i.e. consider the room
// to be in an invalid state).
export async function processRoomStart(
  lc: LogContext,
  roomStartHandler: RoomStartHandler,
  storage: Storage,
): Promise<void> {
  lc.debug?.('processing room start');

  // Note: Internal schema migrations will go here, before
  // invoking the app-specified RoomStartHandler.

  const cache = new EntryCache(storage);
  const startVersion = (await getVersion(cache)) ?? 0;
  const nextVersion = startVersion + 1;

  const tx = new ReplicacheTransaction(
    cache,
    '', // clientID,
    -1, // mutationID,
    startVersion,
  );
  try {
    await roomStartHandler(tx);
    if (!cache.isDirty()) {
      lc.debug?.('noop roomStartHandler');
      return;
    }
    await putVersion(nextVersion, cache);
    await cache.flush();
    lc.debug?.(`finished roomStartHandler (${startVersion} => ${nextVersion})`);
  } catch (e) {
    lc.info?.('roomStartHandler failed', e);
    throw e;
  }
}
