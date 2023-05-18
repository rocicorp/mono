import type {LogContext} from '@rocicorp/logger';
import type {RoomStartHandler} from '../server/room-start.js';
import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {getVersion, putVersion} from '../types/version.js';
import {
  versionIndexMetaKey,
  versionIndexMetaSchema,
  versionIndexSchemaVersion,
  VersionIndexMeta,
} from '../types/version-index.js';

async function initVersionIndex(
  lc: LogContext,
  storage: Storage,
): Promise<void> {
  const current = await storage.get(
    versionIndexMetaKey,
    versionIndexMetaSchema,
  );
  if (current?.schemaVersion === versionIndexSchemaVersion) {
    return;
  }
  lc.info?.(
    `updating version index from to v${current?.schemaVersion} to v${versionIndexSchemaVersion}`,
  );
  const cache = new EntryCache(storage);
  // The initial v0 version index is a null index; however, `{ schemaVersion: 0 }` must be
  // tracked so that if code is rolled back from v1 to v0 and then to v1, the v1
  // migration code will be run again.
  await cache.put<VersionIndexMeta>(versionIndexMetaKey, {
    schemaVersion: versionIndexSchemaVersion,
  });
  await cache.flush();
}

// Processes the roomStartHandler. Errors in starting the room are logged
// and thrown for the caller to handle appropriately (i.e. consider the room
// to be in an invalid state).
export async function processRoomStart(
  lc: LogContext,
  roomStartHandler: RoomStartHandler,
  storage: Storage,
): Promise<void> {
  lc.debug?.('processing room start');

  // Internal schema initializations / migrations receive a raw (uncached)
  // Storage reference so that they can incrementally persist (i.e. `flush()`)
  // changes if necessary.
  await initVersionIndex(lc, storage);

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
