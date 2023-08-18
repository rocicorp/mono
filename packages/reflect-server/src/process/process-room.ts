// Processes zero or more mutations against a room, returning necessary pokes

import type {LogContext} from '@rocicorp/logger';
import {must} from 'shared/src/must.js';
import {fastForwardRoom} from '../ff/fast-forward.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {ClientPoke} from '../types/client-poke.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientMap} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {getVersion, putVersion} from '../types/version.js';
import {processFrame} from './process-frame.js';
import type {MutatorMap} from './process-mutation.js';

export const FRAME_LENGTH_MS = 1000 / 60;
const FLUSH_SIZE_THRESHOLD_FOR_LOG_FLUSH = 500;

/**
 * Process all pending mutations that are ready to be processed for a room.
 * @param clients active clients in the room
 * @param clientGroups client groups with pending mutations
 * @param mutators all known mutators
 * @param durable storage to read/write to
 * @param timestamp timestamp to put in resulting pokes
 */
export async function processRoom(
  lc: LogContext,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  storage: DurableStorage,
): Promise<ClientPoke[]> {
  const cache = new EntryCache(storage);
  const clientIDs = [...clients.keys()];
  lc.debug?.(
    'processing room',
    'clientIDs',
    [...clientIDs.entries()],
    ' pendingMutations',
    [...pendingMutations.entries()],
  );

  // Before running any mutations, fast forward connected clients to
  // current state.
  let currentVersion = await getVersion(cache);
  if (currentVersion === undefined) {
    currentVersion = 0;
    await putVersion(currentVersion, cache);
  }
  lc.debug?.('currentVersion', currentVersion);
  const clientPokes: ClientPoke[] = await fastForwardRoom(
    lc,
    clientIDs,
    currentVersion,
    storage,
  );
  lc.debug?.(
    'clients with pokes from fastforward',
    clientPokes.map(clientPoke => clientPoke.clientID),
  );

  for (const ffClientPoke of clientPokes) {
    const cr = must(
      await getClientRecord(ffClientPoke.clientID, cache),
      `Client record not found: ${ffClientPoke.clientID}`,
    );
    cr.baseCookie = ffClientPoke.poke.cookie;
    await putClientRecord(ffClientPoke.clientID, cr, cache);
  }

  clientPokes.push(
    ...(await processFrame(
      lc,
      pendingMutations,
      mutators,
      disconnectHandler,
      clients,
      cache,
    )),
  );

  const startCacheFlush = Date.now();
  const pendingCounts = cache.pendingCounts();
  lc = lc.withContext('cacheFlushDelCount', pendingCounts.delCount);
  lc = lc.withContext('cacheFlushPutCount', pendingCounts.putCount);
  lc.debug?.('Starting cache flush.', pendingCounts);
  // In case this "large" flush causes the DO to be reset because of:
  // "Durable Object storage operation exceeded timeout which caused object to
  // be reset", flush the logs for debugging.
  if (
    pendingCounts.delCount + pendingCounts.putCount >
    FLUSH_SIZE_THRESHOLD_FOR_LOG_FLUSH
  ) {
    lc.info?.('Starting large cache flush.', pendingCounts);
    void lc.flush();
  }
  await cache.flush();
  const cacheFlushLatencyMs = Date.now() - startCacheFlush;
  lc = lc.withContext('cacheFlushTiming', cacheFlushLatencyMs);
  lc.info?.(
    `Finished cache flush in ${cacheFlushLatencyMs} ms.`,
    pendingCounts,
  );
  return clientPokes;
}
