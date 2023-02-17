// Processes zero or more mutations against a room, returning necessary pokes

import type {LogContext} from '@rocicorp/logger';
import type {DisconnectHandler} from '../server/disconnect.js';
import {fastForwardRoom} from '../ff/fast-forward.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientMap} from '../types/client-state.js';
import {getVersion, putVersion} from '../types/version.js';
import {must} from '../util/must.js';
import {generateMergedMutations} from './generate-merged-mutations.js';
import {processFrame} from './process-frame.js';
import type {MutatorMap} from './process-mutation.js';
import type {PendingMutationMap} from '../types/mutation.js';

export const FRAME_LENGTH_MS = 1000 / 60;

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
  pendingMutations: PendingMutationMap,
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  storage: DurableStorage,
  timestamp: number,
): Promise<ClientPokeBody[]> {
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
  const pokes: ClientPokeBody[] = await fastForwardRoom(
    clientIDs,
    currentVersion,
    storage,
    timestamp,
  );
  lc.debug?.('pokes from fastforward', JSON.stringify(pokes));

  for (const poke of pokes) {
    const cr = must(
      await getClientRecord(poke.clientID, cache),
      `Client record not found: ${poke.clientID}`,
    );
    cr.baseCookie = poke.poke.cookie;
    await putClientRecord(poke.clientID, cr, cache);
  }

  const mergedMutations = generateMergedMutations(pendingMutations);

  pokes.push(
    ...(await processFrame(
      lc,
      mergedMutations,
      mutators,
      disconnectHandler,
      clientIDs,
      cache,
      timestamp,
    )),
  );

  await cache.flush();
  return pokes;
}
