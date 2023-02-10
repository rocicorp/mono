// Processes zero or more mutations against a room, returning necessary pokes

import { fastForwardRoom } from "../ff/fast-forward";
import { DurableStorage } from "../storage/durable-storage";
import { EntryCache } from "../storage/entry-cache";
import { ClientPokeBody } from "../types/client-poke-body";
import { getClientRecord, putClientRecord } from "../types/client-record";
import { ClientID, ClientMap } from "../types/client-state";
import { getVersion, putVersion } from "../types/version";
import { LogContext } from "../util/logger";
import { must } from "../util/must";
import { PeekIterator } from "../util/peek-iterator";
import { generateMergedMutations } from "./generate-merged-mutations";
import { processFrame } from "./process-frame";
import { MutatorMap } from "./process-mutation";

export const FRAME_LENGTH_MS = 1000 / 60;

/**
 * Process all pending mutations that are ready to be processed for a room.
 * @param roomID room to process mutations for
 * @param clients active clients in the room
 * @param mutators all known mutators
 * @param executor database executor
 * @returns
 */
export async function processRoom(
  lc: LogContext,
  clients: ClientMap,
  mutators: MutatorMap,
  durable: DurableObjectStorage
): Promise<ClientPokeBody[]> {
  const storage = new DurableStorage(durable);
  const cache = new EntryCache(storage);

  // TODO: can/should we pass `clients` to fastForward instead?
  const clientIDs = [...clients.keys()];
  lc.debug?.("processing room", "clientIDs", clientIDs);

  // Before running any mutations, fast forward connected clients to
  // current state.
  const gcr = async (clientID: ClientID) =>
    must(
      await getClientRecord(clientID, cache),
      `Client record not found: ${clientID}`
    );
  let currentVersion = await getVersion(cache);
  if (currentVersion === undefined) {
    currentVersion = 0;
    await putVersion(currentVersion, cache);
  }
  lc.debug?.("currentVersion", currentVersion);

  const pokes: ClientPokeBody[] = await fastForwardRoom(
    clientIDs,
    gcr,
    currentVersion,
    durable,
    Date.now()
  );
  lc.debug?.("pokes from fastforward", JSON.stringify(pokes));

  for (const poke of pokes) {
    const cr = must(await getClientRecord(poke.clientID, cache));
    cr.baseCookie = poke.poke.cookie;
    await putClientRecord(poke.clientID, cr, cache);
  }

  const mergedMutations = new PeekIterator(generateMergedMutations(clients));
  pokes.push(
    ...(await processFrame(lc, mergedMutations, mutators, clientIDs, cache))
  );

  await cache.flush();
  return pokes;
}
