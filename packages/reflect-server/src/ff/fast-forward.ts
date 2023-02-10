import type {ClientRecord} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import type {NullableVersion, Version} from '../types/version.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
import {getPatch} from './get-patch.js';
import type {Patch} from '../protocol/poke.js';
import {must} from '../util/must.js';
import type {DurableStorage} from '../storage/durable-storage.js';

export type GetClientRecord = (clientID: ClientID) => Promise<ClientRecord>;

/**
 * Returns zero or more pokes necessary to fast forward any clients in a room
 * that are behind head.
 * @param clients clients active in room
 * @param getClientRecord function to get a client record by ID
 * @param currentVersion head version to fast-forward to
 * @param durable storage to read/write to
 * @param timestamp for resulting pokes
 */
export async function fastForwardRoom(
  clients: ClientID[],
  getClientRecord: GetClientRecord,
  currentVersion: Version,
  storage: DurableStorage,
  timestamp: number,
): Promise<ClientPokeBody[]> {
  // Load all the client records in parallel
  const getMapEntry = async (clientID: ClientID) =>
    [clientID, await getClientRecord(clientID)] as [ClientID, ClientRecord];
  const records = new Map(await Promise.all(clients.map(getMapEntry)));

  // Get all of the distinct base cookies. Typically almost all members of
  // room will have same base cookie. No need to recalculate over and over.
  const distinctBaseCookies = new Set(
    [...records.values()].map(r => r.baseCookie),
  );

  // No need to calculate a patch for the current version!
  distinctBaseCookies.delete(currentVersion);

  // Calculate all the distinct patches in parallel
  const getPatchEntry = async (baseCookie: NullableVersion) =>
    [baseCookie, await getPatch(storage, baseCookie ?? 0)] as [
      NullableVersion,
      Patch,
    ];
  const distinctPatches = new Map(
    await Promise.all([...distinctBaseCookies].map(getPatchEntry)),
  );

  const ret: ClientPokeBody[] = [];
  for (const clientID of clients) {
    const record = must(records.get(clientID));
    if (record.baseCookie === currentVersion) {
      continue;
    }
    const patch = must(distinctPatches.get(record.baseCookie));
    const poke: ClientPokeBody = {
      clientID,
      poke: {
        baseCookie: record.baseCookie,
        cookie: currentVersion,
        lastMutationID: record.lastMutationID,
        timestamp,
        patch,
      },
    };
    ret.push(poke);
  }

  return ret;
}
