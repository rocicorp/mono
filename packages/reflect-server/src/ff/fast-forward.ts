import type {NullableVersion, Patch, Poke, Version} from 'reflect-protocol';
import {must} from 'shared/src/must.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {listClientRecords} from '../types/client-record.js';
import type {ClientGroupID, ClientID} from '../types/client-state.js';
import {compareVersions} from '../types/version.js';
import {getPatch} from './get-patch.js';
import type {LogContext} from '@rocicorp/logger';

/**
 * Returns zero or more pokes necessary to fast forward any clients in a room
 * that are behind head.
 * @param clients clients active in room
 * @param currentVersion head version to fast-forward to
 * @param durable storage to read/write to
 */
export async function fastForwardRoom(
  lc: LogContext,
  clients: ClientID[],
  currentVersion: Version,
  storage: DurableStorage,
): Promise<ClientPoke[]> {
  const clientRecords = await listClientRecords(storage);
  // Get all of the distinct base cookies. Typically almost all active clients
  // of a room will have same base cookie. No need to recalculate over and over.
  const distinctBaseCookies = new Set<NullableVersion>();
  for (const clientID of clients) {
    const record = must(
      clientRecords.get(clientID),
      `Client record not found: ${clientID}`,
    );
    distinctBaseCookies.add(record.baseCookie);
  }
  // No need to calculate a patch for the current version!
  distinctBaseCookies.delete(currentVersion);
  lc.debug?.(
    `Computing patches for ${distinctBaseCookies.size} cookies of ${clients.length} connected clients (${clientRecords.size} clientRecords)`,
  );

  // Calculate all the distinct patches in parallel
  const getPatchEntry = async (baseCookie: NullableVersion) =>
    [baseCookie, await getPatch(storage, baseCookie)] as [
      NullableVersion,
      Patch,
    ];
  const distinctPatches = new Map(
    await Promise.all([...distinctBaseCookies].map(getPatchEntry)),
  );
  lc.debug?.(`Finished computing patches`);

  // Calculate the last mutation id changes for each
  // (base cookie, client group id) combination
  const lastMutationIDChangesByBaseCookieByClientGroupID: Map<
    ClientGroupID,
    Map<NullableVersion, Record<ClientID, number>>
  > = new Map();
  for (const [clientID, record] of clientRecords) {
    if (record.lastMutationIDVersion !== null) {
      const {clientGroupID} = record;
      let changesByBaseCookie =
        lastMutationIDChangesByBaseCookieByClientGroupID.get(clientGroupID);
      if (changesByBaseCookie === undefined) {
        changesByBaseCookie = new Map();
        lastMutationIDChangesByBaseCookieByClientGroupID.set(
          clientGroupID,
          changesByBaseCookie,
        );
      }
      for (const baseCookie of distinctBaseCookies) {
        if (compareVersions(baseCookie, record.lastMutationIDVersion) < 0) {
          let changes = changesByBaseCookie.get(baseCookie);
          if (changes === undefined) {
            changes = {};
            changesByBaseCookie.set(baseCookie, changes);
          }
          changes[clientID] = record.lastMutationID;
        }
      }
    }
  }

  let largestPatch = 0;
  const ret: ClientPoke[] = [];
  for (const clientID of clients) {
    const record = must(clientRecords.get(clientID));
    if (record.baseCookie === currentVersion) {
      continue;
    }
    const patch = must(distinctPatches.get(record.baseCookie));
    largestPatch = Math.max(largestPatch, patch.length);

    const {clientGroupID} = record;
    const poke: Poke = {
      baseCookie: record.baseCookie,
      cookie: currentVersion,
      lastMutationIDChanges:
        lastMutationIDChangesByBaseCookieByClientGroupID
          .get(clientGroupID)
          ?.get(record.baseCookie) ?? {},
      patch,
      // apply immediately
      timestamp: undefined,
    };
    const clientPoke: ClientPoke = {
      clientID,
      poke,
    };
    ret.push(clientPoke);
  }

  lc.debug?.(`Largest patch: ${largestPatch} ops`);
  return ret;
}
