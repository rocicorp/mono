import type {Patch, Poke} from 'reflect-protocol';
import {must} from 'shared/src/must.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {Storage} from '../storage/storage.js';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientID, ClientMap} from '../types/client-state.js';
import {getVersion, putVersion} from '../types/version.js';

export async function addPresence(
  clients: ClientMap,
  pokesByClientID: Map<ClientID, Poke[]>,
  storage: Storage,
  previousConnectedClients: Set<string>,
  nextConnectedClients: Set<string>,
): Promise<void> {
  let numClientsThatNeedInitialPresence = 0;
  let needPokeForInitialPresence = false;
  for (const [clientID, clientState] of clients) {
    if (!clientState.sentInitialPresence) {
      numClientsThatNeedInitialPresence++;
      if (!pokesByClientID.has(clientID)) {
        needPokeForInitialPresence = true;
      }
    }
  }

  const incrementalPresencePatch: Patch = [];
  if (numClientsThatNeedInitialPresence !== clients.size) {
    for (const clientID of nextConnectedClients) {
      if (!previousConnectedClients.has(clientID)) {
        incrementalPresencePatch.push({
          op: 'put',
          key: clientID,
          value: 1,
        });
      }
    }
    for (const clientID of previousConnectedClients) {
      if (!nextConnectedClients.has(clientID)) {
        incrementalPresencePatch.push({
          op: 'del',
          key: clientID,
        });
      }
    }
  }

  let needPokeForIncrementalPresence = false;
  if (incrementalPresencePatch.length > 0) {
    for (const clientID of clients.keys()) {
      if (!pokesByClientID.has(clientID)) {
        needPokeForIncrementalPresence = true;
      }
    }
  }

  if (needPokeForInitialPresence || needPokeForIncrementalPresence) {
    const cache = new EntryCache(storage);
    const prevVersion = must(await getVersion(cache));
    const nextVersion = prevVersion + 1;
    await putVersion(nextVersion, cache);
    const poke = {
      baseCookie: prevVersion,
      cookie: nextVersion,
      lastMutationIDChanges: {},
      presence: [],
      patch: [],
    };
    for (const clientID of clients.keys()) {
      const clientRecord = must(await getClientRecord(clientID, cache));
      const updatedClientRecord: ClientRecord = {
        ...clientRecord,
        baseCookie: nextVersion,
      };
      await putClientRecord(clientID, updatedClientRecord, cache);
      let pokes = pokesByClientID.get(clientID);
      if (!pokes) {
        pokes = [];
        pokesByClientID.set(clientID, pokes);
      }
      pokes.push(poke);
    }
    await cache.flush();
  }

  const initialPresencePatch: Patch = [];
  if (numClientsThatNeedInitialPresence > 0) {
    initialPresencePatch.push({op: 'clear'});
    for (const clientID of nextConnectedClients) {
      initialPresencePatch.push({op: 'put', key: clientID, value: 1});
    }
  }

  for (const [clientID, clientState] of clients) {
    if (
      clientState.sentInitialPresence &&
      incrementalPresencePatch.length === 0
    ) {
      continue;
    }
    const pokes = must(pokesByClientID.get(clientID));
    const presence = clientState.sentInitialPresence
      ? incrementalPresencePatch
      : initialPresencePatch;
    pokes[0] = {...pokes[0], presence};
  }
}
