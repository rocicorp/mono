import type {LogContext} from '@rocicorp/logger';
import type {Mutation, PokeBody} from 'reflect-protocol';
import type {DisconnectHandler} from '../server/disconnect.js';
import {EntryCache} from '../storage/entry-cache.js';
import {unwrapPatch} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientGroupID, ClientID} from '../types/client-state.js';
import {getVersion} from '../types/version.js';
import {must} from '../util/must.js';
import {randomID} from '../util/rand.js';
import {processDisconnects} from './process-disconnects.js';
import {MutatorMap, processMutation} from './process-mutation.js';

// Processes zero or more mutations as a single "frame", returning pokes.
// Pokes are returned if the version changes, even if there is no patch,
// because we need clients to be in sync with server version so that pokes
// can continue to apply.
export async function processFrame(
  lc: LogContext,
  mutations: Iterable<Mutation>,
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  clients: ClientID[],
  storage: Storage,
  timestamp: number,
): Promise<ClientPokeBody[]> {
  lc.debug?.('processing frame - clients', clients);

  const cache = new EntryCache(storage);
  const prevVersion = must(await getVersion(cache));
  const nextVersion = (prevVersion ?? 0) + 1;

  lc.debug?.('prevVersion', prevVersion, 'nextVersion', nextVersion);

  const lastMutationIDChangesByClientGroupID: Map<
    ClientGroupID,
    Record<ClientID, number>
  > = new Map();
  let count = 0;
  for (const mutation of mutations) {
    count++;
    const newLastMutationID = await processMutation(
      lc,
      mutation,
      mutators,
      cache,
      nextVersion,
    );
    if (newLastMutationID !== undefined) {
      const {clientID} = mutation;
      const clientRecord = must(
        await getClientRecord(clientID, cache),
        `Client record not found: ${clientID}`,
      );
      const {clientGroupID} = clientRecord;
      let changes = lastMutationIDChangesByClientGroupID.get(clientGroupID);
      if (changes === undefined) {
        changes = {};
        lastMutationIDChangesByClientGroupID.set(clientGroupID, changes);
      }
      changes[clientID] = newLastMutationID;
    }
  }

  lc.debug?.(`processed ${count} mutations`);

  await processDisconnects(lc, disconnectHandler, clients, cache, nextVersion);

  // If version has not changed, then there should not be any patch or pokes to
  // send. But processDisconnects still makes other changes to cache that need
  // to be flushed.
  if (must(await getVersion(cache)) === prevVersion) {
    await cache.flush();
    return [];
  }

  const ret: ClientPokeBody[] = [];
  const patch = unwrapPatch(cache.pending());
  for (const clientID of clients) {
    const clientRecord = must(
      await getClientRecord(clientID, cache),
      `Client record not found: ${clientID}`,
    );
    clientRecord.baseCookie = nextVersion;
    await putClientRecord(clientID, clientRecord, cache);
    const {clientGroupID} = clientRecord;
    const pokeBody: PokeBody = {
      baseCookie: prevVersion,
      cookie: nextVersion,
      lastMutationIDChanges:
        lastMutationIDChangesByClientGroupID.get(clientGroupID) ?? {},
      patch,
      timestamp,
      requestID: randomID(),
    };
    const poke: ClientPokeBody = {
      clientID,
      poke: pokeBody,
    };
    ret.push(poke);
  }
  lc.debug?.('built poke bodies', ret.length);
  await cache.flush();
  return ret;
}
