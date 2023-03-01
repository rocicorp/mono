import type {LogContext} from '@rocicorp/logger';
import type {Mutation, Poke} from 'reflect-protocol';
import type {DisconnectHandler} from '../server/disconnect.js';
import {EntryCache} from '../storage/entry-cache.js';
import {unwrapPatch} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import {getVersion} from '../types/version.js';
import {assert} from '../util/asserts.js';
import {must} from '../util/must.js';
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
): Promise<ClientPoke[]> {
  lc.debug?.('processing frame - clients', clients);

  const cache = new EntryCache(storage);
  const startVersion = must(await getVersion(cache));
  let prevVersion = startVersion;
  let nextVersion = (prevVersion ?? 0) + 1;

  lc.debug?.('prevVersion', prevVersion, 'nextVersion', nextVersion);
  let count = 0;
  const clientPokes: ClientPoke[] = [];
  for (const mutation of mutations) {
    count++;
    const mutationCache = new EntryCache(cache);
    const newLastMutationID = await processMutation(
      lc,
      mutation,
      mutators,
      mutationCache,
      nextVersion,
    );
    const version = must(await getVersion(mutationCache));
    assert(
      (version !== prevVersion) === (newLastMutationID !== undefined),
      'version should be updated iff the mutation was applied',
    );
    if (version !== prevVersion && newLastMutationID !== undefined) {
      const patch = unwrapPatch(mutationCache.pending());
      await mutationCache.flush();
      const mutationClientID = mutation.clientID;
      const mutationClientGroupID = must(
        await getClientRecord(mutationClientID, cache),
      ).clientGroupID;
      for (const clientID of clients) {
        const clientRecord = must(await getClientRecord(clientID, cache));
        clientRecord.baseCookie = nextVersion;
        await putClientRecord(clientID, clientRecord, cache);
        const clientPoke: ClientPoke = {
          clientID,
          poke: {
            baseCookie: prevVersion,
            cookie: nextVersion,
            lastMutationIDChanges:
              clientRecord.clientGroupID === mutationClientGroupID
                ? {[mutationClientID]: newLastMutationID}
                : {},
            patch,
            timestamp: mutation.timestamp,
          },
        };
        clientPokes.push(clientPoke);
      }
      prevVersion = nextVersion;
      nextVersion = prevVersion + 1;
    }
  }

  lc.debug?.(`processed ${count} mutations`);

  const disconnectsCache = new EntryCache(cache);
  await processDisconnects(
    lc,
    disconnectHandler,
    clients,
    disconnectsCache,
    nextVersion,
  );
  if (must(await getVersion(disconnectsCache)) !== prevVersion) {
    const patch = unwrapPatch(disconnectsCache.pending());
    for (const clientID of clients) {
      const clientRecord = must(
        await getClientRecord(clientID, cache),
        `Client record not found: ${clientID}`,
      );
      clientRecord.baseCookie = nextVersion;
      await putClientRecord(clientID, clientRecord, cache);
      const poke: Poke = {
        baseCookie: prevVersion,
        cookie: nextVersion,
        lastMutationIDChanges: {},
        patch,
        timestamp: undefined,
      };
      const clientPoke: ClientPoke = {
        clientID,
        poke,
      };
      clientPokes.push(clientPoke);
    }
  }

  lc.debug?.('built pokes', clientPokes.length);
  await disconnectsCache.flush();
  await cache.flush();
  return clientPokes;
}
