import type {LogContext} from '@rocicorp/logger';
import type {NullableVersion, Patch, Version} from 'reflect-protocol';
import type {DisconnectHandler} from '../server/disconnect.js';
import {EntryCache} from '../storage/entry-cache.js';
import {unwrapPatch} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {getVersion} from '../types/version.js';
import {assert} from 'shared';
import {must} from 'shared';
import {processDisconnects} from './process-disconnects.js';
import {MutatorMap, processMutation} from './process-mutation.js';

// Processes zero or more mutations as a single "frame", returning pokes.
// Pokes are returned if the version changes, even if there is no patch,
// because we need clients to be in sync with server version so that pokes
// can continue to apply.
export async function processFrame(
  lc: LogContext,
  pendingMutations: PendingMutation[],
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
  for (const pendingMutation of pendingMutations) {
    count++;
    const mutationCache = new EntryCache(cache);
    const newLastMutationID = await processMutation(
      lc,
      pendingMutation,
      mutators,
      mutationCache,
      nextVersion,
    );
    const version = must(await getVersion(mutationCache));
    assert(
      (version !== prevVersion) === (newLastMutationID !== undefined),
      'version should be updated iff the mutation was applied',
    );
    // If mutation was applied, build client pokes for it.
    if (version !== prevVersion && newLastMutationID !== undefined) {
      const patch = unwrapPatch(mutationCache.pending());
      await mutationCache.flush();
      const mutationClientID = pendingMutation.clientID;
      const mutationClientGroupID = pendingMutation.clientGroupID;
      clientPokes.push(
        ...(await buildClientPokesAndUpdateClientRecords(
          cache,
          clients,
          patch,
          prevVersion,
          nextVersion,
          clientRecord =>
            clientRecord.clientGroupID === mutationClientGroupID
              ? {[mutationClientID]: newLastMutationID}
              : {},
          pendingMutation.timestamp,
        )),
      );
      prevVersion = nextVersion;
      nextVersion = prevVersion + 1;
    } else {
      // If mutation was not applied, still flush any changes made by
      // processMutation.
      await mutationCache.flush();
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
  // If processDisconnects updated version it successfully executed
  // disconnectHandler for one or more disconnected clients, create client
  // pokes for the resulting user value changes.
  if (must(await getVersion(disconnectsCache)) !== prevVersion) {
    const patch = unwrapPatch(disconnectsCache.pending());
    await disconnectsCache.flush();
    clientPokes.push(
      ...(await buildClientPokesAndUpdateClientRecords(
        cache,
        clients,
        patch,
        prevVersion,
        nextVersion,
        () => ({}),
        undefined,
      )),
    );
  } else {
    // Wether or not processDisconnects updated version, flush any other changes
    // it made.
    await disconnectsCache.flush();
  }
  lc.debug?.('built pokes', clientPokes.length);
  await cache.flush();
  return clientPokes;
}

function buildClientPokesAndUpdateClientRecords(
  cache: Storage,
  clients: ClientID[],
  patch: Patch,
  prevVersion: NullableVersion,
  nextVersion: Version,
  getLastMutationIDChanges: (
    clientRecord: ClientRecord,
  ) => Record<string, number>,
  timestamp: number | undefined,
): Promise<ClientPoke[]> {
  return Promise.all(
    clients.map(async clientID => {
      const clientRecord = must(await getClientRecord(clientID, cache));
      const updatedClientRecord: ClientRecord = {
        ...clientRecord,
        baseCookie: nextVersion,
      };
      await putClientRecord(clientID, updatedClientRecord, cache);
      const clientPoke: ClientPoke = {
        clientID,
        poke: {
          baseCookie: prevVersion,
          cookie: nextVersion,
          lastMutationIDChanges: await getLastMutationIDChanges(clientRecord),
          patch,
          timestamp,
        },
      };
      return clientPoke;
    }),
  );
}
