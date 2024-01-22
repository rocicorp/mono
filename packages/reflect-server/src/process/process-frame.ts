import type {LogContext} from '@rocicorp/logger';
import type {NullableVersion, Patch, Version} from 'reflect-protocol';
import type {Env} from 'reflect-shared/src/types.js';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {GC_MAX_AGE, collectClients} from '../server/client-gc.js';
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
import type {ClientID, ClientMap} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {getVersion} from '../types/version.js';
import {processDisconnects} from './process-disconnects.js';
import {MutatorMap, processMutation} from './process-mutation.js';

const EMPTY_PRESENCE: Patch = [];

// Processes zero or more mutations as a single "frame", returning pokes.
// Pokes are returned if the version changes, even if there is no patch,
// because we need clients to be in sync with server version so that pokes
// can continue to apply.
export async function processFrame(
  lc: LogContext,
  env: Env,
  pendingMutations: PendingMutation[],
  numPendingMutationsToProcess: number,
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  clients: ClientMap,
  storage: Storage,
  shouldGCClients: (now: number) => boolean,
): Promise<ClientPoke[]> {
  lc.debug?.('processing frame - clients', clients);
  const clientIDs = [...clients.keys()];

  const cache = new EntryCache(storage);
  const startVersion = must(await getVersion(cache));
  let prevVersion = startVersion;
  let nextVersion = (prevVersion ?? 0) + 1;

  lc.debug?.('prevVersion', prevVersion, 'nextVersion', nextVersion);
  const clientPokes: ClientPoke[] = [];
  for (let i = 0; i < numPendingMutationsToProcess; i++) {
    const pendingMutation = pendingMutations[i];
    const mutationCache = new EntryCache(cache);
    const newLastMutationID = await processMutation(
      lc,
      env,
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
          clientIDs,
          clients,
          patch,
          prevVersion,
          nextVersion,
          clientRecord =>
            clientRecord.clientGroupID === mutationClientGroupID
              ? {[mutationClientID]: newLastMutationID}
              : {},
          pendingMutation.timestamps,
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

  lc.debug?.(`processed ${numPendingMutationsToProcess} mutations`);

  const now = Date.now();
  if (shouldGCClients(now)) {
    const gcCache = new EntryCache(cache);
    await collectClients(
      lc,
      gcCache,
      new Set(clientIDs),
      now,
      GC_MAX_AGE,
      nextVersion,
    );

    // If collectClients updated version it successfully collected clients and
    // client keys. Create client pokes for the resulting user value changes.
    [prevVersion, nextVersion] = await addPokesIfUpdated(
      gcCache,
      prevVersion,
      clientPokes,
      clientIDs,
      clients,
      nextVersion,
    );
    // Wether or not the version was updated, flush any other changes it made.
    await gcCache.flush();
  }

  const disconnectsCache = new EntryCache(cache);
  await processDisconnects(
    lc,
    env,
    disconnectHandler,
    clientIDs,
    pendingMutations,
    numPendingMutationsToProcess,
    disconnectsCache,
    nextVersion,
  );
  // If processDisconnects updated version it successfully executed
  // disconnectHandler for one or more disconnected clients, create client
  // pokes for the resulting user value changes.
  await addPokesIfUpdated(
    disconnectsCache,
    prevVersion,
    clientPokes,
    clientIDs,
    clients,
    nextVersion,
  );
  // Wether or not the version was updated, flush any other changes it made.
  await disconnectsCache.flush();

  lc.debug?.('built pokes', clientPokes.length);
  await cache.flush();
  return clientPokes;
}

async function addPokesIfUpdated(
  cache: EntryCache,
  prevVersion: number,
  clientPokes: ClientPoke[],
  clientIDs: string[],
  clients: ClientMap,
  nextVersion: number,
) {
  if (must(await getVersion(cache)) !== prevVersion) {
    const patch = unwrapPatch(cache.pending());
    clientPokes.push(
      ...(await buildClientPokesAndUpdateClientRecords(
        cache,
        clientIDs,
        clients,
        patch,
        prevVersion,
        nextVersion,
        () => ({}),
        undefined,
      )),
    );
    prevVersion = nextVersion;
    nextVersion = prevVersion + 1;
  }

  return [prevVersion, nextVersion];
}

function buildClientPokesAndUpdateClientRecords(
  cache: Storage,
  clientIDs: ClientID[],
  clients: ClientMap,
  patch: Patch,
  prevVersion: NullableVersion,
  nextVersion: Version,
  getLastMutationIDChanges: (
    clientRecord: ClientRecord,
  ) => Record<string, number>,
  timestamps:
    | {
        normalizedTimestamp: number;
        originTimestamp: number;
        serverReceivedTimestamp: number;
      }
    | undefined,
): Promise<ClientPoke[]> {
  const now = Date.now();
  return Promise.all(
    clientIDs.map(async clientID => {
      const clientRecord = must(await getClientRecord(clientID, cache));
      const client = must(clients.get(clientID));
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
          lastMutationIDChanges: getLastMutationIDChanges(clientRecord),
          presence: EMPTY_PRESENCE,
          patch,
          timestamp: timestamps?.normalizedTimestamp,
          debugOriginTimestamp: client.debugPerf
            ? timestamps?.originTimestamp
            : undefined,
          debugServerReceivedTimestamp: client.debugPerf
            ? timestamps?.serverReceivedTimestamp
            : undefined,
          debugServerSentTimestamp: client.debugPerf ? now : undefined,
        },
      };
      return clientPoke;
    }),
  );
}
