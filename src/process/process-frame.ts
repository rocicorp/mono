import type {LogContext} from '@rocicorp/logger';
import type {DisconnectHandler} from '../server/disconnect.js';
import {EntryCache} from '../storage/entry-cache.js';
import {unwrapPatch} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import type {ClientMutation} from '../types/client-mutation.js';
import type {ClientPoke} from '../types/client-poke.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {ClientID} from '../types/client-state.js';
import {getVersion} from '../types/version.js';
import {must} from '../util/must.js';
import {processDisconnects} from './process-disconnects.js';
import {MutatorMap, processMutation} from './process-mutation.js';

// Processes zero or more mutations as a single "frame", returning pokes.
// Pokes are returned if the version changes, even if there is no patch,
// because we need clients to be in sync with server version so that pokes
// can continue to apply.
export async function processFrame(
  lc: LogContext,
  mutations: Iterable<ClientMutation>,
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  clients: ClientID[],
  storage: Storage,
  timestamp: number,
): Promise<ClientPoke[]> {
  lc.debug?.('processing frame - clients', clients);

  const cache = new EntryCache(storage);
  let prevVersion = must(await getVersion(cache));
  let nextVersion = (prevVersion ?? 0) + 1;

  lc.debug?.('prevVersion', prevVersion, 'nextVersion', nextVersion);

  const ret: ClientPoke[] = [];
  for (const mutation of mutations) {
    const mutationCache = new EntryCache(cache);
    await processMutation(lc, mutation, mutators, mutationCache, nextVersion);
    // If version has not changed, then there should not be any patch or pokes to
    // send. But processDisconnects still makes other changes to cache that need
    // to be flushed.
    if (must(await getVersion(mutationCache)) !== prevVersion) {
      const patch = unwrapPatch(mutationCache.pending());
      await mutationCache.flush();
      for (const clientID of clients) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const clientRecord = (await getClientRecord(clientID, cache))!;
        clientRecord.baseCookie = nextVersion;
        await putClientRecord(clientID, clientRecord, cache);
        const poke: ClientPoke = {
          clientID,
          poke: {
            baseCookie: prevVersion,
            cookie: nextVersion,
            lastMutationID: clientRecord.lastMutationID,
            patch,
            clientID: mutation.clientID,
            timestamp: mutation.timestamp,
            unixTimestamp: mutation.old ? undefined : mutation.unixTimestamp,
          },
        };
        ret.push(poke);
      }
      prevVersion = nextVersion;
      nextVersion = prevVersion + 1;
    }
  }

  await processDisconnects(lc, disconnectHandler, clients, cache, nextVersion);
  if (must(await getVersion(cache)) !== prevVersion) {
    const patch = unwrapPatch(cache.pending());
    for (const clientID of clients) {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const clientRecord = (await getClientRecord(clientID, cache))!;
      clientRecord.baseCookie = nextVersion;
      await putClientRecord(clientID, clientRecord, cache);

      const poke: ClientPoke = {
        clientID,
        poke: {
          baseCookie: prevVersion,
          cookie: nextVersion,
          lastMutationID: clientRecord.lastMutationID,
          patch,
          timestamp,
        },
      };
      ret.push(poke);
    }
  }

  await cache.flush();
  return ret;
}
