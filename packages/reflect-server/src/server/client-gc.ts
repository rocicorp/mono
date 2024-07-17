import type {LogContext} from '@rocicorp/logger';
import type {ClientID, Env} from 'reflect-shared/src/mod.js';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {difference} from 'shared/src/set-utils.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {Storage} from '../storage/storage.js';
import {
  ClientRecord,
  ClientRecordMap,
  IncludeDeleted,
  deleteClientRecord,
  deleteClientRecords,
  getClientRecord,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import {userValueKey, userValueSchema} from '../types/user-value.js';
import {putVersion} from '../types/version.js';
import {
  callClientDeleteHandler,
  type ClientDeleteHandler,
} from './client-delete-handler.js';

/**
 * Thw frequency at which we run the client GC. This is used to not do gc in
 * every processFrame.
 */
export const CLIENT_GC_FREQUENCY = 10 * 1000;

// 2 weeks
export const GC_MAX_AGE = 2 * 7 * 24 * 60 * 60 * 1000;

function clientGCSpaceUserKey(clientID: string): string {
  return `-/p/${clientID}`;
}

export async function updateLastSeenForClient(
  lc: LogContext,
  clientID: ClientID,
  storage: Storage,
  now: number,
): Promise<void> {
  const clientRecord = await getClientRecord(
    clientID,
    IncludeDeleted.Exclude,
    storage,
  );
  if (!clientRecord) {
    lc.debug?.(`Not updating lastSeen for removed client ${clientID}`);
    return;
  }
  await putClientRecord(
    clientID,
    {
      ...clientRecord,
      lastSeen: now,
    },
    storage,
  );
}

export function updateLastSeen(
  lc: LogContext,
  oldClients: Set<ClientID>,
  newClients: Set<ClientID>,
  storage: Storage,
  now: number,
) {
  // the clients that are only in old clients are the ones that disconnected
  // the clients that are only in new clients are the ones that connected
  // We update the lastSeen for all disconnected clients.
  const ps: Promise<unknown>[] = [];
  for (const clientID of difference(oldClients, newClients)) {
    ps.push(updateLastSeenForClient(lc, clientID, storage, now));
  }
  return Promise.all(ps);
}

export async function collectClients(
  lc: LogContext,
  env: Env,
  storage: Storage,
  clientDeleteHandler: ClientDeleteHandler,
  connectedClients: Set<ClientID>,
  now: number,
  maxAge: number,
  nextVersion: number,
): Promise<void> {
  const clientRecords = await listClientRecords(
    IncludeDeleted.Exclude,
    storage,
  );
  const clientsToCollect = await findClientsToCollect(
    connectedClients,
    clientRecords,
    storage,
    now,
    maxAge,
  );

  lc.debug?.(
    'connected clients count:',
    connectedClients.size,
    'clientsToCollect',
    clientsToCollect,
  );

  if (clientsToCollect.size > 0) {
    for (const clientID of clientsToCollect.keys()) {
      await callClientDeleteHandler(
        lc,
        clientID,
        env,
        clientDeleteHandler,
        nextVersion,
        storage,
      );
    }

    await collectOldUserSpaceClientKeys(
      lc,
      storage,
      clientsToCollect.keys(),
      nextVersion,
    );
    await deleteClientRecords(clientsToCollect, storage);

    await putVersion(nextVersion, storage);
  }
}

/**
 * Finds clients to collect. These are the clients that are older than maxAge.
 * It also updates the last seen timestamp for all connected clients.
 */
async function findClientsToCollect(
  connectedClients: Set<ClientID>,
  clientRecords: ClientRecordMap,
  storage: Storage,
  now: number,
  maxAge: number,
): Promise<Map<ClientID, ClientRecord>> {
  const clientsToCollect: Map<ClientID, ClientRecord> = new Map();
  for (const [clientID, clientRecord] of clientRecords) {
    const {lastSeen} = clientRecord;
    if (lastSeen === undefined) {
      // Fixup old client records that do not have lastSeen.
      await putClientRecord(
        clientID,
        {
          ...clientRecord,
          lastSeen: now,
        },
        storage,
      );
    } else if (!connectedClients.has(clientID) && lastSeen + maxAge <= now) {
      // Too old. We should collect it.
      clientsToCollect.set(clientID, clientRecord);
    }
  }
  return clientsToCollect;
}

export async function collectOldUserSpaceClientKeys(
  lc: LogContext,
  storage: Storage,
  clientsToCollect: Iterable<ClientID>,
  nextVersion: number,
): Promise<void> {
  // Delete all the keys starting with '-/p/${clientID}' for all old clients.
  const ps: Promise<unknown>[] = [];
  for (const clientID of clientsToCollect) {
    for await (const [key, {value, deleted}] of storage.scan(
      {prefix: userValueKey(clientGCSpaceUserKey(clientID))},
      userValueSchema,
    )) {
      // Make sure we do not update the version if already deleted.
      if (!deleted) {
        ps.push(
          storage.put(key, {
            deleted: true,
            value,
            version: nextVersion,
          }),
        );
      }
    }
  }
  await Promise.all(ps);
  lc.debug?.(`Deleted ${ps.length} old client keys`);
}

export async function collectClientIfDeleted(
  lc: LogContext,
  env: Env,
  clientID: ClientID,
  clientDeleteHandler: ClientDeleteHandler,
  storage: Storage,
  nextVersion: number,
): Promise<void> {
  const clientRecord = must(
    await getClientRecord(clientID, IncludeDeleted.Include, storage),
  );
  const {lastMutationID, lastMutationIDAtClose} = clientRecord;
  if (lastMutationIDAtClose === undefined) {
    lc.debug?.(
      `Client ${clientID} has no lastMutationIDAtClose. Not collecting.`,
    );
    return;
  }

  lc.debug?.('Maybe collecting client', {
    clientID,
    lastMutationID,
    lastMutationIDAtClose,
  });

  if (lastMutationID < lastMutationIDAtClose) {
    // This means that the client still has pending clients that we haven't seen
    // yet. We need to keep the client alive.
    lc.debug?.(
      `Client has lastMutationID < lastMutationIDAtClose. Not collecting.`,
    );

    // TODO(arv): Collect client when the lastMutationIDAtClose gets pushed using mutation recovery.

    return;
  }

  if (lastMutationID > lastMutationIDAtClose) {
    // This means that we received a mutation after the client closed. This can
    // happen if we have a race between closing the client and sending a
    // mutation at close.
    //
    // It is safe to collect the client here because these mutations have been
    // applied and the client is not connected anymore.
    //
    // It is not possible for the client to have persisted pending mutations
    // during "close" because persist is async and writes to IDB which does not
    // block unloading.
    lc.debug?.(
      `Client applied mutations after close beacon was sent/received. Collecting.`,
    );
  } else {
    assert(lastMutationID === lastMutationIDAtClose);
    lc.debug?.(`Client and server are fully synced. Collecting.`);
  }

  if (clientRecord.deleted) {
    lc.debug?.(
      `Client ${clientID} is already deleted. Not calling delete handler again.`,
    );
  } else {
    await callClientDeleteHandler(
      lc,
      clientID,
      env,
      clientDeleteHandler,
      nextVersion,
      storage,
    );
  }

  const cache = new EntryCache(storage);
  await collectOldUserSpaceClientKeys(lc, cache, [clientID], nextVersion);

  await deleteClientRecord(clientID, clientRecord, cache);
  await cache.flush();
}
