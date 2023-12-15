import type {LogContext} from '@rocicorp/logger';
import type {ClientID} from 'reflect-shared';
import {assert} from 'shared/src/asserts.js';
import {difference} from 'shared/src/set-utils.js';
import type {Storage} from '../storage/storage.js';
import {
  ClientRecordMap,
  delClientRecords,
  getClientRecord,
  listClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import {userValueKey, userValueSchema} from '../types/user-value.js';
import {putVersion} from '../types/version.js';

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

async function updateLastSeenForClient(
  clientID: ClientID,
  storage: Storage,
  now: number,
): Promise<void> {
  const clientRecord = await getClientRecord(clientID, storage);
  assert(clientRecord);
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
    ps.push(updateLastSeenForClient(clientID, storage, now));
  }
  return Promise.all(ps);
}

export async function collectClients(
  lc: LogContext,
  storage: Storage,
  connectedClients: Set<ClientID>,
  now: number,
  maxAge: number,
  nextVersion: number,
): Promise<void> {
  const clientRecords = await listClientRecords(storage);
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

  if (clientsToCollect.length > 0) {
    await delClientRecords(clientsToCollect, storage);
    await collectOldUserSpaceClientKeys(
      lc,
      storage,
      clientsToCollect,
      nextVersion,
    );

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
): Promise<string[]> {
  const clientsToCollect: ClientID[] = [];
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
      clientsToCollect.push(clientID);
    }
  }
  return clientsToCollect;
}

export async function collectOldUserSpaceClientKeys(
  lc: LogContext,
  storage: Storage,
  clientsToCollect: string[],
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
