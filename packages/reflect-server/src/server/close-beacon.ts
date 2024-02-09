import type {LogContext} from '@rocicorp/logger';
import type {Env} from 'reflect-shared/src/types.js';
import {must} from 'shared/src/must.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {Storage} from '../storage/storage.js';
import {
  delClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import {getConnectedClients} from '../types/connected-clients.js';
import {getVersion, putVersion} from '../types/version.js';
import {
  callClientDeleteHandler,
  type ClientDeleteHandler,
} from './client-delete-handler.js';
import {collectOldUserSpaceClientKeys} from './client-gc.js';

export async function closeBeacon(
  lc: LogContext,
  env: Env,
  clientID: string,
  roomID: string,
  userID: string,
  lastMutationID: number,
  clientDeleteHandler: ClientDeleteHandler,
  storage: Storage,
): Promise<Response> {
  lc.debug?.(
    'close client beacon request',
    roomID,
    userID,
    clientID,
    lastMutationID,
  );

  // Get the last mutationID for the client.
  const existingRecord = await getClientRecord(clientID, storage);
  if (!existingRecord) {
    lc.debug?.('Client record not found');
    return new Response('client record not found', {status: 404});
  }

  const storedConnectedClients = await getConnectedClients(storage);
  if (storedConnectedClients.has(clientID)) {
    await putClientRecord(
      clientID,
      {
        ...existingRecord,
        lastMutationIDAtClose: lastMutationID,
      },
      storage,
    );
    lc.debug?.(
      'Client is still connected. Will try to clean it in client disconnect handler.',
    );
    return new Response('Client is still connected', {
      status: 200,
    });
  }

  if (lastMutationID < existingRecord.lastMutationID) {
    lc.debug?.(
      'Client sent an older mutationID than we have. This should not happen.',
    );
    return new Response('Client mutationID is less than existing', {
      status: 500,
    });
  }

  if (lastMutationID > existingRecord.lastMutationID) {
    lc.debug?.('Client sent a newer mutationID than we have');
    return new Response('Client has pending mutations', {
      status: 200,
    });
  }

  lc.debug?.(
    `Client is at the same mutationID as we have. Deleting client ${clientID}`,
  );

  const cache = new EntryCache(storage);
  const startVersion = must(await getVersion(cache));
  const nextVersion = startVersion + 1;

  await callClientDeleteHandler(
    lc,
    clientID,
    env,
    clientDeleteHandler,
    nextVersion,
    cache,
  );

  // Use a second cache so that we only update the version if we actually
  // deleted any presence keys.
  const innerCache = new EntryCache(cache);
  await collectOldUserSpaceClientKeys(lc, innerCache, [clientID], nextVersion);

  if (innerCache.isDirty()) {
    await putVersion(nextVersion, innerCache);
    await innerCache.flush();
  }

  await delClientRecord(clientID, cache);
  await cache.flush();

  return new Response('ok');
}
