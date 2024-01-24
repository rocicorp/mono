import type {LogContext} from '@rocicorp/logger';
import {must} from 'shared/src/must.js';
import {EntryCache} from '../storage/entry-cache.js';
import type {Storage} from '../storage/storage.js';
import {delClientRecords, getClientRecord} from '../types/client-record.js';
import {getVersion, putVersion} from '../types/version.js';
import {collectOldUserSpaceClientKeys} from './client-gc.js';

export async function closeBeacon(
  lc: LogContext,
  clientID: string,
  roomID: string,
  userID: string,
  lastMutationID: number,
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

  await delClientRecords([clientID], cache);

  // Use a second cache so that we only update the version if we actually
  // deleted any presence keys.
  const innerCache = new EntryCache(cache);
  await collectOldUserSpaceClientKeys(lc, innerCache, [clientID], nextVersion);

  if (innerCache.isDirty()) {
    await putVersion(nextVersion, innerCache);
    await innerCache.flush();
  }

  await cache.flush();

  return new Response('ok');
}
