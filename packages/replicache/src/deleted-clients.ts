import * as v from '../../shared/src/valita.ts';
import type {Read, Write} from './dag/store.ts';
import {deepFreeze} from './frozen-json.ts';
import type {ClientGroupID, ClientID} from './sync/ids.ts';

/**
 * We keep track of deleted clients in the {@linkcode DELETED_CLIENTS_HEAD_NAME}
 * head.
 */
export const DELETED_CLIENTS_HEAD_NAME = 'deleted-clients';

export const deletedClientsSchema = v.readonlyObject({
  clientIDs: v.readonlyArray(v.string()),
  clientGroupIDs: v.readonlyArray(v.string()),
});

export type DeletedClients = v.Infer<typeof deletedClientsSchema>;

export async function setDeletedClients(
  dagWrite: Write,
  clientIDs: readonly ClientID[],
  clientGroupIDs: readonly ClientGroupID[],
): Promise<DeletedClients> {
  // sort and dedupe

  const data = {
    clientIDs: normalize(clientIDs),
    clientGroupIDs: normalize(clientGroupIDs),
  };
  const chunkData = deepFreeze(data);
  const chunk = dagWrite.createChunk(chunkData, []);
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(DELETED_CLIENTS_HEAD_NAME, chunk.hash);
  return data;
}

export async function getDeletedClients(
  dagRead: Read,
): Promise<DeletedClients> {
  const hash = await dagRead.getHead(DELETED_CLIENTS_HEAD_NAME);
  if (hash === undefined) {
    return {clientIDs: [], clientGroupIDs: []};
  }
  const chunk = await dagRead.mustGetChunk(hash);
  return v.parse(chunk.data, deletedClientsSchema);
}

/**
 * Adds deleted clients to the {@linkcode DELETED_CLIENTS_HEAD_NAME} head.
 * @returns the new list of deleted clients (sorted and deduped).
 */
export async function addDeletedClients(
  dagWrite: Write,
  clientIDs: ClientID[],
  clientGroupIDs: ClientGroupID[],
): Promise<DeletedClients> {
  const {clientIDs: oldClientIDs, clientGroupIDs: oldClientGroupIDs} =
    await getDeletedClients(dagWrite);

  return setDeletedClients(
    dagWrite,
    [...oldClientIDs, ...clientIDs],
    [...oldClientGroupIDs, ...clientGroupIDs],
  );
}

export async function removeDeletedClients(
  dagWrite: Write,
  clientIDs: readonly ClientID[],
  clientGroupIDs: readonly ClientGroupID[],
): Promise<DeletedClients> {
  const {clientIDs: oldClientIDs, clientGroupIDs: oldClientGroupIDs} =
    await getDeletedClients(dagWrite);
  const newDeletedClients = oldClientIDs.filter(
    clientID => !clientIDs.includes(clientID),
  );
  const newDeletedClientGroups = oldClientGroupIDs.filter(
    clientGroupID => !clientGroupIDs.includes(clientGroupID),
  );
  return setDeletedClients(dagWrite, newDeletedClients, newDeletedClientGroups);
}

/**
 * Sorts and dedupes the given array.
 */
export function normalize<T>(arr: readonly T[]): T[] {
  return [...new Set(arr)].sort();
}
