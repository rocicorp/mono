import * as v from '../../shared/src/valita.ts';
import type {Read, Write} from './dag/store.ts';
import {deepFreeze} from './frozen-json.ts';
import type {ClientID} from './sync/ids.ts';

/**
 * We keep track of deleted clients in the {@linkcode DELETED_CLIENTS_HEAD_NAME}
 * head.
 */
export const DELETED_CLIENTS_HEAD_NAME = 'deleted-clients';

export async function setDeletedClients(
  dagWrite: Write,
  deletedClients: ClientID[],
): Promise<ClientID[]> {
  // sort and dedupe
  const normalized = normalize(deletedClients);
  const chunkData = deepFreeze(normalized);
  const chunk = dagWrite.createChunk(chunkData, []);
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(DELETED_CLIENTS_HEAD_NAME, chunk.hash);
  return normalized;
}

const deletedClientsSchema = v.array(v.string());

export async function getDeletedClients(dagRead: Read): Promise<ClientID[]> {
  const hash = await dagRead.getHead(DELETED_CLIENTS_HEAD_NAME);
  if (hash === undefined) {
    return [];
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
): Promise<ClientID[]> {
  const deletedClients = await getDeletedClients(dagWrite);
  return setDeletedClients(dagWrite, [...deletedClients, ...clientIDs]);
}

export async function removeDeletedClients(
  dagWrite: Write,
  clientIDs: ClientID[],
): Promise<ClientID[]> {
  const deletedClients = await getDeletedClients(dagWrite);
  const newDeletedClients = deletedClients.filter(
    clientID => !clientIDs.includes(clientID),
  );
  return setDeletedClients(dagWrite, newDeletedClients);
}

/**
 * Sorts and dedupes the given array.
 */
export function normalize<T>(arr: T[]): T[] {
  return [...new Set(arr)].sort();
}
