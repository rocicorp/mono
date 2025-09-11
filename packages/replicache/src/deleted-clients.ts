import {stringCompare} from '../../shared/src/string-compare.ts';
import * as v from '../../shared/src/valita.ts';
import type {Read, Write} from './dag/store.ts';
import {deepFreeze} from './frozen-json.ts';
import {getClients, setClients} from './persist/clients.ts';
import {
  clientGroupIDSchema,
  clientIDSchema,
  type ClientGroupID,
  type ClientID,
} from './sync/ids.ts';

/**
 * We keep track of deleted clients in the {@linkcode DELETED_CLIENTS_HEAD_NAME}
 * head.
 */
export const DELETED_CLIENTS_HEAD_NAME = 'deleted-clients-v2';

type ClientIDPair = {
  clientGroupID: ClientGroupID;
  clientID: ClientID;
};

export type DeletedClients = readonly Readonly<ClientIDPair>[];

export type WritableDeletedClients = ClientIDPair[];

export const deletedClientsSchema: v.Type<DeletedClients> = v.readonlyArray(
  v.readonlyObject({
    clientGroupID: clientGroupIDSchema,
    clientID: clientIDSchema,
  }),
);

function compare(a: ClientIDPair, b: ClientIDPair): number {
  const cg = stringCompare(a.clientGroupID, b.clientGroupID);
  if (cg !== 0) {
    return cg;
  }
  return stringCompare(a.clientID, b.clientID);
}

export function normalizeDeletedClients(
  deletedClients: DeletedClients,
): DeletedClients {
  // dedupe
  return [...deletedClients]
    .sort(compare)
    .filter(
      (item, index) =>
        index === 0 ||
        compare(item, [...deletedClients].sort(compare)[index - 1]) !== 0,
    );
}

export function mergeDeletedClients(
  a: DeletedClients,
  b: DeletedClients,
): DeletedClients {
  const merged: WritableDeletedClients = [];
  a = normalizeDeletedClients(a);
  b = normalizeDeletedClients(b);
  for (let i = 0, j = 0; i < a.length || j < b.length; ) {
    if (i < a.length && (j >= b.length || compare(a[i], b[j]) < 0)) {
      merged.push(a[i]);
      i++;
    } else if (j < b.length && (i >= a.length || compare(b[j], a[i]) < 0)) {
      merged.push(b[j]);
      j++;
    } else {
      // equal
      merged.push(a[i]);
      i++;
      j++;
    }
  }
  return merged;
}

export function removeFromDeletedClients(
  old: DeletedClients,
  toRemove: DeletedClients,
): DeletedClients {
  old = normalizeDeletedClients(old);
  toRemove = normalizeDeletedClients(toRemove);
  const result: WritableDeletedClients = [];
  for (let i = 0, j = 0; i < old.length; ) {
    if (j >= toRemove.length || compare(old[i], toRemove[j]) < 0) {
      result.push(old[i]);
      i++;
    } else if (j < toRemove.length && compare(old[i], toRemove[j]) === 0) {
      // equal, skip
      i++;
      j++;
    } else {
      // old[i] > toRemove[j]
      j++;
    }
  }
  return result;
}

export async function setDeletedClients(
  dagWrite: Write,
  deletedClients: DeletedClients,
): Promise<DeletedClients> {
  // sort and dedupe

  const data = normalizeDeletedClients(deletedClients);

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
    return [];
  }
  const chunk = await dagRead.mustGetChunk(hash);

  const res = v.test(chunk.data, deletedClientsSchema);
  if (!res.ok) {
    // If not ok then we ignore this. It might be in the old format but we do
    // not know the clientGroupID of the old clients.
    return [];
  }

  return res.value;
}

/**
 * Adds deleted clients to the {@linkcode DELETED_CLIENTS_HEAD_NAME} head.
 * @returns the new list of deleted clients (sorted and deduped).
 */
export async function addDeletedClients(
  dagWrite: Write,
  deletedClientsToAdd: DeletedClients,
): Promise<DeletedClients> {
  const oldDeletedClients = await getDeletedClients(dagWrite);

  return setDeletedClients(
    dagWrite,
    mergeDeletedClients(oldDeletedClients, deletedClientsToAdd),
  );
}

export async function removeDeletedClients(
  dagWrite: Write,
  deletedClientsToRemove: DeletedClients,
): Promise<DeletedClients> {
  const oldDeletedClients = await getDeletedClients(dagWrite);
  return setDeletedClients(
    dagWrite,
    removeFromDeletedClients(oldDeletedClients, deletedClientsToRemove),
  );
}

export async function confirmDeletedClients(
  dagWrite: Write,
  deletedClientIds: readonly ClientID[],
  deletedClientGroupIds: readonly ClientGroupID[],
): Promise<DeletedClients> {
  const deletedClientIDSet = new Set(deletedClientIds);
  const deletedClientGroupIDSet = new Set(deletedClientGroupIds);
  const oldDeletedClients = await getDeletedClients(dagWrite);
  const clients = new Map(await getClients(dagWrite));
  for (const clientID of deletedClientIds) {
    clients.delete(clientID);
  }
  for (const clientGroupID of deletedClientGroupIds) {
    for (const [clientID, client] of clients) {
      if (client.clientGroupID === clientGroupID) {
        clients.delete(clientID);
      }
    }
  }

  await setClients(clients, dagWrite);

  return setDeletedClients(
    dagWrite,
    oldDeletedClients.filter(
      ({clientGroupID, clientID}) =>
        !deletedClientGroupIDSet.has(clientGroupID) &&
        !deletedClientIDSet.has(clientID),
    ),
  );
}

/**
 * Sorts and dedupes the given array.
 */
export function normalize<T>(arr: readonly T[]): T[] {
  return [...new Set(arr)].sort();
}
