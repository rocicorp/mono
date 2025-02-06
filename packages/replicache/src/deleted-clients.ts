import * as v from '../../shared/src/valita.ts';
import type {Read, Write} from './dag/store.ts';
import {deepFreeze} from './frozen-json.ts';
import type {Hash} from './hash.ts';
import type {ClientID} from './sync/ids.ts';

export const DELETED_CLIENTS_HEAD_NAME = 'deleted-clients';

export async function setDeletedClients(
  dagWrite: Write,
  deletedClients: ClientID[],
): Promise<Hash> {
  const chunkData = deepFreeze(deletedClients);
  const chunk = dagWrite.createChunk(chunkData, []);
  await dagWrite.putChunk(chunk);
  await dagWrite.setHead(DELETED_CLIENTS_HEAD_NAME, chunk.hash);
  return chunk.hash;
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
