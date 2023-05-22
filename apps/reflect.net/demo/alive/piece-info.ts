import type {ReadTransaction} from '@rocicorp/reflect';
import {ClientModel, listClients} from './client-model';
import {listPieces, PieceModel} from './piece-model';

// Synced model + derived/runtime state.
export type PieceInfo = PieceModel & {
  // The client currently selecting this piece, if any.
  selector: string | null;
};

export async function getPieceInfos(
  tx: ReadTransaction,
): Promise<Record<string, PieceInfo>> {
  const lp = await listPieces(tx);
  const mp: Record<string, PieceInfo> = {};
  for (const piece of lp) {
    mp[piece.id] = {
      ...piece,
      selector: null,
    };
  }
  const lc = await listClients(tx);
  const mc: Record<string, ClientModel> = {};
  for (const client of lc) {
    mc[client.id] = client;
    if (client.selectedPieceID) {
      mp[client.selectedPieceID].selector = client.id;
    }
  }
  return mp;
}
