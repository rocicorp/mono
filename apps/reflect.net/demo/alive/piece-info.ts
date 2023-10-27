import type {ReadTransaction} from '@rocicorp/reflect';
import {getClient} from './client-model';
import {PieceModel, listPieces} from './piece-model';

// Synced model + derived/runtime state.
export type PieceInfo = PieceModel & {
  // The client currently selecting this piece, if any.
  selector: string | null;
};

export async function getPieceInfos(
  tx: ReadTransaction,
  presentClientIDs: ReadonlySet<string>,
): Promise<Record<string, PieceInfo>> {
  const lp = await listPieces(tx);
  const mp: Record<string, PieceInfo> = {};
  for (const piece of lp) {
    mp[piece.id] = {
      ...piece,
      selector: null,
    };
  }
  const presentClients = [];
  for (const presentClientID of presentClientIDs) {
    const presentClient = await getClient(tx, presentClientID);
    if (presentClient) {
      presentClients.push(presentClient);
    }
  }
  for (const presentClient of presentClients) {
    if (presentClient.selectedPieceID) {
      mp[presentClient.selectedPieceID].selector = presentClient.id;
    }
  }
  return mp;
}
