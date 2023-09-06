import {entitySchema, generate, Update} from '@rocicorp/rails';
import type {WriteTransaction} from '@rocicorp/reflect';
import {z} from 'zod';

// Note, we use the stringified index inside PIECE_DEFINITIONS (aka the "piece num") as the ID.
export const pieceModelSchema = entitySchema.extend({
  x: z.number(),
  y: z.number(),
  rotation: z.number(),
  placed: z.boolean(),
  handleRotation: z.number(),
});

// Export generated interface.
export type PieceModel = z.infer<typeof pieceModelSchema>;
export type PieceModelUpdate = Update<PieceModel>;

const pieceRailsMethod = generate('piece', pieceModelSchema);

export const {
  put: putPiece,
  get: getPiece,
  list: listPieces,
} = pieceRailsMethod;

export const updatePiece = async (
  tx: WriteTransaction,
  value: PieceModelUpdate,
) => {
  const currentPiece = await getPiece(tx, value.id);
  // Don't update a piece that has been placed.
  if (currentPiece?.placed) {
    return;
  }
  await pieceRailsMethod.update(tx, value);
};
