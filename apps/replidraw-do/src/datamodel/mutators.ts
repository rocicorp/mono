import type { WriteTransaction } from "replicache";
import {
  initClientState,
  setCursor,
  overShape,
  selectShape,
} from "./client-state";
import {
  putShape,
  deleteShape,
  moveShape,
  scanShape,
  resizeShape,
  rotateShape,
  initShapes,
} from "./shape";

export type M = typeof mutators;

export const mutators = {
  createShape: putShape,
  deleteShape,
  moveShape,
  scanShape,
  resizeShape,
  rotateShape,
  initClientState,
  setCursor,
  overShape,
  selectShape,
  initShapes,
  nop: async (_: WriteTransaction) => {},
};
