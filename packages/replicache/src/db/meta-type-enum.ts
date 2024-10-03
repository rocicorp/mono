/* eslint-disable @typescript-eslint/naming-convention */

export const IndexChangeSDD = 1;
export const LocalSDD = 2;
export const SnapshotSDD = 3;
export const LocalDD31 = 4;
export const SnapshotDD31 = 5;

export type IndexChangeSDD = typeof IndexChangeSDD;
export type LocalSDD = typeof LocalSDD;
export type SnapshotSDD = typeof SnapshotSDD;
export type LocalDD31 = typeof LocalDD31;
export type SnapshotDD31 = typeof SnapshotDD31;

export type Type =
  | IndexChangeSDD
  | LocalSDD
  | SnapshotSDD
  | LocalDD31
  | SnapshotDD31;
