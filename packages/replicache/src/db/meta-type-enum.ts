/* eslint-disable @typescript-eslint/naming-convention */

export const SnapshotSDD = 3;
export const LocalDD31 = 4;
export const SnapshotDD31 = 5;

export type SnapshotSDD = typeof SnapshotSDD;
export type LocalDD31 = typeof LocalDD31;
export type SnapshotDD31 = typeof SnapshotDD31;

export type Type = SnapshotSDD | LocalDD31 | SnapshotDD31;
