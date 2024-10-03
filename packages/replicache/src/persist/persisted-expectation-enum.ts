/* eslint-disable @typescript-eslint/naming-convention */

export const Snapshot = 0;
export const SnapshotAndLocals = 1;
export const Locals = 2;
export const Nothing = 3;

export type Snapshot = typeof Snapshot;
export type SnapshotAndLocals = typeof SnapshotAndLocals;
export type Locals = typeof Locals;
export type Nothing = typeof Nothing;

export type Type = Snapshot | SnapshotAndLocals | Locals | Nothing;
