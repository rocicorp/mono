import * as v from '../../../../../../shared/src/valita.ts';

// Note: The stages are ordered by restore availability:
// * InitialSync is the first to create the generation, by definition.
// * Replicate has already restored and thus can be restored from
// * Restore is in the process of awaiting the backup
export const InitialSync = 0;
export const Replicate = 1;
export const Restore = 2;

export type InitialSync = typeof InitialSync;
export type Replicate = typeof Replicate;
export type Restore = typeof Restore;

export const replicaStageSchema = v.literalUnion(
  InitialSync,
  Replicate,
  Restore,
);

export type ReplicaStage = v.Infer<typeof replicaStageSchema>;
