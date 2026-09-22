/**
 * Pure schema definitions for the `/snapshot` reservation status message.
 *
 * These live in their own module (depending only on valita) so that both
 * {@link snapshot.ts} (which also pulls in the change-streamer HTTP client) and
 * {@link subscribe.ts} can import them without forming an import cycle.
 */

import * as v from '../../../../shared/src/valita.ts';

export const statusSchema = v.object({
  tag: v.literal('status'),

  /**
   * The location from which litestream should perform the restore.
   */
  backupURL: v.string(),

  /**
   * The `replicaVersion` of the backup. If a subscriber's restored or
   * existing replica is of a different version, it should delete it and
   * retry the restore from litestream (i.e. equivalent to a
   * `WrongReplicaVersion` response from a `/changes` subscription).
   */
  replicaVersion: v.string(),

  /**
   * The earliest watermark from which catchup is possible. If the
   * subscriber's replica is older that this watermark, it should delete it
   * and (retry the) restore from litestream (i.e. equivalent to a
   * `WatermarkTooOld` response from a `/changes` subscription).
   */
  minWatermark: v.string(),

  /**
   * The size in bytes of the replication-manager's replica when the snapshot
   * was reserved, used as an estimate of the size of the restored replica
   * when reporting restore progress. Absent from older replication-managers.
   */
  replicaSize: v.number().optional(),
});

export type SnapshotStatus = v.Infer<typeof statusSchema>;

export const statusMessageSchema = v.tuple([v.literal('status'), statusSchema]);

export const snapshotMessageSchema = v.union(statusMessageSchema);

export type SnapshotMessage = v.Infer<typeof statusMessageSchema>;
