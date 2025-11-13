/**
 * Definitions for the `snapshot` API, which serves the purpose of:
 * - informing subscribers (i.e. view-syncers) of the (litestream)
 *   backup location from which to restore a replica snapshot
 * - checking whether a restored backup or existing replica is
 *   compatible with the change-streamer
 * - preventing change-log cleanup while a snapshot restore is in
 *   progress
 * - tracking the approximate time it takes from the beginning of
 *   snapshot "reservation" to the subsequent subscription, which
 *   serves as the minimum interval to wait before cleaning up
 *   backed up changes.
 */

import * as v from '../../../../shared/src/valita.ts';

const statusSchema = v.object({
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
});

export type SnapshotStatus = v.Infer<typeof statusSchema>;

const statusMessageSchema = v.tuple([v.literal('status'), statusSchema]);

export const snapshotMessageSchema = v.union(statusMessageSchema);

export type SnapshotMessage = v.Infer<typeof statusMessageSchema>;
