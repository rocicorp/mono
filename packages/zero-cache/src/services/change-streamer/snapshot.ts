/**
 * Definitions for the `snapshot` API, which serves the purpose of:
 * - informing subscribers (i.e. view-syncers) of the (litestream)
 *   backup location from which to restore a replica snapshot
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
  backupURL: v.string(),
});

const statusMessageSchema = v.tuple([v.literal('status'), statusSchema]);

export const snapshotMessageSchema = v.union(statusMessageSchema);

export type SnapshotMessage = v.Infer<typeof statusMessageSchema>;
