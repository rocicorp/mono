import type {ReadTransaction} from 'replicache';
import {z} from 'zod';

export const PARTIAL_SYNC_STATE_KEY = 'control/partialSync';

export const partialSyncStateSchema = z.union([
  z.literal('ISSUES_SYNCED'),
  z.object({endKey: z.string()}),
  z.literal('PARTIAL_SYNC_COMPLETE'),
]);

export type PartialSyncState = z.TypeOf<typeof partialSyncStateSchema>;

export async function getPartialSyncState(
  tx: ReadTransaction,
): Promise<PartialSyncState | undefined> {
  const val = await tx.get(PARTIAL_SYNC_STATE_KEY);
  if (val === undefined) {
    return undefined;
  }
  return partialSyncStateSchema.parse(val);
}
