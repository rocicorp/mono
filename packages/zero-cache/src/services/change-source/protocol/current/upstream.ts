import * as v from '../../../../../../shared/src/valita.ts';
import {
  backfillIDSchema,
  backfillProgressMarkSchema,
  identifierSchema,
  tableMetadataSchema,
} from './data.ts';
import {upstreamStatusMessageSchema} from './status.ts';

/** At the moment, the only upstream messages are status messages.  */
export const changeSourceUpstreamSchema = upstreamStatusMessageSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;

/**
 * Contains the information for requesting a backfill of columns in a table.
 * Backfills are automatically started for new tables and columns in a given
 * change stream session; however, if the session is terminated before the
 * backfill completes, it must be restarted with appropriate
 * {@link BackfillRequest}s in subsequent session(s), as backfill state is
 * ephemeral, per-replication-manager, and not persisted upstream.
 *
 * All replication subscribers track their backfill state, which includes
 * tracking any changes to the table name, column names, or table metadata,
 * and present that state to the `change-streamer` when starting a
 * subscription.
 *
 * From there the `change-streamer` manages requesting from the change-source
 * the superset (and minimum progressMark) of all subscriber-specified
 * backfills. While subscribers are connected, the change-streamer also tracks
 * this per-subscriber state in memory, so that it can properly resume
 * backfills on a new change-stream if disconnected from upstream.
 */
export const backfillRequestSchema = v.object({
  table: identifierSchema.extend({
    // The table metadata is set to null if it is never specified by the
    // change-source.
    metadata: tableMetadataSchema.nullable(),
  }),
  columns: v.record(
    v.object({
      id: backfillIDSchema,
      progress: backfillProgressMarkSchema.optional(),
    }),
  ),
});

export type BackfillRequest = v.Infer<typeof backfillRequestSchema>;
