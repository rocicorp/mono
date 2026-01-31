import * as v from '../../../../../../shared/src/valita.ts';
import {
  backfillIDSchema,
  identifierSchema,
  tableMetadataSchema,
} from './data.ts';
import {statusMessageSchema} from './status.ts';

/** At the moment, the only upstream messages are status messages.  */
export const changeSourceUpstreamSchema = statusMessageSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;

/**
 * Contains the information for requesting a backfill of columns in a table.
 * Backfills are automatically started for new tables and columns in a given
 * change stream session; however, if the session is terminated before the
 * backfill completes, it must be restarted with appropriate
 * {@link BackfillRequest}s when creating a new session.
 *
 * The `change-streamer` is responsible for tracking any changes to the table
 * name, column names, or table metadata, and constructing a BackfillRequest
 * based on the current values (which may be different from when the
 * tables/columns were originally added).
 */
export const backfillRequestSchema = v.object({
  table: identifierSchema.extend({
    // The table metadata is set to null if it is never specified by the
    // change-source.
    metadata: tableMetadataSchema.nullable(),
  }),
  columns: v.record(backfillIDSchema),
});

export type BackfillRequest = v.Infer<typeof backfillRequestSchema>;
