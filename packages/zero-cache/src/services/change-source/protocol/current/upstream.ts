import * as v from '../../../../../../shared/src/valita.ts';
import {
  backfillIDSchema,
  identifierSchema,
  tableMetadataSchema,
} from './data.ts';
import {upstreamStatusMessageSchema} from './status.ts';

/**
 * A mark identifies how far a subscriber has applied an ordered backfill run:
 * the Postgres text form of the last applied row's key values, in
 * `relation.rowKey.columns` order. It is opaque to everything but the change
 * source, which is the only party that ever orders keys.
 */
export const markSchema = v.array(v.string());

export type Mark = v.Infer<typeof markSchema>;

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

  // Where the backfill should resume from, if the change-streamer knows of a
  // subscriber that has already applied a prefix of an ordered run. Absent
  // means "from the beginning".
  resumeFrom: markSchema.nullable().optional(),

  // The watermark at which `resumeFrom` was recorded. A mark recorded before
  // a row key change on the table is not safe to resume from (see
  // `minSnapshot`), so the change source drops it.
  resumeFromWatermark: v.string().nullable().optional(),

  // The earliest snapshot at which a backfill of this table is valid: the
  // version of the most recent row-key-changing update on it that the cookie
  // jar knows about. A mark whose `resumeFromWatermark` precedes this is
  // dropped, and a run whose snapshot precedes it is canceled and retried.
  minSnapshot: v.string().nullable().optional(),
});

export type BackfillRequest = v.Infer<typeof backfillRequestSchema>;

/**
 * Sent by the change-streamer when a subscriber declares progress on a
 * backfill that the change-streamer could not resolve from its own change
 * log: either the subscriber needs rows that a running run has already
 * passed, or it needs a table that this change-source session has already
 * finished.
 */
export const backfillRequestMessageSchema = v.tuple([
  v.literal('backfill-request'),
  v.object({
    table: identifierSchema.extend({
      metadata: tableMetadataSchema.nullable(),
    }),
    columns: v.record(backfillIDSchema),

    // The declaring subscriber's mark, or null if it has none (or if its mark
    // was dropped).
    mark: markSchema.nullable(),

    // The watermark at which the subscriber recorded `mark`.
    markWatermark: v.string().nullable(),

    // The run the subscriber is already following, if any. If this is the
    // running run, the subscriber is already covered and nothing is done.
    runID: v.string().nullable(),

    // Who declared it. Carried for attribution only -- no decision reads it --
    // so that the restart a declaration causes names the subscriber that
    // caused it. Optional: a change-streamer that predates the field, or one
    // resending a request it remembered before it existed, sends none.
    subscriberID: v.string().optional(),
  }),
]);

export type BackfillRequestMessage = v.Infer<
  typeof backfillRequestMessageSchema
>;

/**
 * Upstream messages are status messages and backfill requests.
 *
 * A change source that does not support backfill requests ignores everything
 * but `status`.
 */
export const changeSourceUpstreamSchema = v.union(
  upstreamStatusMessageSchema,
  backfillRequestMessageSchema,
);
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;
