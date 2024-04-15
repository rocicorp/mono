import * as v from 'shared/src/valita.js';
import {normalizedFilterSpecSchema} from '../../types/invalidation.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';

// Note: Same as zql/invalidation.ts:InvalidationInfo
const queryInvalidationSchema = v.object({
  filters: v.array(normalizedFilterSpecSchema),
  hashes: v.array(v.string()),
});

export const watchRequestSchema = v.object({
  /**
   * Maps caller-defined query ID strings to the query's invalidation filters and hashes.
   */
  queries: v.record(queryInvalidationSchema),

  /**
   * The starting version from which to watch for query invalidation (i.e. the CVR version),
   * or absent if the caller is starting from scratch and has no queries (rows) to invalidate.
   */
  fromVersion: v.string().optional(),
});

export type WatchRequest = v.Infer<typeof watchRequestSchema>;

const queryInvalidationUpdateSchema = v.object({
  /** The newest version of the database at the time the invalidations were processed. */
  newVersion: v.string(),

  /** The starting point (exclusive) from which invalidations were computed. */
  fromVersion: v.string(),

  /** Maps caller-defined query ID strings to the versions at which they were invalidated. */
  invalidatedQueries: v.record(v.string()),
});

export type QueryInvalidationUpdate = v.Infer<
  typeof queryInvalidationUpdateSchema
>;

/**
 * An Invalidation Watcher is a per-Service Runner (i.e. Durable Object) service that
 * serves as the liaison between the View Syncers in the Service Runner and the global
 * Replicator.
 *
 * ```
 * ┌-------------------------------------------------------┐
 * |                                      <--> View Syncer |
 * | Replicator <--> Invalidation Watcher <--> View Syncer |
 * |     ^                                <--> View Syncer |
 * └-----|-------------------------------------------------┘
 *       |
 * ┌-----|-------------------------------------------------┐
 * |     |                                <--> View Syncer |
 * |     └---------> Invalidation Watcher <--> View Syncer |
 * |                                      <--> View Syncer |
 * └-------------------------------------------------------┘
 * ```
 *
 * The Invalidation Watcher serves two architectural purposes:
 *
 * * **Reduces notification fan-out from the Replicator**: Replicators only need to manage
 *   `O(num-service-runners)` notification streams, which is orders of magnitudes less than
 *   `O(num-view-syncers)`.
 *
 * * **Reduces query fan-in when computing view invalidation**: View Syncers register their
 *   invalidation hashes with the Invalidation Watcher. On each replication change, the
 *   Invalidation Watcher makes a single, composite query on the Invalidation Index, as
 *   opposed to all View Syncers querying individually. This is a critical scalability
 *   component; the connection, cpu, and I/O usage incurred by having all View Syncers
 *   query the index for every transaction would be otherwise untenable.
 *
 * As a logistical corollary to the latter, the Invalidation Watcher also plays a role in
 * connection / transaction management. On each replication change, the Invalidation Watcher
 * creates a read-only TransactionPool, initially sized to a single connection, to query
 * the Invalidation Index. If queries have been invalidated, it passes the TransactionPool
 * to the corresponding View Syncers to execute their queries at the same snapshot of the
 * database, growing the pool to a configurable maximum number of connections to increase
 * concurrency and reduce latency. When the View Syncers finish, the `Subscription` cleanup
 * logic facilitates reference counting so that TransactionPools can be closed when
 * no longer needed.
 */
export interface InvalidationWatcher {
  /**
   * Creates a Subscription of {@link QueryInvalidationUpdate}s for the set of queries
   * specified in the {@link WatchRequest}.
   *
   * * The Invalidation Watcher ensures that all Invalidation Filter Specs are
   *   registered with the Replicator, noting the starting version from which each
   *   filter has been active.
   *
   * * At the same time, it queries the Invalidation Index for all specified
   *   invalidation hashes to see if any have been invalidated since the request's
   *   `fromVersion` field (i.e. the version of the CVR.)
   *
   * The first update returned from the subscription spans the `fromVersion` specified
   * in the `request`, up to the current `newVersion` of the database, indicating the
   * `invalidatedQueries` that have hashes or filter registrations that are newer than
   * `fromVersion`.
   *
   * Subsequent updates are sent for incremental invalidations as new transactions are
   * replicated. If the subscriber takes a long time processing an update (i.e. re-executing
   * queries) during which multiple new updates are produced, those updates will be
   * coalesced into a single update representing the cumulative invalidations since the
   * one being processed.
   *
   * For new views with no existing data, `fromVersion` should be omitted from the request.
   * In this case, the query to the Invalidation Index will be skipped, and the first
   * message will span `{fromVersion: newVersion, newVersion: newVersion}` with no
   * invalidated queries.
   */
  watch(
    request: WatchRequest,
  ): CancelableAsyncIterable<QueryInvalidationUpdate>;
}
