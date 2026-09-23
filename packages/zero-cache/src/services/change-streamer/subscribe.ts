/**
 * The `subscribe` API (protocol v7+) merges the previous `/snapshot` and
 * `/changes` connections into a single bidirectional WebSocket, so that the
 * replication-manager that reserves change-log entries for a soon-to-be
 * subscriber is the same replication-manager that serves the subscription.
 *
 * The connection is driven by an upstream (view-syncer → replication-manager)
 * application message that selects the flow:
 *
 * - **Fresh startup:** the view-syncer sends `['reserve-snapshot', {taskID}]`.
 *   The change-streamer opens a snapshot reservation (pinning the change log)
 *   and replies downstream with a single `['reserved', …]` message telling the
 *   view-syncer where to restore the backup from and the watermark from which
 *   catchup is possible. Once the backup is restored, the view-syncer sends
 *   `['start-subscription', ctx]`, which transitions the same connection into
 *   the subscription phase (change stream downstream). The reservation is
 *   released only after the subscriber is registered, so there is never a gap
 *   in which the change-streamer does not know the client's watermark.
 *
 * - **Reconnect (replica already valid):** the view-syncer sends
 *   `['start-subscription', ctx]` immediately, with no reservation.
 *
 * The reservation status uses a distinct `'reserved'` tag (rather than the
 * change stream's `'status'`) so the downstream union is unambiguous under
 * passthrough parsing.
 */

import * as v from '../../../../shared/src/valita.ts';
import {downstreamSchema} from './change-streamer.ts';
import {statusSchema} from './snapshot-message.ts';

const snapshotStatusSchema = statusSchema.extend({
  tag: v.literal('snapshot'), // Replace tag: 'status' with tag: 'snapshot'
});

// ---------------------------------------------------------------------------
// Upstream (view-syncer → replication-manager) — application control messages.
// ---------------------------------------------------------------------------

export const reserveSnapshotMessageSchema = v.tuple([
  v.literal('reserve-snapshot'),
  v.object({taskID: v.string()}),
]);

/**
 * The subscription-initialization context, sent in the `start-subscription`
 * message. Equivalent to the previous URL-encoded `SubscriberContext`, minus
 * the fields that are now implicit on the merged connection:
 * - `protocolVersion` comes from the request path.
 * - `wsBatched` is always on (the transport batches internally).
 *
 * Parsed in `passthrough` mode so a forthcoming, orthogonal change can add
 * arbitrary table/column/backfill state without a protocol bump.
 */
export const subscribeContextSchema = v.object({
  /** Task ID, links the subscription to a preceding snapshot reservation. */
  taskID: v.string(),
  /** Subscriber id (debugging only). */
  id: v.string(),
  /** 'serving' (user-facing) or 'backup' (in-RM replicator). */
  mode: v.union(v.literal('serving'), v.literal('backup')),
  /** The replica version the subscriber restored / is running. */
  replicaVersion: v.string(),
  /** The watermark up to which the subscriber is up to date. */
  watermark: v.string(),
});

export type SubscribeContext = v.Infer<typeof subscribeContextSchema>;

export const startSubscriptionMessageSchema = v.tuple([
  v.literal('start-subscription'),
  subscribeContextSchema,
]);

export const subscribeUpstreamSchema = v.union(
  reserveSnapshotMessageSchema,
  startSubscriptionMessageSchema,
);

export type SubscribeUpstream = v.Infer<typeof subscribeUpstreamSchema>;

// ---------------------------------------------------------------------------
// Downstream (replication-manager → view-syncer).
// ---------------------------------------------------------------------------

/**
 * The reservation confirmation. Carries the same payload as the legacy
 * `/snapshot` status (`backupURL`, `replicaVersion`, `minWatermark`,
 * `replicaSize?`) but under a distinct `'reserved'` tag so it does not collide
 * with the change stream's `['status', …]` message.
 */
export const reservedMessageSchema = v.tuple([
  v.literal('reserved'),
  snapshotStatusSchema,
]);

export type ReservedMessage = v.Infer<typeof reservedMessageSchema>;

export const subscribeDownstreamSchema = v.union(
  reservedMessageSchema,
  downstreamSchema,
);

export type SubscribeDownstream = v.Infer<typeof subscribeDownstreamSchema>;
