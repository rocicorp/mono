import type {Enum} from '../../../../shared/src/enum.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {ParsedJSON, Source} from '../../types/streams.ts';
import {
  changeStreamDataSchema,
  type ChangeStreamData,
} from '../change-source/protocol/current/downstream.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import {changeSourceTimingsSchema} from '../replicator/reporter/report-schema.ts';
import type {Service} from '../service.ts';
import * as ErrorType from './error-type-enum.ts';

type ErrorType = Enum<typeof ErrorType>;

export type ChangeTag = ChangeStreamData[1]['tag'];

/**
 * Internally all downstream messages (not just commits) are given a watermark.
 * These are used for internal ordering while replaying stored changes and
 * filtering changes already seen by a subscriber.
 *
 * Only commit watermarks are exposed to subscribers. The JSON string is the
 * canonical serialization shared by storage and live forwarding.
 */
export type WatermarkedChange = [
  watermark: string,
  tag: ChangeTag,
  json: string,
];

/**
 * The ChangeStreamer is the component between replicators ("subscribers")
 * and a canonical upstream source of changes (e.g. a Postgres logical
 * replication slot). It facilitates multiple subscribers without incurring
 * the associated upstream expense (e.g. PG replication slots are resource
 * intensive) with a "forward-store-ack" procedure.
 *
 * * Changes from the upstream source are immediately **forwarded** to
 *   connected subscribers to minimize latency.
 *
 * * They are then **stored** in a separate DB to facilitate catchup
 *   of connecting subscribers that are behind.
 *
 * * **Acknowledgements** are sent upstream after they are successfully
 *   stored.
 *
 * Unlike Postgres replication slots, in which the progress of a static
 * subscriber is tracked in the replication slot, the ChangeStreamer
 * supports a dynamic set of subscribers (i.e.. zero-caches) that can
 * can continually change.
 *
 * However, it is not the case that the ChangeStreamer needs to support
 * arbitrarily old subscribers. Because the replica is continually
 * backed up to a global location and used to initialize new subscriber
 * tasks, an initial subscription request from a subscriber constitutes
 * a signal for how "behind" a new subscriber task can be. This is
 * reflected in the {@link SubscriberContext}, which indicates whether
 * the watermark corresponds to an "initial" watermark derived from the
 * replica at task startup.
 *
 * The ChangeStreamer uses a combination of this signal with ACK
 * responses from connected subscribers to determine the watermark up
 * to which it is safe to purge old change log entries.
 */
export interface ChangeStreamer {
  /**
   * Subscribes to changes based on the supplied subscriber `ctx`,
   * which indicates the watermark at which the subscriber is up to
   * date. Each result contains both parsed, validated data and the exact JSON
   * payload sent by the ChangeStreamerService.
   */
  subscribe(ctx: SubscriberContext): Promise<Source<SerializedDownstream>>;
}

// v1: v0.18
//   - Client-side support for JSON_FORMAT. Introduced in 0.18.
// v2: v0.19
//   - Adds the "status" message which is initially used to signal that the
//     subscription is valid (i.e. starting at the requested watermark).
// v3: v0.20
//   - Adds the "taskID" to the subscription context, and support for
//     the backup monitor-mediated "/snapshot" request.
// v4: v0.25
//   - Adds the "replicaVersion" and "minWatermark" fields to the "/snapshot"
//     status request so that a subscriber can verify whether its replica,
//     whether it be restored or existing in a permanent volume, is compatible
//     with the change-streamer.
// v5: v0.26
//   - Moves relation.keyColumns and relation.replicaIdentity to
//     relation.rowKey: { columns, type }.
//   - Adds `metadata` to `create-table` message
//   - Adds `tableMetadata` to `add-column` message
//   - Adds `table-update-metadata` message
// v6: v0.26
//   - Adds support for `backfill` messages
// v6: v1.0.1  (backwards compatible, no version change)
//   - Adds lag reporting to status messages

export const PROTOCOL_VERSION = 6;

export type SubscriberContext = {
  /**
   * The supported change-streamer protocol version.
   */
  protocolVersion: number;

  /**
   * Task ID. This is used to link the request with a preceding snapshot
   * reservation.
   */
  taskID: string | null; // TODO: Make required when v3 is min.

  /**
   * Subscriber id. This is only used for debugging.
   */
  id: string;

  /**
   * The ReplicatorMode of the subscriber. 'backup' indicates that the
   * subscriber is local to the `change-streamer` in the `replication-manager`,
   * while 'serving' indicates that user-facing requests depend on the subscriber.
   */
  mode: ReplicatorMode;

  /**
   * The ChangeStreamer will return an Error if the subscriber is
   * on a different replica version (i.e. the initial snapshot associated
   * with the replication slot).
   */
  replicaVersion: string;

  /**
   * The watermark up to which the subscriber is up to date.
   * Only changes after the watermark will be streamed.
   */
  watermark: string;

  /**
   * Whether this is the first subscription request made by the task,
   * i.e. indicating that the watermark comes from a restored replica
   * backup. The ChangeStreamer uses this to determine which changes
   * are safe to purge from the Storer.
   */
  initial: boolean;
};

/**
 * The StatusMessage payload for now is empty, but can be extended to
 * include meta-level information in the future.
 */
export const statusSchema = v.object({
  tag: v.literal('status'),

  lagReport: v
    .object({
      lastTimings: changeSourceTimingsSchema.optional(),
      nextSendTimeMs: v.number(),
    })
    .optional(),
});

export type Status = v.Infer<typeof statusSchema>;

export const statusMessageSchema = v.tuple([v.literal('status'), statusSchema]);

/**
 * A StatusMessage will be immediately sent on a (v2+) subscription to
 * indicate that the subscription is valid (i.e. starting at the requested
 * watermark). Invalid subscriptions will instead result in a
 * SubscriptionError as the first message.
 */
export type StatusMessage = v.Infer<typeof statusMessageSchema>;

const subscriptionErrorSchema = v.object({
  type: v.number(), // ErrorType
  message: v.string().optional(),
});

export type SubscriptionError = v.Infer<typeof subscriptionErrorSchema>;

const errorSchema = v.tuple([v.literal('error'), subscriptionErrorSchema]);

export const downstreamSchema = v.union(
  statusMessageSchema,
  changeStreamDataSchema,
  errorSchema,
);

export type Error = v.Infer<typeof errorSchema>;

export function errorTypeToReadableName(val: ErrorType) {
  switch (val) {
    case ErrorType.WrongReplicaVersion:
      return 'WrongReplicaVersion';
    case ErrorType.WatermarkTooOld:
      return 'WatermarkTooOld';
    case ErrorType.Unknown:
      return 'Unknown';
    default:
      return 'Unknown';
  }
}

/**
 * A stream of transactions, each starting with a {@link Begin} message,
 * containing one or more {@link Data} messages, and ending with a
 * {@link Commit} or {@link Rollback} message. The 'commit' tuple
 * includes a `watermark` that should be stored with the committed
 * data and used for resuming a subscription (e.g. in the
 * {@link SubscriberContext}).
 *
 * A {@link SubscriptionError} indicates an unrecoverable error that requires
 * manual intervention (e.g. configuration / operational error).
 */
export type Downstream = v.Infer<typeof downstreamSchema>;

/** A downstream message and its canonical ChangeStreamer JSON. */
export type SerializedDownstream = ParsedJSON<Downstream>;

export interface ChangeStreamerService
  extends Omit<ChangeStreamer, 'subscribe'>, Service {
  /**
   * The server-side interface overrides `subscribe()` to return a stream
   * of already-stringified {@link Downstream} payloads.
   */
  subscribe(ctx: SubscriberContext): Promise<Source<string>>;

  /**
   * Notifies the change streamer of a watermark that has been backed up,
   * indicating that changes before the watermark can be purged if active
   * subscribers have progressed beyond the watermark.
   */
  scheduleCleanup(watermark: string): void;

  getChangeLogState(): Promise<{
    replicaVersion: string;
    minWatermark: string;
  }>;
}
