import * as v from '../../../../shared/src/valita.ts';
import type {Source} from '../../types/streams.ts';
import {type Change} from '../change-source/protocol/current/data.ts';
import {changeStreamDataSchema} from '../change-source/protocol/current/downstream.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {Service} from '../service.ts';

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
   * date.
   */
  subscribe(ctx: SubscriberContext): Promise<Source<Downstream>>;
}

// v1: Client-side support for JSON_FORMAT. Introduced in 0.18.
// v2: v0.19
//   - Adds the "status" message which is initially used to signal that the
//     subscription is valid (i.e. starting at the requested watermark).
// v3: Adds the "taskID" to the subscription context, and support for
//     the BackupMonitor-mediated "/snapshot" request.
// v4: Adds the "replicaVersion" and "minWatermark" fields to the "/snapshot"
//     status request so that a subscriber can verify whether its replica,
//     whether it be restored or existing in a permanent volume, is compatible
//     with the change-streamer.
// v5: v0.26
//   - Moves relation.keyColumns and relation.replicaIdentity to
//     relation.rowKey: { columns, type }.
//   - Adds `metadata` to `create-table` message
//   - Adds `tableMetadata` to `add-column` message
//   - Adds `table-update-metadata` message

export const PROTOCOL_VERSION = 5;

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

export type ChangeEntry = {
  change: Change;

  /**
   * Note that it is technically possible for multiple changes to have
   * the same watermark, but that of a commit is guaranteed to be final,
   * so subscribers should only store the watermark of commit changes.
   */
  watermark: string;
};

/**
 * The StatusMessage payload for now is empty, but can be extended to
 * include meta-level information in the future.
 */
export const statusSchema = v.object({
  tag: v.literal('status'),
});

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

export interface ChangeStreamerService extends ChangeStreamer, Service {
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
