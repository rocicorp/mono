import {CancelableAsyncIterable} from 'zero-cache/src/types/streams.js';
import {Change} from './schema/change.js';

/**
 * The ChangeStreamer is the interface between replicators ("subscribers")
 * and a canonical upstream source of changes (e.g. a Postgres logical
 * replication slot).
 *
 * It facilitates multiple subscribers without incurring the associated
 * upstream expense (e.g. PG replication slots are resource intensive)
 * by employing a "forward-store-ack" algorithm.
 *
 * * Changes from the upstream source are immediately **forwarded** to
 *   connected subscribers to minimize latency.
 *
 * * They are then **stored** in a separate archive to facilitate catchup
 *   of connecting subscribers that are behind.
 *
 * * **Acknowledgements** are sent upstream after they are successfully
 *   archived.
 *
 * **Cleanup**
 *
 * Old changes in the archive must be periodically purged to avoid
 * unbounded growth. Postgres replication uses an ACK protocol to track,
 * per replication slot, the latest LSN that the subscriber confirmed,
 * allowing older log entries to be cleaned up.
 *
 * The ChangeStreamer has a more flexible protocol and supports a
 * dynamic set of subscribers. Rather than using an ACK protocol to
 * track the progress of each subscriber, the protocol is simplified
 * based on the fact that all subscribers (i.e. tasks) are initialized
 * with a global backup of the replica that is continually updated.
 * A tasks, when connecting to the ChangeStreamer, indicates when its
 * watermark comes from the replica (i.e. its `initial` subscription),
 * which the ChangeStreamer uses as a signal for how up to date the
 * backup is. The ChangeStreamer can thus safely purge changes up to
 * the backup's watermark, or that of its most behind connected client,
 * whichever is earlier.
 */
export interface ChangeStreamer {
  /**
   * Subscribes to changes based on the supplied subscriber `ctx`,
   * which indicates the watermark at which the subscriber is up to
   * date.
   */
  subscribe(ctx: SubscriberContext): CancelableAsyncIterable<Downstream>;
}

export type SubscriberContext = {
  /**
   * Subscriber id.
   *
   * Only one subscription per `id` is maintained. Old subscriptions
   * with the same `id` will be closed.
   */
  id: string;

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
   * are safe to purge from the archive.
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

export enum ErrorType {
  Unknown,
  WrongReplicaVersion,
  WatermarkTooOld,
}

export type SubscriptionError = {
  type: ErrorType;
  message?: string | undefined;
};

export type Downstream = ['change', ChangeEntry] | ['error', SubscriptionError];
