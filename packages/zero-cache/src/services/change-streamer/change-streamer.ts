import type {Source} from '../../types/streams.js';
import type {Service} from '../service.js';
import type {
  Change,
  DataChange,
  MessageBegin,
  MessageCommit,
} from './schema/change.js';

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
 * **Cleanup** (Not yet implemented)
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
  // TODO: Also take a Source<Upstream> for receiving ACKs.
  subscribe(ctx: SubscriberContext): Promise<Source<Downstream>>;
}

export type SubscriberContext = {
  /**
   * Subscriber id. This is only used for debugging.
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

export const enum ErrorType {
  Unknown,
  WrongReplicaVersion,
  WatermarkTooOld,
}

export type SubscriptionError = {
  type: ErrorType;
  message?: string | undefined;
};

export type Begin = ['begin', MessageBegin];
export type Data = ['data', DataChange];
export type Commit = ['commit', MessageCommit, {watermark: string}];
export type Error = ['error', SubscriptionError];

export type DownstreamChange = Begin | Data | Commit;

/**
 * A stream of transactions, each starting with a {@link MessageBegin},
 * containing one or more {@link DataChange}s, and ending with a
 * {@link MessageCommit}. The 'commit' tuple includes a `watermark` that
 * should be stored with the committed data and used for resuming
 * a subscription (e.g. in the {@link SubscriberContext}).
 *
 * A {@link SubscriptionError} indicates an unrecoverable error that requires
 * manual intervention (e.g. configuration / operational error).
 */
export type Downstream = DownstreamChange | Error;

export interface ChangeStreamerService extends ChangeStreamer, Service {}
